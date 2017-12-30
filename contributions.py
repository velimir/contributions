#!/usr/bin/env python3.6

import aiohttp
import argparse
import asyncio
import asyncio_extras
import json
import logging
import os
import progressbar
import sys

GITHUB_DEFAULT_BASE_URL = "https://api.github.com"


def arg_parser():
    parser = argparse.ArgumentParser(
        description=('List projects to which user have contributed to. '
                     'Script requires valid GitHub token set via GITHUB_TOKEN '
                     'environment variables.'),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-g', '--url',
                        help='GitHub base url (default: %(default)s)',
                        default=GITHUB_DEFAULT_BASE_URL)
    parser.add_argument('-u', '--username', help='Contributor username',
                        default=None)
    parser.add_argument('-o', '--owner', dest='owners', action='append',
                        help='Owners to check')
    parser.add_argument('-v', '--verbose',
                        help='Verbose mode (default: %(default)s)',
                        default=False, action='store_true')
    parser.add_argument('-n', '--no-progress',
                        help='Verbose mode (default: %(default)s)',
                        default=False, action='store_false')
    parser.add_argument('-m', '--max-concurrency',
                        help=('Maximum # of concurrent requests to GitHub '
                              '(default: %(default)s)'),
                        type=int, default=20)
    parser.add_argument('-c', '--max-contributors',
                        help=('Maximum # of concurrent requests to GitHub '
                              'contributors endpoint (default: %(default)s)'),
                        type=int, default=20)
    parser.add_argument('--out',
                        help=('output file with contributions '
                              '(default=stdout)'),
                        type=argparse.FileType('w'), default=sys.stdout)
    return parser


def setup_logging(is_verbose):
    level = logging.DEBUG if is_verbose else logging.WARN
    log_format = ('%(asctime)s %(levelname)s %(filename)s:%(lineno)d '
                  '%(funcName)s %(message)s')
    logging.basicConfig(format=log_format, level=level)


def paginated(field, info_field='pageInfo'):
    def paginated_decorator(func):
        async def wrapper(*args, after=None, **kwargs):
            while True:
                page = await func(*args, **{**kwargs, **{'after': after}})
                for it in page[field]:
                    yield it

                info = page[info_field]
                if not info['hasNextPage']:
                    return

                after = info['endCursor']
        return wrapper
    return paginated_decorator


class GHClient(aiohttp.ClientSession):

    def __init__(self, token, base_url,
                 concur_max=10, contrib_max=5, contrib_delay=5, headers=None,
                 **kwargs):
        self.__base_url = base_url
        self.semaphore = asyncio.Semaphore(concur_max)
        self.contributors_sem = asyncio.Semaphore(contrib_max)
        self.contributors_delay = contrib_delay
        client_headers = {
            'Authorization': f'bearer {token}',
            'User-Agent': 'stats-script/velimir'
        }
        headers = {**(headers or {}), **client_headers}
        super().__init__(headers=headers, **kwargs)

    async def gql_request(self, query, **variables):
        path = '/graphql'
        payload = {'query': query, 'variables': variables}
        async with self.request('post', path, json=payload) as resp:
            res = await resp.json()
            if not res.get('data', None):
                text = await resp.text()
                logging.error('query %s response: %s', text, query)
                reason = res.get('errors', text)
                raise RuntimeError(f"GQL request failed: {reason}")
            return res['data']

    @asyncio_extras.async_contextmanager
    async def request(self, method, path, **kwargs):
        # TODO: wrap request into retry logic with exponential backoff
        # see https://github.com/aio-libs/aiohttp/issues/850 for example
        async with self.semaphore:
            url = self.__base_url + path
            logging.debug('%s: %s', method, url)
            async with super().request(method, url, **kwargs) as resp:
                yield resp

    async def contributors(self, owner, repo):
        async with self.contributors_sem:
            contr = await self.__contributors(owner, repo)
            return (owner, repo, contr)

    async def __contributors(self, owner, repo, params=None):
        while True:
            path = f'/repos/{owner}/{repo}/stats/contributors'
            if self.__base_url != GITHUB_DEFAULT_BASE_URL:
                path = '/v3' + path
            async with self.request('get', path, params=None) as resp:
                if resp.status == 202:
                    logging.debug('contributor request for %s/%s initiated',
                                  owner, repo)
                    await asyncio.sleep(self.contributors_delay)
                elif resp.status == 204:
                    logging.debug('no contributions yet for %s/%s',
                                  owner, repo)
                    return []
                elif resp.status == 403:
                    logging.debug('no access to %s/%s', owner, repo)
                    return []
                elif resp.status == 200:
                    return await resp.json()
                else:
                    text = await resp.text()
                    logging.error('response %s: %s', resp.status, text)
                    raise RuntimeError(
                        f'unexpected response status {resp.status}')

    @paginated('organizations')
    async def organizations(self, user, after=None, limit=100):
        query = '''
            query userOrganizations($user: String!, $first: Int!,
                                    $after: String) {
                user(login: $user) {
                    organizations(first: $first, after: $after) {
                        edges {
                            node {
                                login
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            }
        '''
        res = await self.gql_request(query, user=user, after=after,
                                     first=limit)
        orgs = res['user']['organizations']
        return {
            'organizations': [edge['node'] for edge in orgs['edges']],
            'pageInfo': orgs['pageInfo']
        }

    @paginated('repositories')
    async def repositories(self, name, after=None, limit=100):
        query = '''
            query ownerRepositories($login: String!, $first: Int!,
                                    $after: String) {
                repositoryOwner(login: $login) {
                    repositories(first: $first, after: $after) {
                        edges {
                            node {
                                id
                                name
                                nameWithOwner
                            }
                        }
                        pageInfo {
                            hasNextPage
                            endCursor
                        }
                    }
                }
            }
        '''
        res = await self.gql_request(query, login=name, after=after,
                                     first=limit)
        repos = res['repositoryOwner']['repositories']

        def shape_node(node):
            owner, _ = node['nameWithOwner'].split('/')
            node['owner'] = owner
            return node

        return {
            'repositories': [shape_node(edge['node'])
                             for edge in repos['edges']],
            'pageInfo': repos['pageInfo']
        }

    async def viewer(self):
        query = '''
            query viewerLogin {
                viewer {
                    login
                }
            }
        '''
        rs = await self.gql_request(query)
        return rs['viewer']


async def affected_owners(gh, username, organizations):
    if organizations:
        for org in organizations:
            yield org

    else:
        async for org in gh.organizations(username):
            yield org['login']

    yield username


def task_iterator(progress_enabled, max_value):
    if progress_enabled:
        return progressbar.ProgressBar(
            max_value=max_value,
            widgets=[progressbar.widgets.SimpleProgress(), ' ',
                     progressbar.widgets.ETA(), ' ',
                     progressbar.widgets.Percentage(), ' ',
                     progressbar.widgets.Bar()])
    return iter


async def list_contributions(token, base_url, username=None, owners=None,
                             max_concurrency=20, max_contributions=20,
                             pbar_enabled=True):
    async with GHClient(token, base_url, concur_max=max_concurrency,
                        contrib_max=max_contributions) as gh:
        if not username:
            user = await gh.viewer()
            username = user['login']

        tasks = []
        async for owner in affected_owners(gh, username, owners):
            async for repo in gh.repositories(owner):
                coro = gh.contributors(repo['owner'], repo['name'])
                task = asyncio.ensure_future(coro)
                tasks.append(task)

        user_stats = []
        async_iter = asyncio.as_completed(tasks)
        do_it = task_iterator(pbar_enabled, len(tasks))
        for task in do_it(async_iter):
            try:
                owner, repo, contributions = await task
            except RuntimeError as e:
                logging.exception('failed to get contributions: %s', e)
            else:
                stats = [c for c in contributions
                         if c['author']['login'] == username]
                if stats:
                    repo_stats = {
                        'repo': {
                            'owner': owner,
                            'name': repo,
                        },
                        'stats': stats[0]
                    }
                    user_stats.append(repo_stats)
        return user_stats


def user_contributions(gh_token, gh_url, username, owners, max_concurrency,
                       max_contributions, pbar_enabled):
    loop = asyncio.get_event_loop()
    coro = list_contributions(gh_token, gh_url, username, owners,
                              max_concurrency, max_contributions, pbar_enabled)
    task = asyncio.ensure_future(coro, loop=loop)
    try:
        loop.run_until_complete(task)
    finally:
        loop.close()

    return task.result()


def main():
    parser = arg_parser()
    args = parser.parse_args()

    gh_token = os.getenv('GITHUB_TOKEN')
    if not gh_token:
        parser.error('environment variable GITHUB_TOKEN is undefined')

    pbar_enabled = not args.no_progress
    if pbar_enabled:
        progressbar.streams.wrap_stderr()
    setup_logging(args.verbose)

    contrs = user_contributions(gh_token, args.url, args.username, args.owners,
                                args.max_concurrency,
                                args.max_contributors,
                                pbar_enabled)
    json.dump(contrs, args.out, indent=4)


if __name__ == '__main__':
    main()
