"""
Определите метод get_repositories в классе GithubReposScrapper, который возвращает Data-класс Repository, где authors_commits_num_today - список авторов с количеством коммитов за последний день.
a. Отправлять запросы для получения коммитов репозитория АСИНХРОННО
b. Ограничить максимальное количество одновременных запросов (MCR)
c. Ограничить количество запросов в секунду (RPS)

"""

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Any
import logging

from aiohttp import ClientSession

logger = logging.getLogger("__name__")

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


class RateLimiter:
    def __init__(self, max_requests_per_second: int):
        self.max_requests_per_second = max_requests_per_second
        self.requests = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = datetime.now()
            # Удаляем запросы старше 1 секунды
            self.requests = [req_time for req_time in self.requests if now - req_time < timedelta(seconds=1)]

            # Если достигли лимита, ждем
            if len(self.requests) >= self.max_requests_per_second:
                sleep_time = 1.0 - (now - self.requests[0]).total_seconds()
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                # Обновляем список запросов после ожидания
                now = datetime.now()
                self.requests = [req_time for req_time in self.requests if now - req_time < timedelta(seconds=1)]

            self.requests.append(now)


class GithubReposScrapper:
    def __init__(self, access_token: str, max_concurrent_requests: int = 10, requests_per_second: int = 30):
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._rate_limiter = RateLimiter(requests_per_second)

    async def _make_request(self, endpoint: str, method: str = "GET", params: dict[str, Any] | None = None) -> Any:
        async with self._semaphore:
            await self._rate_limiter.acquire()
            async with self._session.request(method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params) as response:
                return await response.json()

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        data = await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )
        return data["items"]

    async def _get_repository_commits(self, owner: str, repo: str) -> list[dict[str, Any]]:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        # Получаем коммиты за последний день
        since = (datetime.now() - timedelta(days=1)).isoformat()

        try:
            commits = await self._make_request(
                endpoint=f"repos/{owner}/{repo}/commits",
                params={"since": since, "per_page": 100}
            )
            return commits if isinstance(commits, list) else []
        except Exception as e:
            logger.exception(f"Error fetching commits for {owner}/{repo}: {e}", exc_info=e)
            return []

    async def _get_repository_commits_count_by_author(self, owner: str, repo: str) -> list[RepositoryAuthorCommitsNum]:
        """Получает количество коммитов по авторам за последний день"""
        commits = await self._get_repository_commits(owner, repo)

        author_commits = defaultdict(int)
        for commit in commits:
            if commit.get('commit') and commit['commit'].get('author'):
                author_name = commit['commit']['author'].get('name', 'Unknown')
                author_commits[author_name] += 1

        return [
            RepositoryAuthorCommitsNum(author=author, commits_num=count)
            for author, count in author_commits.items()
        ]

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        """Получает топ репозиториев с информацией о коммитах авторов за последний день"""
        top_repos = await self._get_top_repositories(limit)

        tasks = []
        repositories_data = []

        for position, repo_data in enumerate(top_repos, 1):
            owner = repo_data.get('owner', {}).get('login', '')
            name = repo_data.get('name', '')

            repositories_data.append({
                'name': name,
                'owner': owner,
                'position': position,
                'stars': repo_data.get('stargazers_count', 0),
                'watchers': repo_data.get('watchers_count', 0),
                'forks': repo_data.get('forks_count', 0),
                'language': repo_data.get('language', 'Unknown'),
            })

            task = self._get_repository_commits_count_by_author(owner, name)
            tasks.append(task)

        commits_results = await asyncio.gather(*tasks, return_exceptions=True)

        repositories = []
        for repo_data, commits_result in zip(repositories_data, commits_results):
            if isinstance(commits_result, Exception):
                logger.exception(f"Commits error: {repo_data['owner']}/{repo_data['name']}: {commits_result}", exc_info=commits_results)
                commits_result = []

            repository = Repository(
                name=repo_data['name'],
                owner=repo_data['owner'],
                position=repo_data['position'],
                stars=repo_data['stars'],
                watchers=repo_data['watchers'],
                forks=repo_data['forks'],
                language=repo_data['language'],
                authors_commits_num_today=commits_result
            )
            repositories.append(repository)

        return repositories

    async def close(self):
        await self._session.close()
