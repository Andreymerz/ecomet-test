import asyncio
import os
from datetime import date, datetime
from typing import List
import logging

from aiochclient import ChClient
from aiohttp import ClientSession, ClientTimeout

from task2.main import Repository, GithubReposScrapper

logger = logging.getLogger(__name__)


class ClickHouseConnectionManager:
    """
    Вообще логику батчинга лучше вынести отдельно в модуль с
    репозиториями, но для тестового задания оставим все в одном классе
    """
    def __init__(self, url: str = "http://localhost:8123", user: str = "clickhouse",
                 password: str = "password123", database: str = "test", batch_size: int = 1000):
        self.url = url
        self.user = user
        self.password = password
        self.database = database
        self.batch_size = batch_size
        self.client = None
        self._session = None

    async def __aenter__(self):
        timeout = ClientTimeout(total=30, connect=10)
        self._session = ClientSession(timeout=timeout)

        self.client = ChClient(
            self._session,
            url=self.url,
            user=self.user,
            password=self.password,
            database=self.database
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def save_repositories_data(self, repositories: List[Repository]):
        if not repositories:
            logger.warning("No repositories data to save")
            return

        logger.info(f"Starting to save data for {len(repositories)} repositories")

        try:
            # Подготовка данных репозиториев
            current_time = datetime.now().replace(microsecond=0)
            repositories_data = [
                (repo.name, repo.owner, repo.stars, repo.watchers, repo.forks,
                 repo.language or 'Unknown', current_time)
                for repo in repositories
            ]

            # Подготовка данных позиций
            current_date = date.today()
            positions_data = [
                (current_date, f"{repo.owner}/{repo.name}", repo.position)
                for repo in repositories
            ]

            # Подготовка данных коммитов авторов
            authors_commits_data = []
            for repo in repositories:
                for author_commit in repo.authors_commits_num_today:
                    authors_commits_data.append((
                        current_date,
                        f"{repo.owner}/{repo.name}",
                        author_commit.author,
                        author_commit.commits_num
                    ))

            await self._save_in_batches(repositories_data, "repositories",
                                        "(name, owner, stars, watchers, forks, language, updated)")
            await self._save_in_batches(positions_data, "repositories_positions",
                                        "(date, repo, position)")
            await self._save_in_batches(authors_commits_data, "repositories_authors_commits",
                                        "(date, repo, author, commits_num)")

            logger.info(f"Successfully saved data for {len(repositories)} repositories")
            logger.info(f"Saved {len(authors_commits_data)} author commits records")

        except Exception as e:
            logger.error(f"Error saving repositories data: {e}")
            raise

    async def _save_in_batches(self, data: List[tuple], table: str, columns: str):
        if not data:
            return

        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]

            if not batch:
                raise Exception(f"Empty {table} batch")  # в проде поменять на кастомный Exception

            try:
                await self.client.execute(
                    f"INSERT INTO {table} {columns} VALUES",
                    *batch
                )
                logger.info(f"Inserted {len(batch)} records into {table}")
            except Exception as e:
                logger.error(f"Error inserting {table} batch: {e}")
                raise

            # Небольшая задержка между батчами для снижения нагрузки
            if i + self.batch_size < len(data):
                await asyncio.sleep(0.1)


async def main():
    scrapper = GithubReposScrapper(
        access_token=os.getenv("GITHUB_ACCESS_TOKEN"),
        max_concurrent_requests=5,
        requests_per_second=10
    )

    try:
        logger.info("Fetching repositories data...")
        repositories = await scrapper.get_repositories(limit=10)

        async with ClickHouseConnectionManager(batch_size=100) as saver:
            await saver.save_repositories_data(repositories)

    except Exception as e:
        logger.error(f"Error in main: {e}")

    finally:
        await scrapper.close()


if __name__ == "__main__":
    asyncio.run(main())
