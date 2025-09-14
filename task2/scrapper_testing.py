# Просто чтобы убедиться что scrapper работает, полноценные тесты нужно писать отдельно
import asyncio
import os
from task2.main import GithubReposScrapper


async def main():
    token = os.getenv("GITHUB_ACCESS_TOKEN")
    scrapper = GithubReposScrapper(access_token=token)
    try:
        repositories = await scrapper.get_repositories(limit=5)
        print(f"Получено {len(repositories)} репозиториев")
    finally:
        await scrapper.close()

if __name__ == '__main__':
    asyncio.run(main())
