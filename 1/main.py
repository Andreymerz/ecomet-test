from typing import AsyncIterator
from contextlib import asynccontextmanager
import asyncpg
from fastapi import FastAPI, Request, Depends


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Инициализация пула соединений при старте приложения
    database_url = "postgresql://postgres:password@localhost:5432/testdb"
    pool = await asyncpg.create_pool(
        database_url,
        min_size=1,
        max_size=20,
        command_timeout=60,
    )
    app.state.pg_pool = pool
    
    yield
    
    # Закрытие пула соединений при остановке приложения
    await pool.close()


# Создание FastAPI приложения с lifespan
app = FastAPI(lifespan=lifespan)


async def get_pg_connection(request: Request) -> AsyncIterator[asyncpg.Connection]:
    pool: asyncpg.Pool | None = getattr(request.app.state, "pg_pool", None)
    if pool is None:
        raise RuntimeError("PostgreSQL pool is not initialized")
    async with pool.acquire() as conn:
        # при необходимости можно настроить codecs/statement cache здесь
        yield conn  # async generator dependency — FastAPI корректно освободит ресурс


@app.get("/db-version")
async def get_db_version(conn: asyncpg.Connection = Depends(get_pg_connection)):
    result = await conn.fetchrow("SELECT version() as version")
    return {"version": result["version"]}


@app.get("/health")
async def health_check():
    return {"status": "ok"}
