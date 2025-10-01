import asyncio
from httpx import AsyncClient
from arq import create_pool
from arq.connections import RedisSettings, ArqRedis

from fastapi import FastAPI, Depends
import uvicorn

app = FastAPI()

# Here you can configure the Redis connection.
# The default is to connect to localhost:6379, no password.
REDIS_SETTINGS = RedisSettings(host="arq_redis", port=6379,password="yourpassword")

async def get_arq_pool():
    return await create_pool(REDIS_SETTINGS)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/")
async def root_post(pool: ArqRedis = Depends(get_arq_pool)):
    for url in ("https://facebook.com", "https://microsoft.com", "https://github.com"):
        pool.enqueue_job("download_content", url)

    return {"message": "Hello World"}


async def download_content(ctx, url):
    session: AsyncClient = ctx["session"]
    response = await session.get(url)
    print(f"{url}: {response.text:.80}...")
    return len(response.text)


async def startup(ctx):
    ctx["session"] = AsyncClient()


async def shutdown(ctx):
    await ctx["session"].aclose()


# WorkerSettings defines the settings to use when creating the work,
# It's used by the arq CLI.
# redis_settings might be omitted here if using the default settings
# For a list of all available settings, see https://arq-docs.helpmanual.io/#arq.worker.Worker
class WorkerSettings:
    functions = [download_content]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = REDIS_SETTINGS


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)