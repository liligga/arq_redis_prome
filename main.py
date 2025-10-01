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
    sites = (
        "https://www.google.com ",
        "https://www.youtube.com ",
        "https://www.facebook.com ",
        "https://www.twitter.com ",
        "https://www.instagram.com ",
        "https://www.amazon.com ",
        "https://www.wikipedia.org ",
        "https://www.tiktok.com ",
        "https://www.reddit.com ",
        "https://www.netflix.com",
    )
    for url in sites:
        await pool.enqueue_job(
            "download_content", url, _queue_name="message_processing"
        )

    return {"message": "Hello World"}


@app.get("/queue")
async def get_queue(pool: ArqRedis = Depends(get_arq_pool)):
    # Check types first (optional debugging)
    type1 = await pool.type("message_processing")
    type2 = await pool.type("arq:jobs")  # ← note: plural!
    type3 = await pool.type("arq:message_processing")

    print(f"Types: {type1}, {type2}, {type3}")

    # Now safely get counts based on actual types
    try:
        res1 = await pool.zcard("message_processing") if type1 == b"zset" else 0
    except:
        res1 = 0

    try:
        res2 = await pool.scard("arq:jobs") if type2 == b"set" else 0
    except:
        res2 = 0

    try:
        res3 = await pool.llen("arq:message_processing") if type3 == b"list" else 0
    except:
        res3 = 0

    print(f"Results: {res1}, {res2}, {res3}")

    return {"q1": res1, "q2": res2, "q3": res3}


async def download_content(ctx, url):
    session: AsyncClient = ctx["session"]
    response = await session.get(url)
    await asyncio.sleep(2)
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
