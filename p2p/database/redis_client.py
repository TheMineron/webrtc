import logging
import os

import redis.asyncio as redis

CHAT_HISTORY_SIZE = int(os.getenv("CHAT_HISTORY_SIZE", "50"))
logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


async def init_redis() -> None:
    try:
        await redis_client.ping()
        logger.info("Подключено к Redis")
    except Exception as e:
        logger.error(f"Ошибка подключения к Redis: {e}")


async def close_redis() -> None:
    await redis_client.close()
    logger.info("Соединение с Redis закрыто")
