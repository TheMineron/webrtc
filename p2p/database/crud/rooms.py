from typing import Optional

from p2p.database.redis_client import redis_client


async def create_room(room_id: str) -> None:
    await redis_client.sadd("rooms", room_id)


async def delete_room(room_id: str) -> None:
    await redis_client.srem("rooms", room_id)
    await redis_client.delete(f"room:{room_id}:participants")


async def get_room_params(room_id: str) -> dict:
    data = await redis_client.hgetall(f"room:{room_id}:params")
    return {
        "is_locked": data.get("is_locked", "false") == "true",
        "max_participants": int(data["max_participants"]) if data.get("max_participants") else None,
        "password_hash": data.get("password_hash"),
        "moderator_id": data.get("moderator_id"),
    }


async def set_room_locked(room_id: str, locked: bool) -> None:
    await redis_client.hset(f"room:{room_id}:params", "is_locked", "true" if locked else "false")


async def set_room_max_participants(room_id: str, limit: Optional[int]) -> None:
    if limit is None:
        await redis_client.hdel(f"room:{room_id}:params", "max_participants")
    else:
        await redis_client.hset(f"room:{room_id}:params", "max_participants", str(limit))


async def set_room_password_hash(room_id: str, password_hash: Optional[str]) -> None:
    if password_hash is None:
        await redis_client.hdel(f"room:{room_id}:params", "password_hash")
    else:
        await redis_client.hset(f"room:{room_id}:params", "password_hash", password_hash)


async def set_room_moderator(room_id: str, participant_id: Optional[str]) -> None:
    if participant_id is None:
        await redis_client.hdel(f"room:{room_id}:params", "moderator_id")
    else:
        await redis_client.hset(f"room:{room_id}:params", "moderator_id", participant_id)
