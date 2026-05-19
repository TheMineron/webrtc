import os
import json
from typing import Optional

from p2p.database.redis_client import redis_client
from p2p.database.models import ChatMessage

CHAT_HISTORY_SIZE = int(os.getenv("CHAT_HISTORY_SIZE", "50"))


async def save_message(room_id: str, msg: ChatMessage) -> None:
    msg_key = f"room:{room_id}:msg:{msg.msg_id}"
    pipe = redis_client.pipeline()
    await pipe.lpush(f"room:{room_id}:messages", msg.msg_id)
    await pipe.ltrim(f"room:{room_id}:messages", 0, CHAT_HISTORY_SIZE - 1)
    await pipe.set(msg_key, json.dumps(msg.to_dict()))
    await pipe.execute()


async def get_message(room_id: str, msg_id: str) -> Optional[ChatMessage]:
    data = await redis_client.get(f"room:{room_id}:msg:{msg_id}")
    if data:
        return ChatMessage.from_dict(json.loads(data))
    return None


async def edit_message(room_id: str, msg_id: str, new_text: str) -> Optional[ChatMessage]:
    msg = await get_message(room_id, msg_id)
    if msg and not msg.deleted:
        msg.text = new_text
        msg.edited = True
        await redis_client.set(f"room:{room_id}:msg:{msg_id}", json.dumps(msg.to_dict()))
        return msg
    return None


async def delete_message(room_id: str, msg_id: str) -> Optional[ChatMessage]:
    msg = await get_message(room_id, msg_id)
    if msg and not msg.deleted:
        msg.deleted = True
        msg.text = ""
        await redis_client.set(f"room:{room_id}:msg:{msg_id}", json.dumps(msg.to_dict()))
        return msg
    return None


async def get_chat_history(
        room_id: str,
        for_participant_id: str,
        count: int = CHAT_HISTORY_SIZE
) -> list[dict]:
    ids = await redis_client.lrange(f"room:{room_id}:messages", 0, count - 1)
    messages = []
    for msg_id in reversed(ids):
        msg = await get_message(room_id, msg_id)
        if msg and not msg.deleted:
            if msg.target_id is None or msg.target_id == for_participant_id or msg.from_id == for_participant_id:
                messages.append(msg.to_dict())
    return messages


async def pin_message(room_id: str, msg_id: str) -> bool:
    await redis_client.sadd(f"room:{room_id}:pinned", msg_id)
    return True


async def unpin_message(room_id: str, msg_id: str) -> bool:
    await redis_client.srem(f"room:{room_id}:pinned", msg_id)
    return True


async def get_pinned_messages(room_id: str) -> list[str]:
    return list(await redis_client.smembers(f"room:{room_id}:pinned"))


async def clear_chat(room_id: str) -> None:
    msg_ids = await redis_client.lrange(f"room:{room_id}:messages", 0, -1)
    pipe = redis_client.pipeline()
    for msg_id in msg_ids:
        pipe.delete(f"room:{room_id}:msg:{msg_id}")
    pipe.delete(f"room:{room_id}:messages")
    pipe.delete(f"room:{room_id}:pinned")
    await pipe.execute()


async def update_sender_name_in_all_messages(
        room_id: str,
        participant_id: str,
        new_name: str
) -> None:
    msg_ids = await redis_client.lrange(f"room:{room_id}:messages", 0, -1)
    pipe = redis_client.pipeline()
    for msg_id in msg_ids:
        msg_data = await redis_client.get(f"room:{room_id}:msg:{msg_id}")
        if msg_data:
            msg = json.loads(msg_data)
            if msg.get("from_id") == participant_id:
                msg["from_name"] = new_name
                pipe.set(f"room:{room_id}:msg:{msg_id}", json.dumps(msg))
    await pipe.execute()
