from p2p.database.redis_client import redis_client


async def add_participant_to_room(room_id: str, participant_id: str, nickname: str) -> None:
    await redis_client.sadd(f"room:{room_id}:participants", participant_id)
    await redis_client.hset(
        f"participant:{participant_id}",
        mapping={"nickname": nickname, "room_id": room_id}
    )


async def remove_participant_from_room(room_id: str, participant_id: str) -> None:
    await redis_client.srem(f"room:{room_id}:participants", participant_id)
    await redis_client.delete(f"participant:{participant_id}")


async def set_participant_role(participant_id: str, role: str) -> None:
    await redis_client.hset(f"participant:{participant_id}", "role", role)


async def get_participant_role(participant_id: str) -> str | None:
    return await redis_client.hget(f"participant:{participant_id}", "role")


async def set_participant_status(
        participant_id: str,
        audio_enabled: bool,
        video_enabled: bool,
        screen_sharing: bool = False,
) -> None:
    pipe = redis_client.pipeline()
    pipe.hset(f"participant:{participant_id}", "audio_enabled", str(audio_enabled))
    pipe.hset(f"participant:{participant_id}", "video_enabled", str(video_enabled))
    pipe.hset(f"participant:{participant_id}", "screen_sharing", str(screen_sharing))
    await pipe.execute()


async def get_participant_status(participant_id: str) -> dict:
    data = await redis_client.hgetall(f"participant:{participant_id}")
    return {
        "audio_enabled": data.get("audio_enabled", "true") == "true",
        "video_enabled": data.get("video_enabled", "true") == "true",
        "screen_sharing": data.get("screen_sharing", "false") == "true",
    }


async def update_participant_nickname(participant_id: str, new_nickname: str) -> None:
    await redis_client.hset(f"participant:{participant_id}", "nickname", new_nickname)
