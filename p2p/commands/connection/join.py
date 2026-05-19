import logging

from p2p.commands.register import register_handler
from database.models import Participant
from database.crud import get_chat_history
from p2p.utils import (
    CommandContext,
    RoomParticipantPair,
    safe_send_json,
    broadcast_to_room
)

logger = logging.getLogger(__name__)


@register_handler("join")
async def handle_join(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    room_id = data.get("room")
    nickname = data.get("nickname")
    password = data.get("password")
    if not room_id or not nickname:
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "room и nickname обязательны"
        })
        return ctx.current_room, ctx.current_participant

    room = await ctx.room_manager.get_or_create(room_id)

    can_join, reason = room.can_join(password)
    if not can_join:
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": reason
        })
        return ctx.current_room, ctx.current_participant

    participant = Participant(ctx.websocket, nickname=nickname)
    await ctx.room_manager.add_participant(room, participant)

    logger.info(f"Участник {nickname} ({participant.id}) вошёл в комнату {room_id}")
    await safe_send_json(ctx.websocket, {
        "type": "joined",
        "room": room_id,
        "nickname": nickname,
        "participant_id": participant.id,
        "role": participant.role,
    })
    await broadcast_to_room(
        room,
        message={
            "type": "participant_joined",
            "participant": {
                "id": participant.id,
                "name": nickname,
                "role": participant.role,
                "audio_enabled": True,
                "video_enabled": True,
            }
        },
        exclude_ws=ctx.websocket
    )
    existing = room.get_info_list(exclude_id=participant.id)
    await safe_send_json(ctx.websocket, data={
        "type": "existing_participants",
        "participants": existing
    })
    history = await get_chat_history(room.id, participant.id)
    if history:
        await safe_send_json(ctx.websocket, data={
            "type": "chat_history",
            "messages": history
        })
    return room, participant
