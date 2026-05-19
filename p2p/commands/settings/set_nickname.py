from commands.register import register_handler
from utils import (
    safe_send_json,
    broadcast_to_room,
    CommandContext,
    RoomParticipantPair
)
import logging

logger = logging.getLogger(__name__)


@register_handler("set_nickname")
async def handle_set_nickname(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    new_nickname = data.get("nickname", "").strip()
    if not new_nickname:
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Никнейм не может быть пустым"
        })
        return ctx.current_room, ctx.current_participant

    old_nickname = ctx.current_participant.nickname
    await ctx.room_manager.update_participant_nickname(
        ctx.current_room,
        ctx.current_participant.id,
        new_nickname
    )
    await broadcast_to_room(ctx.current_room, {
        "type": "participant_renamed",
        "participant_id": ctx.current_participant.id,
        "old_name": old_nickname,
        "new_name": new_nickname
    })
    return ctx.current_room, ctx.current_participant
