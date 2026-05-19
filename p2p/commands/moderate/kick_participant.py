from commands.register import register_handler
from utils import safe_send_json, broadcast_to_room, CommandContext, RoomParticipantPair

import logging

logger = logging.getLogger(__name__)


@register_handler("kick_participant")
async def handle_kick_participant(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    target_id = data.get("target_id")
    if not target_id:
        return ctx.current_room, ctx.current_participant
    target = ctx.current_room.participants.get(target_id)
    if not target:
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Участник не найден"
        })
        return ctx.current_room, ctx.current_participant
    await safe_send_json(target.websocket, data={
        "type": "kicked",
        "reason": data.get("reason", "")
    })
    await target.websocket.close(code=1000, reason="kicked")
    await ctx.room_manager.remove_participant(ctx.current_room, target_id)
    await broadcast_to_room(ctx.current_room, message={
        "type": "participant_left",
        "participant_id": target_id
    })
    return ctx.current_room, ctx.current_participant
