import logging

from commands.register import register_handler
from utils import safe_send_json, broadcast_to_room, CommandContext, RoomParticipantPair

logger = logging.getLogger(__name__)


@register_handler("set_room_limit")
async def handle_set_room_limit(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    limit = data.get("limit")
    if limit is not None:
        try:
            limit = int(limit)
            if limit < 1:
                raise ValueError
        except ValueError:
            await safe_send_json(ctx.websocket, data={
                "type": "error",
                "message": "Неверное значение limit"
            })
            return ctx.current_room, ctx.current_participant
    ctx.current_room.max_participants = limit
    await ctx.room_manager.update_room_params(ctx.current_room)
    await broadcast_to_room(ctx.current_room, message={
        "type": "room_limit_updated",
        "limit": limit
    })

    return ctx.current_room, ctx.current_participant
