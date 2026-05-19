import logging

from commands.register import register_handler
from utils import safe_send_json, CommandContext, RoomParticipantPair

logger = logging.getLogger(__name__)


@register_handler("set_room_password")
async def handle_set_room_password(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket,data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    password = data.get("password")
    if password == "":
        password = None
    ctx.current_room.set_password(password)
    await ctx.room_manager.update_room_params(ctx.current_room)
    await safe_send_json(ctx.websocket,data={
        "type": "room_password_updated",
        "has_password": password is not None
    })
    return ctx.current_room, ctx.current_participant
