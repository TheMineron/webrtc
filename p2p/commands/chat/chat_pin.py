import logging

from commands.register import register_handler
from database.crud.chat import pin_message
from utils import safe_send_json, broadcast_to_room, CommandContext, RoomParticipantPair

logger = logging.getLogger(__name__)


@register_handler("chat_pin")
async def handle_pin_message(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    msg_id = data.get("msg_id")
    if not msg_id:
        return ctx.current_room, ctx.current_participant
    await pin_message(ctx.current_room.id, msg_id)
    await broadcast_to_room(ctx.current_room, message={
        "type": "chat_pinned",
        "msg_id": msg_id
    })
    return ctx.current_room, ctx.current_participant
