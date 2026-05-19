import logging
import secrets
import time

from commands.register import register_handler
from database.crud import get_message, save_message
from database.models import ChatMessage
from utils import safe_send_json, get_visible_participants

logger = logging.getLogger(__name__)


@register_handler("chat_reply")
async def handle_reply_to_message(ctx, data):
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    text = data.get("text")
    reply_to_msg_id = data.get("reply_to_msg_id")
    if not text or not reply_to_msg_id:
        return ctx.current_room, ctx.current_participant
    original = await get_message(ctx.current_room.id, reply_to_msg_id)
    if not original or original.deleted:
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Исходное сообщение не найдено или удалено"
        })
        return ctx.current_room, ctx.current_participant
    target_id = data.get("target_id")
    msg = ChatMessage(
        msg_id=secrets.token_urlsafe(16),
        from_id=ctx.current_participant.id,
        from_name=ctx.current_participant.nickname,
        text=text,
        target_id=target_id,
        timestamp=time.time(),
        reply_to_msg_id=reply_to_msg_id,
    )
    await save_message(ctx.current_room.id, msg)

    targets = get_visible_participants(ctx.current_room, msg)
    for p in targets:
        await safe_send_json(p.websocket, {
            "type": "chat",
            **msg.to_dict()
        })
    return ctx.current_room, ctx.current_participant
