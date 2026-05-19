import secrets
import time

from commands.register import register_handler
from database.crud import save_message
from database.models import ChatMessage
from utils import safe_send_json, get_visible_participants


@register_handler("chat_send")
async def handle_chat_send(ctx, data):
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    text = data.get("text")
    if not text:
        return ctx.current_room, ctx.current_participant

    target_id = data.get("target_id")
    msg = ChatMessage(
        msg_id=secrets.token_urlsafe(16),
        from_id=ctx.current_participant.id,
        from_name=ctx.current_participant.nickname,
        text=text,
        target_id=target_id,
        timestamp=time.time()
    )
    await save_message(ctx.current_room.id, msg)

    targets = get_visible_participants(ctx.current_room, msg)
    for p in targets:
        await safe_send_json(p.websocket, {
            "type": "chat_sent",
            **msg.to_dict()
        })
    return ctx.current_room, ctx.current_participant
