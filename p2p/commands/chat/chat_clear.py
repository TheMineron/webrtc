from commands.register import register_handler
from database.crud.chat import clear_chat
from utils import safe_send_json, broadcast_to_room


@register_handler("chat_clear")
async def handle_clear_chat(ctx, data):
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    await clear_chat(ctx.current_room.id)
    await broadcast_to_room(ctx.current_room, message={"type": "chat_cleared"})
    return ctx.current_room, ctx.current_participant
