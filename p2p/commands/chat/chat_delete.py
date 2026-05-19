from commands.register import register_handler
from database.crud import get_message, delete_message
from utils import safe_send_json, get_visible_participants


@register_handler("chat_delete")
async def handle_chat_delete(ctx, data):
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    msg_id = data.get("msg_id")
    if not msg_id:
        return ctx.current_room, ctx.current_participant

    msg = await get_message(ctx.current_room.id, msg_id)
    if not msg:
        return ctx.current_room, ctx.current_participant

    if ctx.current_participant.role != "moderator" and msg.from_id != ctx.current_participant.id:
        await safe_send_json(ctx.websocket, {
            "type": "error",
            "message": "Недостаточно прав для удаления этого сообщения"
        })
        return ctx.current_room, ctx.current_participant

    deleted_msg = await delete_message(ctx.current_room.id, msg_id)
    if deleted_msg:
        targets = get_visible_participants(ctx.current_room, deleted_msg)
        for p in targets:
            await safe_send_json(p.websocket, {
                "type": "chat_deleted",
                "msg_id": msg_id
            })
    return ctx.current_room, ctx.current_participant