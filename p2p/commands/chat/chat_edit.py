from commands.register import register_handler
from database.crud import edit_message
from utils import safe_send_json, CommandContext, RoomParticipantPair, get_visible_participants


@register_handler("chat_edit")
async def handle_chat_edit(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    msg_id = data.get("msg_id")
    new_text = data.get("text", "")
    if not msg_id or not new_text:
        return ctx.current_room, ctx.current_participant

    edited_msg = await edit_message(ctx.current_room.id, msg_id, new_text)
    if edited_msg:
        if edited_msg.from_id != ctx.current_participant.id:
            await safe_send_json(ctx.websocket, {
                "type": "error",
                "message": "Можно редактировать только свои сообщения"
            })
            return ctx.current_room, ctx.current_participant

        targets = get_visible_participants(ctx.current_room, edited_msg)
        for p in targets:
            await safe_send_json(p.websocket, {
                "type": "chat_edited",
                "msg_id": msg_id,
                "text": new_text,
                "edited": True
            })
    return ctx.current_room, ctx.current_participant
