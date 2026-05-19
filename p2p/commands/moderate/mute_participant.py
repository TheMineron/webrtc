from commands.register import register_handler
from database.crud.participants import set_participant_status
from utils import safe_send_json, broadcast_to_room, CommandContext, RoomParticipantPair

import logging

logger = logging.getLogger(__name__)


@register_handler("mute_participant")
async def handle_mute_participant(ctx: CommandContext, data: dict) -> RoomParticipantPair:
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
        return ctx.current_room, ctx.current_participant
    target.audio_enabled = False
    await set_participant_status(
        target.id,
        target.audio_enabled,
        target.video_enabled,
        target.screen_sharing
    )
    await safe_send_json(target.websocket, data={
        "type": "force_mute",
        "by": ctx.current_participant.nickname
    })
    await broadcast_to_room(ctx.current_room, {
        "type": "participant_updated",
        "participant_id": target_id,
        "audio_enabled": False
    })
    return ctx.current_room, ctx.current_participant
