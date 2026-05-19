from commands.register import register_handler
from database.crud.participants import set_participant_status
from utils import safe_send_json, broadcast_to_room, CommandContext, RoomParticipantPair

import logging

logger = logging.getLogger(__name__)


@register_handler("mute_all")
async def handle_mute_all(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    if ctx.current_participant.role != "moderator":
        await safe_send_json(ctx.websocket, data={
            "type": "error",
            "message": "Требуются права модератора"
        })
        return ctx.current_room, ctx.current_participant
    for p in ctx.current_room.participants.values():
        if p.id == ctx.current_participant.id:
            continue
        p.audio_enabled = False
        await set_participant_status(
            p.id,
            p.audio_enabled,
            p.video_enabled,
            p.screen_sharing
        )
        await safe_send_json(p.websocket, data={
            "type": "force_mute",
            "by": "moderator"
        })
    await broadcast_to_room(ctx.current_room, message={"type": "all_muted"})
    return ctx.current_room, ctx.current_participant
