from p2p.commands.register import register_handler
from p2p.utils import CommandContext
from utils import broadcast_to_room, RoomParticipantPair
from database.crud.participants import set_participant_status
import logging

logger = logging.getLogger(__name__)


@register_handler("set_video_enabled")
async def handle_set_video_enabled(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    enabled = data.get("enabled", True)
    ctx.current_participant.video_enabled = enabled
    await set_participant_status(
        ctx.current_participant.id,
        ctx.current_participant.audio_enabled,
        enabled,
        ctx.current_participant.screen_sharing
    )
    await broadcast_to_room(ctx.current_room, {
        "type": "participant_updated",
        "participant_id": ctx.current_participant.id,
        "video_enabled": enabled
    })
    return ctx.current_room, ctx.current_participant
