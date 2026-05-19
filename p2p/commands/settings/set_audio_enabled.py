from commands.register import register_handler
from database.crud.participants import set_participant_status
from utils import broadcast_to_room, CommandContext, RoomParticipantPair


@register_handler("set_audio_enabled")
async def handle_set_audio_enabled(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    enabled = data.get("enabled", True)
    ctx.current_participant.audio_enabled = enabled
    await set_participant_status(
        ctx.current_participant.id,
        enabled,
        ctx.current_participant.video_enabled,
        ctx.current_participant.screen_sharing
    )
    await broadcast_to_room(ctx.current_room, {
        "type": "participant_updated",
        "participant_id": ctx.current_participant.id,
        "audio_enabled": enabled
    })
    return ctx.current_room, ctx.current_participant
