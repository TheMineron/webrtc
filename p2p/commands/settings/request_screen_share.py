from commands.register import register_handler
from database.crud.participants import set_participant_status
from utils import broadcast_to_room, CommandContext, RoomParticipantPair


@register_handler("request_screen_share")
async def handle_request_screen_share(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant
    enabled = data.get("enabled", True)
    ctx.current_participant.screen_sharing = enabled
    await set_participant_status(
        ctx.current_participant.id,
        ctx.current_participant.audio_enabled,
        ctx.current_participant.video_enabled,
        enabled
    )
    await broadcast_to_room(ctx.current_room, {
        "type": "screen_share_state",
        "participant_id": ctx.current_participant.id,
        "enabled": enabled
    })
    return ctx.current_room, ctx.current_participant
