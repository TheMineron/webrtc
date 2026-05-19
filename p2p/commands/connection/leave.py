import logging

from p2p.commands.register import register_handler
from p2p.utils import CommandContext, RoomParticipantPair, broadcast_to_room

logger = logging.getLogger(__name__)


@register_handler("leave")
async def handle_leave(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    await ctx.room_manager.remove_participant(ctx.current_room, ctx.current_participant.id)
    await broadcast_to_room(
        ctx.current_room,
        message={
            "type": "participant_left",
            "participant_id": ctx.current_participant.id
        },
        exclude_ws=ctx.websocket
    )
    await ctx.room_manager.remove_if_empty(ctx.current_room.id)

    logger.info(f"Участник {ctx.current_participant} покинул комнату {ctx.current_room.id}")

    return None, None
