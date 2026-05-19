import logging

from p2p.commands.register import register_handler
from p2p.utils import CommandContext, RoomParticipantPair, safe_send_json

logger = logging.getLogger(__name__)


@register_handler("ping")
async def handle_ping(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    timestamp = data.get("timestamp")
    if timestamp:
        await safe_send_json(ctx.websocket, data={
            "type": "pong",
            "timestamp": timestamp
        })

    return ctx.current_room, ctx.current_participant
