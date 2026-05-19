import logging

from p2p.commands.register import register_handler
from p2p.utils import CommandContext, RoomParticipantPair, safe_send_json

logger = logging.getLogger(__name__)


@register_handler("signal")
async def handle_signal(ctx: CommandContext, data: dict) -> RoomParticipantPair:
    if not ctx.current_room or not ctx.current_participant:
        return ctx.current_room, ctx.current_participant

    target_id = data.get("target_id")
    if target_id == ctx.current_participant.id:
        logger.warning(f"Попытка отправить сигнал самому "
                       f"себе от {ctx.current_participant.id}")
        return ctx.current_room, ctx.current_participant

    signal_data = data.get("data")
    if not target_id or not signal_data:
        return ctx.current_room, ctx.current_participant

    target = ctx.current_room.participants.get(target_id)
    if target:
        await safe_send_json(target.websocket, {
            "type": "signal",
            "from_id": ctx.current_participant.id,
            "from_name": ctx.current_participant.nickname,
            "data": signal_data
        })
    else:
        logger.warning(f"Цель {target_id} не найдена в комнате {ctx.current_room.id}")

    return ctx.current_room, ctx.current_participant
