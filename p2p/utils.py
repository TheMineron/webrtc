import logging
from typing import Callable, Awaitable, TypeAlias
from fastapi import WebSocket
from database.models import Room, Participant, ChatMessage
from .room_manager import RoomManager

logger = logging.getLogger(__name__)

RoomParticipantPair: TypeAlias = tuple[Room | None, Participant | None]
CommandHandler: TypeAlias = Callable[..., Awaitable[RoomParticipantPair]]


class CommandContext:
    def __init__(
        self,
        websocket: WebSocket,
        room_manager: RoomManager,
        current_room: Room | None,
        current_participant: Participant | None
    ) -> None:
        self.websocket = websocket
        self.room_manager = room_manager
        self.current_room = current_room
        self.current_participant = current_participant


async def safe_send_json(websocket: WebSocket, data: dict) -> None:
    try:
        await websocket.send_json(data)
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение: {e}")


async def broadcast_to_room(
    room: Room,
    message: dict,
    exclude_ws: WebSocket | None = None
) -> None:
    for participant in room.participants.values():
        if exclude_ws and participant.websocket == exclude_ws:
            continue
        await safe_send_json(participant.websocket, message)


def get_visible_participants(room: "Room", msg: "ChatMessage") -> list["Participant"]:
    if msg.target_id is None:
        return list(room.participants.values())
    else:
        visible = []
        sender = room.participants.get(msg.from_id)
        if sender:
            visible.append(sender)
        receiver = room.participants.get(msg.target_id)
        if receiver and receiver.id != msg.from_id:
            visible.append(receiver)
        return visible
