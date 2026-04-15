import json
import uuid
import logging
from contextlib import asynccontextmanager
from typing import Callable, Awaitable, TypeAlias, Final

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Signaling server started")
    yield
    logger.info("Signaling server stopped")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="p2p/static"), name="static")


class Participant:
    def __init__(self, websocket: WebSocket, nickname: str):
        self.id = str(uuid.uuid4())
        self.websocket = websocket
        self.nickname = nickname

    def __str__(self):
        return f"self.nickname(id={self.id})"


class Room:
    def __init__(self, room_id: str):
        self.id = room_id
        self.participants: dict[str, Participant] = {}

    def add(self, participant: Participant) -> None:
        self.participants[participant.id] = participant

    def remove(self, participant_id: str) -> Participant | None:
        return self.participants.pop(participant_id, None)

    def get_participant_by_websocket(
            self,
            websocket: WebSocket
    ) -> Participant | None:
        for participant in self.participants.values():
            if participant.websocket == websocket:
                return participant
        return None

    def get_info_list(self, exclude_id: str | None = None):
        return [
            {"id": pid, "name": p.nickname}
            for pid, p in self.participants.items()
            if pid != exclude_id
        ]

    def __str__(self) -> str:
        return str(self.id)


class RoomManager:
    def __init__(self):
        self.rooms: dict[str, Room] = {}

    def get_or_create(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]

    def remove_if_empty(self, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room and not room.participants:
            del self.rooms[room_id]
            logger.info(f"Комната {room_id} удалена (пуста)")

    def find_room_by_websocket(self, websocket: WebSocket) -> Room | None:
        for room in self.rooms.values():
            if room.get_participant_by_websocket(websocket):
                return room
        return None


room_manager = RoomManager()


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


RoomParticipantPair: TypeAlias = tuple[Room | None, Participant | None]
CommandHandler: TypeAlias = Callable[
    [CommandContext, dict],
    Awaitable[RoomParticipantPair]
]


class JoinCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        room_id = data.get("room")
        nickname = data.get("nickname")
        if not room_id or not nickname:
            await safe_send_json(ctx.websocket, {
                "type": "error",
                "message": "room и nickname обязательны"
            })
            return ctx.current_room, ctx.current_participant

        room = ctx.room_manager.get_or_create(room_id)
        participant = Participant(ctx.websocket, nickname)
        room.add(participant)

        logger.info(f"Участник {nickname} ({participant.id}) вошёл в комнату {room_id}")

        await safe_send_json(ctx.websocket, {
            "type": "joined",
            "room": room_id,
            "nickname": nickname,
            "participant_id": participant.id
        })

        await broadcast_to_room(
            room,
            message={
                "type": "participant_joined",
                "participant": {
                    "id": participant.id,
                    "name": nickname
                }
            },
            exclude_ws=ctx.websocket
        )

        existing = room.get_info_list(exclude_id=participant.id)
        await safe_send_json(ctx.websocket, {
            "type": "existing_participants",
            "participants": existing
        })

        return room, participant


class SignalCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        if not ctx.current_room or not ctx.current_participant:
            return ctx.current_room, ctx.current_participant

        target_id = data.get("target_id")
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


class PingCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        if not ctx.current_participant:
            return ctx.current_room, ctx.current_participant

        timestamp = data.get("timestamp")
        if timestamp:
            await safe_send_json(ctx.websocket, {"type": "pong", "timestamp": timestamp})

        return ctx.current_room, ctx.current_participant


COMMAND_HANDLERS: Final[dict[str, CommandHandler]] = {
    "join": JoinCommand.handle,
    "signal": SignalCommand.handle,
    "ping": PingCommand.handle,
}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    current_room: Room | None = None
    current_participant: Participant | None = None

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("Получен невалидный JSON")
                continue

            msg_type = msg.get("type")
            if not msg_type:
                continue

            handler = COMMAND_HANDLERS.get(msg_type)
            if handler is None:
                logger.debug(f"Неизвестный тип сообщения: {msg_type}")
                continue

            ctx = CommandContext(websocket, room_manager, current_room, current_participant)
            try:
                new_room, new_participant = await handler(ctx, msg)
                current_room, current_participant = new_room, new_participant
            except Exception as e:
                logger.exception(f"Ошибка при обработке команды {msg_type}: {e}")
                await safe_send_json(websocket, data={
                    "type": "error",
                    "message": "Internal error"
                })

    except WebSocketDisconnect:
        logger.info(f"WebSocket отключён: {current_participant}")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка: {e}")
    finally:
        if current_room and current_participant:
            removed = current_room.remove(current_participant.id)
            if removed:
                logger.info(
                    f"Участник {current_participant} удалён из комнаты {current_room}"
                )
                await broadcast_to_room(
                    current_room,
                    message={
                        "type": "participant_left",
                        "participant_id": current_participant.id
                    },
                    exclude_ws=websocket
                )
                room_manager.remove_if_empty(current_room.id)
        try:
            await websocket.close()
        except Exception as e:
            logger.exception(f"Неожиданная ошибка: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="localhost", port=8000)
