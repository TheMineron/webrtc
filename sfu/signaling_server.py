import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Awaitable, Callable, Final

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SFU_WS_URL = "wss://130.193.46.12:8001/ws"


class Participant:

    def __init__(self, websocket: WebSocket, nickname: str) -> None:
        self.id = str(uuid.uuid4())
        self.websocket = websocket
        self.nickname = nickname
        self.last_pong = asyncio.get_event_loop().time()

    def __str__(self) -> str:
        return f"{self.nickname}(id={self.id})"


class Room:
    def __init__(self, room_id: str) -> None:
        self.id = room_id
        self.participants: dict[str, Participant] = {}

    def add(self, participant: Participant) -> None:
        self.participants[participant.id] = participant

    def remove(self, participant_id: str) -> Participant | None:
        return self.participants.pop(participant_id, None)

    def get_participant_by_websocket(self, websocket: WebSocket) -> Participant | None:
        for participant in self.participants.values():
            if participant.websocket == websocket:
                return participant
        return None

    def get_info_list(self, exclude_id: str | None = None) -> list[dict[str, str]]:
        return [
            {"id": pid, "name": p.nickname}
            for pid, p in self.participants.items()
            if pid != exclude_id
        ]

    def __str__(self) -> str:
        return self.id


class RoomManager:
    def __init__(self) -> None:
        self.rooms: dict[str, Room] = {}

    def get_or_create(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]

    def remove_if_empty(self, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room and not room.participants:
            del self.rooms[room_id]
            logger.info(f"Room {room_id} deleted (empty)")

    def find_room_by_websocket(self, websocket: WebSocket) -> Room | None:
        for room in self.rooms.values():
            if room.get_participant_by_websocket(websocket):
                return room
        return None

    def remove_participant_by_websocket(self, websocket: WebSocket) -> tuple[
        Room | None, Participant | None]:
        for room in self.rooms.values():
            for pid, p in list(room.participants.items()):
                if p.websocket == websocket:
                    removed = room.remove(pid)
                    if room.participants:
                        return room, removed
                    else:
                        self.remove_if_empty(room.id)
                        return None, removed
        return None, None


room_manager = RoomManager()


async def safe_send_json(websocket: WebSocket, data: dict) -> None:
    try:
        await websocket.send_json(data)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")


async def broadcast_to_room(room: Room, message: dict, exclude_ws: WebSocket | None = None) -> None:
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
            current_participant: Participant | None,
    ) -> None:
        self.websocket = websocket
        self.room_manager = room_manager
        self.current_room = current_room
        self.current_participant = current_participant
        self.should_leave = False


RoomParticipantPair = tuple[Room | None, Participant | None]
CommandHandler = Callable[[CommandContext, dict], Awaitable[RoomParticipantPair]]


class JoinCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        room_id = data.get("room")
        nickname = data.get("nickname")
        if not room_id or not nickname:
            await safe_send_json(ctx.websocket, {
                "type": "error",
                "message": "room and nickname are required"
            })
            return ctx.current_room, ctx.current_participant

        old_room, old_participant = ctx.room_manager.remove_participant_by_websocket(ctx.websocket)
        if old_participant:
            logger.info(f"Removed {old_participant} from previous room {old_room}")

        room = ctx.room_manager.get_or_create(room_id)
        participant = Participant(ctx.websocket, nickname)
        room.add(participant)

        logger.info(f"Participant {nickname} ({participant.id}) joined room {room_id}")

        await safe_send_json(ctx.websocket, {
            "type": "joined",
            "room": room_id,
            "nickname": nickname,
            "participant_id": participant.id,
            "sfu_url": SFU_WS_URL
        })

        await broadcast_to_room(
            room,
            message={
                "type": "participant_joined",
                "participant": {"id": participant.id, "name": nickname}
            },
            exclude_ws=ctx.websocket
        )

        existing = room.get_info_list(exclude_id=participant.id)
        await safe_send_json(ctx.websocket, {
            "type": "existing_participants",
            "participants": existing
        })

        return room, participant


class PingCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        if ctx.current_participant:
            ctx.current_participant.last_pong = asyncio.get_event_loop().time()
            timestamp = data.get("timestamp")
            if timestamp:
                await safe_send_json(ctx.websocket, {
                    "type": "pong",
                    "timestamp": timestamp
                })
        return ctx.current_room, ctx.current_participant


class LeaveCommand:
    @staticmethod
    async def handle(ctx: CommandContext, data: dict) -> RoomParticipantPair:
        ctx.should_leave = True
        await safe_send_json(ctx.websocket, {
            "type": "left",
            "message": "You left the room"
        })
        return ctx.current_room, ctx.current_participant


COMMAND_HANDLERS: Final[dict[str, CommandHandler]] = {
    "join": JoinCommand.handle,
    "ping": PingCommand.handle,
    "leave": LeaveCommand.handle,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    async def heartbeat_checker():
        while True:
            await asyncio.sleep(30)
            now = asyncio.get_event_loop().time()
            # Iterate over a snapshot of rooms to avoid modification during iteration
            for room in list(room_manager.rooms.values()):
                for participant_id, participant in list(room.participants.items()):
                    if now - participant.last_pong > 60:
                        logger.warning(
                            f"Participant {participant.nickname} heartbeat timeout, closing")
                        try:
                            await participant.websocket.close(code=1000)
                        except Exception:
                            pass
                        room.remove(participant_id)
                if not room.participants:
                    room_manager.remove_if_empty(room.id)

    task = asyncio.create_task(heartbeat_checker())
    logger.info("Signaling server started")
    yield
    task.cancel()
    logger.info("Signaling server stopped")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="sfu/static"), name="static")


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
                await safe_send_json(websocket, {"type": "error", "message": "Invalid JSON"})
                continue

            msg_type = msg.get("type")
            if not msg_type:
                continue

            handler = COMMAND_HANDLERS.get(msg_type)
            if handler is None:
                logger.debug(f"Unknown message type: {msg_type}")
                await safe_send_json(websocket, {"type": "error", "message": "Unknown command"})
                continue

            ctx = CommandContext(websocket, room_manager, current_room, current_participant)
            try:
                new_room, new_participant = await handler(ctx, msg)
                current_room, current_participant = new_room, new_participant
                if ctx.should_leave:
                    break
            except Exception as e:
                logger.exception(f"Error processing command {msg_type}: {e}")
                await safe_send_json(websocket, {"type": "error", "message": "Internal error"})

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {current_participant}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        if current_room and current_participant:
            removed = current_room.remove(current_participant.id)
            if removed:
                logger.info(f"Participant {current_participant} removed from room {current_room}")
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
            logger.exception(f"Error during WebSocket close: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
