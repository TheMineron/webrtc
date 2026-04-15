import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("signaling")


class ParticipantInfo:
    def __init__(self, websocket: WebSocket, nickname: str, room_id: str):
        self.id = str(uuid.uuid4())
        self.websocket = websocket
        self.nickname = nickname
        self.room_id = room_id
        self.last_pong = asyncio.get_event_loop().time()


class RoomInfo:
    def __init__(self, room_id: str):
        self.id = room_id
        self.participants: Dict[str, ParticipantInfo] = {}

rooms: Dict[str, RoomInfo] = {}

SFU_WS_URL = "ws://localhost:8001/ws"

@asynccontextmanager
async def lifespan(app: FastAPI):
    async def heartbeat_checker():
        while True:
            await asyncio.sleep(30)
            now = asyncio.get_event_loop().time()
            for room in list(rooms.values()):
                for pid, participant in list(room.participants.items()):
                    if now - participant.last_pong > 60:
                        logger.warning(f"Participant {participant.nickname} heartbeat timeout, closing")
                        try:
                            await participant.websocket.close(code=1000)
                        except:
                            pass

                        room.participants.pop(pid, None)
                        if not room.participants:
                            rooms.pop(room.id, None)
    asyncio.create_task(heartbeat_checker())
    logger.info("Signaling server started")
    yield
    logger.info("Signaling server stopped")

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.websocket("/ws")
async def signaling_websocket(websocket: WebSocket):
    await websocket.accept()
    current_participant: Optional[ParticipantInfo] = None
    current_room: Optional[RoomInfo] = None
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "message": "Invalid JSON"})
                continue

            msg_type = msg.get("type")
            if msg_type == "join":
                room_id = msg.get("room")
                nickname = msg.get("nickname")
                if not room_id or not nickname:
                    await websocket.send_json({"type": "error", "message": "room and nickname required"})
                    continue

                old_room = None
                for r in rooms.values():
                    for p in r.participants.values():
                        if p.websocket == websocket:
                            old_room = r
                            break
                    if old_room:
                        break
                if old_room:
                    for pid, p in list(old_room.participants.items()):
                        if p.websocket == websocket:
                            old_room.participants.pop(pid)
                            break
                    if not old_room.participants:
                        rooms.pop(old_room.id, None)
                if room_id not in rooms:
                    rooms[room_id] = RoomInfo(room_id)
                current_room = rooms[room_id]
                current_participant = ParticipantInfo(websocket, nickname, room_id)
                current_room.participants[current_participant.id] = current_participant
                logger.info(f"Participant {nickname} ({current_participant.id}) joined room {room_id}")

                await websocket.send_json({
                    "type": "joined",
                    "room": room_id,
                    "nickname": nickname,
                    "participant_id": current_participant.id,
                    "sfu_url": SFU_WS_URL
                })

                for p in current_room.participants.values():
                    if p.id != current_participant.id:
                        await p.websocket.send_json({
                            "type": "participant_joined",
                            "participant": {"id": current_participant.id, "name": nickname}
                        })
            elif msg_type == "ping":
                if current_participant:
                    current_participant.last_pong = asyncio.get_event_loop().time()
                    await websocket.send_json({"type": "pong", "timestamp": msg.get("timestamp")})
            elif msg_type == "leave":
                break
            else:
                await websocket.send_json({"type": "error", "message": "Unknown command"})
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {current_participant.nickname if current_participant else 'unknown'}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        if current_room and current_participant:
            current_room.participants.pop(current_participant.id, None)
            logger.info(f"Participant {current_participant.nickname} removed from room {current_room.id}")
            for p in current_room.participants.values():
                try:
                    await p.websocket.send_json({
                        "type": "participant_left",
                        "participant_id": current_participant.id
                    })
                except:
                    pass
            if not current_room.participants:
                rooms.pop(current_room.id, None)
        try:
            await websocket.close()
        except:
            pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
