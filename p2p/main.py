import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import uvicorn

from commands.register import get_commands_handlers
from .room_manager import RoomManager
from .utils import CommandContext, safe_send_json, broadcast_to_room
from database.redis_client import init_redis, close_redis, redis_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

room_manager = RoomManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_redis()
    logger.info("Signaling server started")
    yield
    await close_redis()
    logger.info("Signaling server stopped")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="p2p/static"), name="static")


command_handlers = get_commands_handlers()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    current_room = None
    current_participant = None

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

            handler = command_handlers.get(msg_type)
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
            removed = await room_manager.remove_participant(current_room, current_participant.id)
            if removed:
                logger.info(f"Участник {current_participant} удалён из комнаты {current_room}")
                await broadcast_to_room(
                    current_room,
                    message={"type": "participant_left", "participant_id": current_participant.id},
                    exclude_ws=websocket
                )
                await room_manager.remove_if_empty(current_room.id)
        try:
            await websocket.close()
        except Exception:
            logger.exception("Ошибка при закрытии WebSocket")


@app.get("/health")
async def health():
    if await redis_client.ping():
        return {"status": "ok"}
    return {"status": "redis_down"}, 503


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
