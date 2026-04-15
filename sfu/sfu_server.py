import asyncio
import json
import logging
import ssl
import uuid
from typing import Dict, Optional, List

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.sdp import candidate_from_sdp
from websockets.legacy.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SFU")

# ---------------------- Fan-out трек для рассылки кадров ----------------------
class BroadcastTrack(MediaStreamTrack):
    """
    Читает кадры из source_track и копирует их во все очереди подписчиков.
    """
    kind = "video"  # будет переопределён

    def __init__(self, source_track: MediaStreamTrack):
        super().__init__()
        self.kind = source_track.kind
        self._source_track = source_track
        self._subscribers: List[asyncio.Queue] = []
        self._task = asyncio.create_task(self._broadcast())
        self._closed = False

    async def _broadcast(self):
        try:
            while True:
                frame = await self._source_track.recv()
                if self._closed:
                    break
                # Отправляем копию кадра всем подписчикам
                for q in self._subscribers:
                    try:
                        q.put_nowait(frame)
                    except asyncio.QueueFull:
                        logger.warning("BroadcastTrack queue full, dropping frame")
        except Exception as e:
            logger.error(f"BroadcastTrack error: {e}")
        finally:
            # Сигнализируем подписчикам о закрытии
            for q in self._subscribers:
                q.put_nowait(None)

    def subscribe(self) -> asyncio.Queue:
        """Подписывает нового получателя и возвращает его очередь."""
        q = asyncio.Queue(maxsize=10)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        """Отписывает получателя."""
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def recv(self):
        """Этот метод не должен вызываться напрямую — используйте подписку."""
        raise NotImplementedError("Use subscribe()")

    def stop(self):
        self._closed = True
        self._task.cancel()
        super().stop()


class RelayTrack(MediaStreamTrack):
    """
    Прокси-трек для каждого получателя. Получает кадры из своей очереди,
    которая наполняется BroadcastTrack'ом.
    """
    kind = "video"

    def __init__(self, queue: asyncio.Queue, kind: str):
        super().__init__()
        self.kind = kind
        self._queue = queue

    async def recv(self):
        frame = await self._queue.get()
        if frame is None:
            self.stop()
            raise StopAsyncIteration
        return frame


# ---------------------- Комната SFU ----------------------
class SFURoom:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.participants: Dict[str, RTCPeerConnection] = {}
        # Для каждого исходного трека храним BroadcastTrack (один на всех)
        self.broadcast_tracks: Dict[MediaStreamTrack, BroadcastTrack] = {}
        # Для каждого участника храним список RelayTrack (чтобы закрыть при выходе)
        self.participant_relays: Dict[str, List[RelayTrack]] = {}

    def add_participant(self, participant_id: str, pc: RTCPeerConnection):
        self.participants[participant_id] = pc
        self.participant_relays[participant_id] = []
        logger.info(f"[{self.room_id}] Participant {participant_id} added")

    def remove_participant(self, participant_id: str):
        pc = self.participants.pop(participant_id, None)
        # Закрываем все RelayTrack'ы этого участника
        for relay in self.participant_relays.pop(participant_id, []):
            relay.stop()
        if pc:
            asyncio.create_task(pc.close())
        logger.info(f"[{self.room_id}] Participant {participant_id} removed")

    async def add_existing_tracks_to_new_participant(self, new_participant_id: str, new_pc: RTCPeerConnection):
        """Добавляет все существующие BroadcastTrack'и новому участнику (до установки remote description)."""
        for source_track, broadcast in self.broadcast_tracks.items():
            # Подписываемся на BroadcastTrack
            queue = broadcast.subscribe()
            relay = RelayTrack(queue, broadcast.kind)
            # Добавляем трек в peer connection нового участника
            new_pc.addTrack(relay)
            self.participant_relays.setdefault(new_participant_id, []).append(relay)
            logger.info(f"[{self.room_id}] Added existing {broadcast.kind} relay to newcomer {new_participant_id}")

    async def add_track_from_participant(self, participant_id: str, source_track: MediaStreamTrack):
        """
        Вызывается, когда участник опубликовал новый трек.
        Создаёт BroadcastTrack (если ещё не создан) и сохраняет его для будущих участников.
        """
        if source_track not in self.broadcast_tracks:
            broadcast = BroadcastTrack(source_track)
            self.broadcast_tracks[source_track] = broadcast
            logger.info(f"[{self.room_id}] Created BroadcastTrack for {source_track.kind} from {participant_id}")

        # Примечание: уже подключённым участникам этот трек НЕ отправляется,
        # чтобы избежать renegotiation. Новые участники получат его при подключении.


# ---------------------- WebSocket обработчик SFU ----------------------
rooms: Dict[str, SFURoom] = {}

async def sfu_websocket_handler(websocket: WebSocketServerProtocol):
    participant_id = str(uuid.uuid4())
    current_room: Optional[SFURoom] = None
    pc: Optional[RTCPeerConnection] = None
    pending_candidates: List = []

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "join":
                room_id = data.get("room")
                if not room_id:
                    await websocket.send(json.dumps({"type": "error", "message": "room required"}))
                    continue

                if room_id not in rooms:
                    rooms[room_id] = SFURoom(room_id)
                current_room = rooms[room_id]

                pc = RTCPeerConnection()

                # Обработка входящих треков от участника
                @pc.on("track")
                def on_track(track):
                    logger.info(f"[{current_room.room_id}] Received track {track.kind} from {participant_id}")
                    # Сохраняем трек для будущих участников (но не рассылаем текущим)
                    asyncio.create_task(current_room.add_track_from_participant(participant_id, track))

                @pc.on("iceconnectionstatechange")
                def on_ice_state():
                    state = pc.iceConnectionState
                    logger.info(f"[{current_room.room_id}] ICE state for {participant_id}: {state}")
                    if state in ["failed", "closed", "disconnected"]:
                        asyncio.create_task(handle_disconnect())

                @pc.on("connectionstatechange")
                def on_connection_state():
                    state = pc.connectionState
                    logger.info(f"[{current_room.room_id}] Connection state for {participant_id}: {state}")
                    if state in ["failed", "closed"]:
                        asyncio.create_task(handle_disconnect())

                async def handle_disconnect():
                    if current_room:
                        current_room.remove_participant(participant_id)
                        try:
                            await websocket.close()
                        except:
                            pass

                # 1) Добавляем участника в комнату
                current_room.add_participant(participant_id, pc)

                # 2) Добавляем все существующие треки ДО того, как получим offer
                await current_room.add_existing_tracks_to_new_participant(participant_id, pc)

                await websocket.send(json.dumps({"type": "joined", "participant_id": participant_id}))

            elif msg_type == "offer" and pc and current_room:
                offer = RTCSessionDescription(sdp=data["sdp"], type="offer")
                await pc.setRemoteDescription(offer)

                # 3) Создаём answer (больше не добавляем треки)
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)

                await websocket.send(json.dumps({
                    "type": "answer",
                    "sdp": pc.localDescription.sdp
                }))

                # Добавляем отложенные ICE-кандидаты
                if pending_candidates:
                    logger.info(f"Adding {len(pending_candidates)} pending ICE candidates for {participant_id}")
                    for candidate in pending_candidates:
                        await pc.addIceCandidate(candidate)
                    pending_candidates.clear()

            elif msg_type == "ice-candidate" and pc:
                candidate_data = data.get("candidate")
                if candidate_data is None or candidate_data.get("candidate") is None:
                    logger.info(f"End of ICE candidates for {participant_id}")
                    continue

                candidate_sdp = candidate_data.get("candidate")
                if not candidate_sdp:
                    continue

                candidate = candidate_from_sdp(candidate_sdp)
                if candidate:
                    candidate.sdpMid = candidate_data.get("sdpMid")
                    candidate.sdpMLineIndex = candidate_data.get("sdpMLineIndex", 0)

                    if pc.remoteDescription:
                        await pc.addIceCandidate(candidate)
                        logger.info(f"ICE candidate added for {participant_id}")
                    else:
                        pending_candidates.append(candidate)
                        logger.info(f"ICE candidate buffered for {participant_id}")
                else:
                    logger.warning(f"Failed to parse ICE candidate SDP: {candidate_sdp}")

            elif msg_type == "ping":
                await websocket.send(json.dumps({"type": "pong", "timestamp": data.get("timestamp")}))

            else:
                logger.warning(f"Unknown message type from {participant_id}: {msg_type}")

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"SFU WebSocket closed for {participant_id}")
    except Exception as e:
        logger.exception(f"SFU error for {participant_id}: {e}")
    finally:
        if current_room and participant_id:
            current_room.remove_participant(participant_id)
        if pc:
            await pc.close()
        if current_room and not current_room.participants:
            rooms.pop(current_room.room_id, None)
            logger.info(f"Room {current_room.room_id} deleted (empty)")

# ---------------------- Запуск сервера ----------------------
async def main():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain('cert.pem', 'key.pem')

    async with websockets.serve(
        sfu_websocket_handler,
        "0.0.0.0",
        8001,
        ssl=ssl_context
    ):
        logger.info("SFU server running on wss://0.0.0.0:8001")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())