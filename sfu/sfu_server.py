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

class RelayTrack(MediaStreamTrack):
    """
    Прокси-трек для ретрансляции видео/аудио от одного участника всем остальным.
    """
    kind = "video"  # будет переопределено при создании

    def __init__(self, source_track: MediaStreamTrack):
        super().__init__()
        self.kind = source_track.kind
        self.source_track = source_track
        self._queue = asyncio.Queue()
        self._task = asyncio.create_task(self._forward())

    async def _forward(self):
        try:
            while True:
                frame = await self.source_track.recv()
                await self._queue.put(frame)
        except Exception as e:
            logger.error(f"RelayTrack forwarding error: {e}")
            await self._queue.put(None)

    async def recv(self):
        frame = await self._queue.get()
        if frame is None:
            self.stop()
            raise StopAsyncIteration
        return frame

    def stop(self):
        self._task.cancel()
        super().stop()

class SFURoom:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.participants: Dict[str, RTCPeerConnection] = {}
        # Для каждого исходного трека создаётся один RelayTrack
        self.relay_tracks: Dict[MediaStreamTrack, RelayTrack] = {}
        # Для каждого участника храним список RelayTrack, которые ему отправляются (для очистки)
        self.participant_relays: Dict[str, List[RelayTrack]] = {}

    def add_participant(self, participant_id: str, pc: RTCPeerConnection):
        self.participants[participant_id] = pc
        self.participant_relays[participant_id] = []
        logger.info(f"[{self.room_id}] Participant {participant_id} added")

    def remove_participant(self, participant_id: str):
        pc = self.participants.pop(participant_id, None)
        if pc:
            # Останавливаем и удаляем релей-треки, которые были отправлены этому участнику
            for relay in self.participant_relays.pop(participant_id, []):
                # Останавливаем только если этот релей-трек больше никому не нужен
                # Упрощённо: не останавливаем, так как он может использоваться другими
                pass
            asyncio.create_task(pc.close())
            logger.info(f"[{self.room_id}] Participant {participant_id} removed")

    async def add_track_for_participant(self, participant_id: str, source_track: MediaStreamTrack):
        """Создаём RelayTrack (если ещё не создан) и рассылаем его всем остальным участникам."""
        if source_track not in self.relay_tracks:
            self.relay_tracks[source_track] = RelayTrack(source_track)
            logger.info(f"[{self.room_id}] Created RelayTrack for {source_track.kind} from {participant_id}")

        relay = self.relay_tracks[source_track]

        # Добавляем этот релей-трек всем другим участникам (кроме автора)
        for other_id, other_pc in self.participants.items():
            if other_id == participant_id:
                continue
            logger.info(f"[{self.room_id}] Adding relay track from {participant_id} to {other_id}")
            # Используем addTransceiver с явным направлением sendonly
            transceiver = other_pc.addTransceiver(relay, direction="sendonly")
            # Сохраняем relay в список для этого участника (для возможной очистки)
            self.participant_relays.setdefault(other_id, []).append(relay)

    async def add_existing_tracks_to_new_participant(self, new_participant_id: str, new_pc: RTCPeerConnection):
        """Добавляем все существующие RelayTrack новому участнику."""
        for source_track, relay in self.relay_tracks.items():
            logger.info(f"[{self.room_id}] Adding existing relay track to newcomer {new_participant_id}")
            transceiver = new_pc.addTransceiver(relay, direction="sendonly")
            self.participant_relays.setdefault(new_participant_id, []).append(relay)

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

                @pc.on("track")
                def on_track(track):
                    logger.info(f"[{current_room.room_id}] Received track {track.kind} from {participant_id}")
                    asyncio.create_task(current_room.add_track_for_participant(participant_id, track))

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

                current_room.add_participant(participant_id, pc)
                await websocket.send(json.dumps({"type": "joined", "participant_id": participant_id}))

            elif msg_type == "offer" and pc and current_room:
                offer = RTCSessionDescription(sdp=data["sdp"], type="offer")
                await pc.setRemoteDescription(offer)
                # Добавляем существующие треки новому участнику до создания answer
                await current_room.add_existing_tracks_to_new_participant(participant_id, pc)
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await websocket.send(json.dumps({
                    "type": "answer",
                    "sdp": pc.localDescription.sdp
                }))
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
