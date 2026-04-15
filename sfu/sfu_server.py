import asyncio
import json
import logging
import ssl
import uuid
from typing import Dict, Optional

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay
from aiortc.sdp import candidate_from_sdp
from websockets.legacy.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SFU")

class SFURoom:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.participants: Dict[str, RTCPeerConnection] = {}
        self.relay = MediaRelay()
        self.published_tracks: Dict[str, Dict[str, MediaStreamTrack]] = {}  # participant_id -> {kind: track}

    def add_participant(self, participant_id: str, pc: RTCPeerConnection):
        self.participants[participant_id] = pc
        logger.info(f"[{self.room_id}] Participant {participant_id} added")

    def remove_participant(self, participant_id: str):
        pc = self.participants.pop(participant_id, None)
        if pc:
            asyncio.create_task(pc.close())
        self.published_tracks.pop(participant_id, None)
        logger.info(f"[{self.room_id}] Participant {participant_id} removed")

    def add_existing_tracks_to(self, target_pc: RTCPeerConnection):
        """Добавить все ранее опубликованные треки новому участнику."""
        for src_id, tracks in self.published_tracks.items():
            for kind, src_track in tracks.items():
                relayed = self.relay.subscribe(src_track)
                target_pc.addTrack(relayed)
                logger.info(f"[{self.room_id}] Added existing {kind} from {src_id} to newcomer")

    def publish_track(self, participant_id: str, track: MediaStreamTrack):
        """Сохраняет оригинальный трек и рассылает его всем остальным участникам."""
        if participant_id not in self.published_tracks:
            self.published_tracks[participant_id] = {}
        self.published_tracks[participant_id][track.kind] = track
        logger.info(f"[{self.room_id}] Published {track.kind} from {participant_id}")

        # Рассылаем этот трек всем остальным участникам
        for other_id, other_pc in self.participants.items():
            if other_id != participant_id:
                relayed = self.relay.subscribe(track)
                other_pc.addTrack(relayed)
                logger.info(f"[{self.room_id}] Sent {track.kind} from {participant_id} to {other_id}")

rooms: Dict[str, SFURoom] = {}

async def sfu_websocket_handler(websocket: WebSocketServerProtocol):
    participant_id = str(uuid.uuid4())
    current_room: Optional[SFURoom] = None
    pc: Optional[RTCPeerConnection] = None
    pending_candidates = []

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
                    current_room.publish_track(participant_id, track)

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

                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await websocket.send(json.dumps({
                    "type": "answer",
                    "sdp": pc.localDescription.sdp
                }))

                if pending_candidates:
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