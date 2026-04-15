import asyncio
import json
import logging
import uuid
from typing import Dict, Set, Optional, List

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.sdp import candidate_from_sdp
from websockets.legacy.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SFU")

class SFURoom:
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.participants: Dict[str, RTCPeerConnection] = {}
        self.track_to_owner: Dict[MediaStreamTrack, str] = {}
        self.participant_senders: Dict[str, Set] = {}

    def add_participant(self, participant_id: str, pc: RTCPeerConnection):
        self.participants[participant_id] = pc
        self.participant_senders[participant_id] = set()
        logger.info(f"[{self.room_id}] Participant {participant_id} added")

    def remove_participant(self, participant_id: str):
        pc = self.participants.pop(participant_id, None)
        if pc:
            # Удаляем треки этого участника из других PeerConnection
            tracks_to_remove = [t for t, owner in self.track_to_owner.items() if owner == participant_id]
            for track in tracks_to_remove:
                for other_id, other_pc in self.participants.items():
                    if other_id != participant_id:
                        for sender in other_pc.getSenders():
                            if sender.track == track:
                                other_pc.removeTrack(sender)
                                break
                del self.track_to_owner[track]
            asyncio.create_task(pc.close())
            logger.info(f"[{self.room_id}] Participant {participant_id} removed")

    async def add_track_for_participant(self, participant_id: str, track: MediaStreamTrack):
        self.track_to_owner[track] = participant_id
        for other_id, other_pc in self.participants.items():
            if other_id != participant_id:
                logger.info(f"[{self.room_id}] Adding track from {participant_id} to {other_id}")
                sender = other_pc.addTrack(track)
                self.participant_senders[other_id].add(sender)

    async def add_existing_tracks_to_new_participant(self, new_participant_id: str, new_pc: RTCPeerConnection):
        for track, owner_id in self.track_to_owner.items():
            if owner_id != new_participant_id:
                logger.info(f"[{self.room_id}] Adding existing track from {owner_id} to newcomer {new_participant_id}")
                sender = new_pc.addTrack(track)
                self.participant_senders[new_participant_id].add(sender)

rooms: Dict[str, SFURoom] = {}

async def sfu_websocket_handler(websocket: WebSocketServerProtocol):
    participant_id = str(uuid.uuid4())
    current_room: Optional[SFURoom] = None
    pc: Optional[RTCPeerConnection] = None
    pending_candidates: List = []  # список объектов RTCIceCandidate

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
                    if state == "failed" or state == "closed":
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
                await current_room.add_existing_tracks_to_new_participant(participant_id, pc)
                answer = await pc.createAnswer()
                await pc.setLocalDescription(answer)
                await websocket.send(json.dumps({
                    "type": "answer",
                    "sdp": pc.localDescription.sdp
                }))
                # Добавляем накопленные ICE-кандидаты
                if pending_candidates:
                    logger.info(f"Adding {len(pending_candidates)} pending ICE candidates for {participant_id}")
                    for candidate in pending_candidates:
                        await pc.addIceCandidate(candidate)
                    pending_candidates.clear()

            elif msg_type == "ice-candidate" and pc:
                candidate_data = data.get("candidate")
                # Завершение ICE-сбора (null или пустая строка)
                if candidate_data is None or candidate_data.get("candidate") is None or candidate_data.get("candidate") == "":
                    logger.info(f"End of ICE candidates for {participant_id}")
                    continue

                candidate_sdp = candidate_data.get("candidate")
                if not candidate_sdp:
                    continue

                # Используем candidate_from_sdp для создания объекта RTCIceCandidate
                candidate = candidate_from_sdp(candidate_sdp)
                if candidate:
                    candidate.sdpMid = candidate_data.get("sdpMid")
                    candidate.sdpMLineIndex = candidate_data.get("sdpMLineIndex", 0)

                    if pc.remoteDescription:
                        await pc.addIceCandidate(candidate)
                        logger.info(f"ICE candidate added for {participant_id}")
                    else:
                        pending_candidates.append(candidate)
                        logger.info(f"ICE candidate buffered for {participant_id} (remote description not set)")
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
    async with websockets.serve(sfu_websocket_handler, "0.0.0.0", 8001):
        logger.info("SFU server running on ws://0.0.0.0:8001")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())