import asyncio
import json
import logging
import ssl
import struct
from typing import Dict, Set, Final, Optional

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.contrib.media import MediaRelay
from aiortc import rtp



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HEARTBEAT_TIMEOUT: Final[int] = 60
ICE_CONSENT_TIMEOUT: Final[int] = 60

relay = MediaRelay()


class Room:
    def __init__(self, room_id: str) -> None:
        self.id = room_id
        self.participants: Dict[str, "Participant"] = {}

    def add_participant(self, participant: "Participant") -> None:
        self.participants[participant.id] = participant
        logger.info(f"Participant {participant.id} added to room {self.id}")

    def remove_participant(self, participant_id: str) -> None:
        if participant_id in self.participants:
            del self.participants[participant_id]
            logger.info(f"Participant {participant_id} removed from room {self.id}")

    def is_empty(self) -> bool:
        return len(self.participants) == 0

    async def broadcast_track(self, sender_id: str, track, replace_existing: bool = True) -> None:
        for pid, participant in self.participants.items():
            if pid != sender_id:
                await participant.add_or_replace_track(sender_id, track, replace_existing)

    async def send_existing_tracks_to_newcomer(self, newcomer_id: str) -> None:
        newcomer = self.participants.get(newcomer_id)
        if not newcomer:
            return
        for pid, participant in self.participants.items():
            if pid == newcomer_id:
                continue
            for track in participant.local_tracks:
                relayed_track = relay.subscribe(track)
                await newcomer.add_or_replace_track(pid, relayed_track, replace_existing=False)

    def __str__(self) -> str:
        return self.id


class Participant:
    def __init__(self, participant_id: str, room: Room, websocket) -> None:
        self.id = participant_id
        self.room = room
        self.websocket = websocket
        self.peer_connection = RTCPeerConnection()
        self.local_tracks: Set = set()
        self.remote_senders: Dict[str, Dict[str, object]] = {}
        self._tracks_initialized = False
        self.pending_tracks = []
        self._renegotiation_pending = False
        self._renegotiation_timeout_task = None
        self._setup_peer_connection_handlers()

    def _setup_peer_connection_handlers(self) -> None:
        @self.peer_connection.on("track")
        async def on_track(track):
            logger.info(f"Track received from {self.id}: {track.kind}")
            self.local_tracks.add(track)
            await self.room.broadcast_track(self.id, track)
            @track.on("ended")
            def on_ended():
                self.local_tracks.discard(track)

        @self.peer_connection.on("connectionstatechange")
        async def on_connectionstatechange():
            state = self.peer_connection.connectionState
            logger.info(f"Connection state for {self.id}: {state}")
            if state in ("failed", "closed"):
                await self.close()

        @self.peer_connection.on("iceconnectionstatechange")
        async def on_iceconnectionstatechange():
            state = self.peer_connection.iceConnectionState
            logger.info(f"ICE connection state for {self.id}: {state}")

        @self.peer_connection.on("icegatheringstatechange")
        async def on_icegatheringstatechange():
            state = self.peer_connection.iceGatheringState
            logger.info(f"ICE gathering state for {self.id}: {state}")

        @self.peer_connection.on("signalingstatechange")
        async def on_signalingstatechange():
            state = self.peer_connection.signalingState
            logger.info(f"Signaling state for {self.id}: {state}")
            if state == "stable" and self.pending_tracks:
                pending = self.pending_tracks.copy()
                self.pending_tracks.clear()
                for sender_id, track, replace_existing in pending:
                    await self.add_or_replace_track(sender_id, track, replace_existing)

    async def add_or_replace_track(self, sender_id: str, track, replace_existing: bool = True) -> None:
        if self.peer_connection.signalingState != "stable":
            logger.info(f"Deferring add track for {sender_id} ({track.kind}) because state is {self.peer_connection.signalingState}")
            self.pending_tracks.append((sender_id, track, replace_existing))
            return

        if sender_id not in self.remote_senders:
            self.remote_senders[sender_id] = {}

        senders = self.remote_senders[sender_id]
        existing_sender = senders.get(track.kind)

        if replace_existing and existing_sender:
            logger.info(f"Replacing {track.kind} track from {sender_id}")
            existing_sender.replaceTrack(track)   # синхронный метод
            return

        if not replace_existing and existing_sender:
            logger.warning(f"Track {track.kind} already exists, skipping")
            return

        logger.info(f"Adding new {track.kind} track via addTrack for {sender_id}")
        sender = self.peer_connection.addTrack(track)
        if sender is None:
            logger.error(f"addTrack returned None for {track.kind}")
            return
        senders[track.kind] = sender
        await self.send_offer()

    async def send_offer(self) -> None:
        if self._renegotiation_pending:
            logger.info(f"Renegotiation already pending for {self.id}")
            return
        if self.peer_connection.signalingState != "stable":
            logger.warning(f"Cannot send offer, state is {self.peer_connection.signalingState}")
            return
        self._renegotiation_pending = True
        try:
            offer = await self.peer_connection.createOffer()
            await self.peer_connection.setLocalDescription(offer)
            await self.websocket.send(json.dumps({"type": "offer", "sdp": offer.sdp}))
            logger.info(f"Sent offer to {self.id}")
            self._renegotiation_timeout_task = asyncio.create_task(self._renegotiation_timeout())
        except Exception as e:
            logger.error(f"Failed to send offer: {e}")
            self._renegotiation_pending = False

    async def _renegotiation_timeout(self):
        await asyncio.sleep(10)
        if self._renegotiation_pending:
            logger.warning(f"Renegotiation timeout for {self.id}, resetting flag")
            self._renegotiation_pending = False

    async def close(self) -> None:
        logger.info(f"Closing participant {self.id}")
        for pid, participant in self.room.participants.items():
            if pid != self.id and self.id in participant.remote_senders:
                for kind, sender in participant.remote_senders[self.id].items():
                    try:
                        if sender:
                            sender.replaceTrack(None)
                    except Exception as e:
                        logger.warning(f"Failed to stop track {kind} sender: {e}")
                del participant.remote_senders[self.id]
                await participant.send_offer()
        self.room.remove_participant(self.id)
        await self.peer_connection.close()

    def __str__(self) -> str:
        return self.id


class RoomManager:
    def __init__(self) -> None:
        self.rooms: Dict[str, Room] = {}

    def get_or_create(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]

    def remove_if_empty(self, room_id: str) -> None:
        room = self.rooms.get(room_id)
        if room and room.is_empty():
            del self.rooms[room_id]
            logger.info(f"Room {room_id} deleted")

    def find_room_by_participant_id(self, participant_id: str) -> Optional[Room]:
        for room in self.rooms.values():
            if participant_id in room.participants:
                return room
        return None


room_manager = RoomManager()


def create_ice_candidate_from_js(candidate_data: dict) -> RTCIceCandidate:
    return RTCIceCandidate(
        component=candidate_data.get("component", 1),
        foundation=candidate_data.get("foundation", ""),
        ip=candidate_data.get("ip", ""),
        port=candidate_data.get("port", 0),
        priority=candidate_data.get("priority", 0),
        protocol=candidate_data.get("protocol", ""),
        type=candidate_data.get("type", ""),
        relatedAddress=candidate_data.get("relatedAddress"),
        relatedPort=candidate_data.get("relatedPort"),
        sdpMid=candidate_data.get("sdpMid"),
        sdpMLineIndex=candidate_data.get("sdpMLineIndex"),
        tcpType=candidate_data.get("tcpType"),
    )


async def handle_client(websocket) -> None:
    participant_id = None
    room = None
    participant = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                continue

            msg_type = data.get("type")

            if msg_type == "join":
                room_id = data.get("room")
                participant_id = data.get("participant_id")
                if not room_id or not participant_id:
                    await websocket.send(json.dumps({"type": "error", "message": "room and participant_id required"}))
                    continue
                room = room_manager.get_or_create(room_id)
                participant = Participant(participant_id, room, websocket)
                room.add_participant(participant)
                await websocket.send(json.dumps({"type": "joined", "room": room_id}))

            elif msg_type == "offer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue
                logger.info(f"Received offer from {participant.id}")
                offer = RTCSessionDescription(sdp=data["sdp"], type=data["type"])
                await participant.peer_connection.setRemoteDescription(offer)

                if not participant._tracks_initialized:
                    await room.send_existing_tracks_to_newcomer(participant.id)
                    participant._tracks_initialized = True

                if participant.peer_connection.signalingState == "have-remote-offer":
                    try:
                        answer = await participant.peer_connection.createAnswer()
                        await participant.peer_connection.setLocalDescription(answer)
                        await websocket.send(json.dumps({"type": "answer", "sdp": answer.sdp}))
                    except Exception as e:
                        logger.error(f"Failed to create/send answer: {e}", exc_info=True)
                    participant._renegotiation_pending = False
                    if participant._renegotiation_timeout_task:
                        participant._renegotiation_timeout_task.cancel()

            elif msg_type == "answer":
                if not participant:
                    continue
                answer = RTCSessionDescription(sdp=data["sdp"], type="answer")
                await participant.peer_connection.setRemoteDescription(answer)
                participant._renegotiation_pending = False
                if participant._renegotiation_timeout_task:
                    participant._renegotiation_timeout_task.cancel()

            elif msg_type == "ice-candidate":
                if not participant:
                    continue
                candidate_data = data.get("candidate")
                if not candidate_data or not candidate_data.get("candidate"):
                    continue
                try:
                    candidate = create_ice_candidate_from_js(candidate_data)
                    await participant.peer_connection.addIceCandidate(candidate)
                except Exception as e:
                    logger.warning(f"Failed to add ICE candidate: {e}")

            elif msg_type == "leave":
                break

            else:
                await websocket.send(json.dumps({"type": "error", "message": f"Unknown message type: {msg_type}"}))

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"WebSocket closed for {participant_id}")
    except Exception as e:
        logger.exception(f"Error in client handler: {e}")
    finally:
        if participant:
            await participant.close()
            if room and room.is_empty():
                room_manager.remove_if_empty(room.id)
        logger.info(f"Client {participant_id} disconnected")


async def main() -> None:
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        ssl_context.load_cert_chain("cert.pem", "key.pem")
        proto = "wss"
    except FileNotFoundError:
        logger.warning("SSL certificates not found, using ws://")
        ssl_context = None
        proto = "ws"

    async with websockets.serve(handle_client, "0.0.0.0", 8001, ssl=ssl_context, max_size=10**7):
        logger.info(f"SFU server started on {proto}://0.0.0.0:8001")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())