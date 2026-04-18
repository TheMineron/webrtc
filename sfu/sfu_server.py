import asyncio
import json
import logging
import ssl
from typing import Dict, Set, Final, Optional

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.contrib.media import MediaRelay

import aiortc.rtcpeerconnection

original_and_direction = aiortc.rtcpeerconnection.and_direction


def patched_and_direction(a, b):
    if a is None or b is None:
        return 'inactive'
    return original_and_direction(a, b)


aiortc.rtcpeerconnection.and_direction = patched_and_direction


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
        logger.info(f"Room {self.id} participants: {list(self.participants.keys())}")

    def remove_participant(self, participant_id: str) -> None:
        if participant_id in self.participants:
            del self.participants[participant_id]
            logger.info(f"Participant {participant_id} removed from room {self.id}")
            logger.info(f"Room {self.id} participants: {list(self.participants.keys())}")

    def is_empty(self) -> bool:
        return len(self.participants) == 0

    async def broadcast_track(self, sender_id: str, track, replace_existing: bool = True) -> None:
        logger.info(
            f"Broadcasting {track.kind} track from {sender_id} to all except self, replace_existing={replace_existing}")
        for pid, participant in self.participants.items():
            if pid != sender_id:
                logger.info(f"  -> sending to {pid}")
                await participant.add_or_replace_track(sender_id, track, replace_existing)

    async def send_existing_tracks_to_newcomer(self, newcomer_id: str) -> None:
        newcomer = self.participants.get(newcomer_id)
        if not newcomer:
            logger.warning(f"Newcomer {newcomer_id} not found in room")
            return
        logger.info(f"Sending existing tracks to newcomer {newcomer_id}")
        for pid, participant in self.participants.items():
            if pid == newcomer_id:
                continue
            logger.info(
                f"  from participant {pid}, local_tracks: {[t.kind for t in participant.local_tracks]}")
            for track in participant.local_tracks:
                relayed_track = relay.subscribe(track)
                if relayed_track is None:
                    logger.error(f"Failed to subscribe to {track.kind} from {pid}")
                    continue
                logger.info(f"    subscribing to {track.kind} track from {pid}")
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
        self._renegotiation_pending = False
        self._tracks_initialized = False
        self.pending_tracks = []  # (sender_id, track, replace_existing)
        self._setup_peer_connection_handlers()

    def _setup_peer_connection_handlers(self) -> None:
        @self.peer_connection.on("track")
        async def on_track(track):
            logger.info(f"Track received from {self.id}: {track.kind}")
            self.local_tracks.add(track)
            logger.info(
                f"Participant {self.id} local_tracks now: {[t.kind for t in self.local_tracks]}")
            await self.room.broadcast_track(self.id, track)

            @track.on("ended")
            def on_ended():
                logger.info(f"Track {track.kind} ended for {self.id}")
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
                logger.info(f"Processing {len(self.pending_tracks)} pending tracks for {self.id}")
                pending = self.pending_tracks.copy()
                self.pending_tracks.clear()
                for sender_id, track, replace_existing in pending:
                    await self.add_or_replace_track(sender_id, track, replace_existing)
                # После добавления треков инициируем renegotiation
                await self.notify_renegotiation_needed()

        @self.peer_connection.on("icecandidate")
        async def on_icecandidate(candidate):
            if candidate:
                logger.info(f"Sending ICE candidate to {self.id}: {candidate.candidate[:50]}...")
                try:
                    await self.websocket.send(json.dumps({
                        "type": "ice-candidate",
                        "candidate": {
                            "candidate": candidate.candidate,
                            "sdpMid": candidate.sdpMid,
                            "sdpMLineIndex": candidate.sdpMLineIndex,
                            "usernameFragment": candidate.usernameFragment
                        }
                    }))
                except Exception as e:
                    logger.error(f"Failed to send ICE candidate to {self.id}: {e}")
            else:
                logger.info(f"ICE candidate gathering complete for {self.id}")

    async def _add_or_replace_track(self, sender_id: str, track,
                                   replace_existing: bool = True) -> None:
        logger.info(
            f"add_or_replace_track({self.id}, sender={sender_id}, kind={track.kind}, replace={replace_existing})")
        logger.info(f"  current signalingState={self.peer_connection.signalingState}")

        if self.peer_connection.signalingState != "stable":
            logger.info(
                f"Deferring add track for {sender_id} ({track.kind}) because state is {self.peer_connection.signalingState}")
            self.pending_tracks.append((sender_id, track, replace_existing))
            return

        if sender_id not in self.remote_senders:
            self.remote_senders[sender_id] = {}
            logger.info(f"Created remote_senders entry for {sender_id}")

        senders = self.remote_senders[sender_id]
        existing_sender = senders.get(track.kind)

        if replace_existing and existing_sender:
            logger.info(f"Replacing {track.kind} track from {sender_id} to {self.id}")
            await existing_sender.replaceTrack(track)
            return

        if not replace_existing and existing_sender:
            logger.warning(
                f"Track {track.kind} from {sender_id} already exists, skipping (replace_existing=False)")
            return

        for transceiver in self.peer_connection.getTransceivers():
            logger.info(f"transceiver: {transceiver.kind=} "
                        f"{transceiver.direction=} "
                        f"{transceiver.mid=}"
                        f"{transceiver.sender=}"
                        f"{transceiver.receiver=}")
            if transceiver.sender.track is None and transceiver.direction in (
            'sendonly', 'sendrecv'):
                # Можно использовать этот трансивер
                break
        else:
            logger.info("Создается новый трансивер")
            self.peer_connection.addTransceiver(track.kind, direction='sendonly')

        # Новый трек – используем addTrack
        logger.info(f"Adding new {track.kind} track via addTrack for {sender_id}")
        sender = self.peer_connection.addTrack(track)
        if sender is None:
            logger.error(f"addTrack returned None for {track.kind}")
            return
        senders[track.kind] = sender
        logger.info(f"remote_senders[{sender_id}] now: {list(senders.keys())}")
        await self.notify_renegotiation_needed()

    async def add_or_replace_track(self, sender_id: str, track,
                                   replace_existing: bool = True) -> None:
        logger.info(
            f"add_or_replace_track({self.id}, sender={sender_id}, kind={track.kind}, replace={replace_existing})")
        logger.info(f"  current signalingState={self.peer_connection.signalingState}")

        if self.peer_connection.signalingState != "stable":
            logger.info(
                f"Deferring add track for {sender_id} ({track.kind}) because state is {self.peer_connection.signalingState}")
            self.pending_tracks.append((sender_id, track, replace_existing))
            return

        if sender_id not in self.remote_senders:
            self.remote_senders[sender_id] = {}
            logger.info(f"Created remote_senders entry for {sender_id}")

        senders = self.remote_senders[sender_id]
        existing_sender = senders.get(track.kind)

        # Проверяем, действительно ли sender активен и трек прикреплён
        sender_valid = False
        if existing_sender:
            try:
                # aiortc RTCRtpSender имеет свойство track
                if existing_sender.track is not None:
                    sender_valid = True
            except Exception:
                pass

        if replace_existing and existing_sender and sender_valid:
            logger.info(f"Replacing {track.kind} track from {sender_id} to {self.id}")
            await existing_sender.replaceTrack(track)
            return

        if not replace_existing and existing_sender and sender_valid:
            logger.warning(
                f"Track {track.kind} from {sender_id} already exists and is active, skipping (replace_existing=False)")
            return

        # Если sender есть, но невалидный – удаляем его из словаря и пересоздаём
        if existing_sender and not sender_valid:
            logger.info(f"Removing invalid sender for {sender_id} ({track.kind})")
            del senders[track.kind]

        # Создаём новый трансивер с direction='sendonly', если нет свободного
        transceiver = None
        for t in self.peer_connection.getTransceivers():
            logger.info(f"transceiver: {t.kind=} "
                        f"{t.direction=} "
                        f"{t.mid=}"
                        f"{t.sender=}"
                        f"{t.receiver=}")
            if t.direction in ('sendonly', 'sendrecv') and t.sender.track is None:
                transceiver = t
                break
        if not transceiver:
            logger.info(f"Creating new transceiver for {track.kind} (sendonly)")
            transceiver = self.peer_connection.addTransceiver(track.kind, direction='sendonly')
        else:
            logger.info(f"Reusing existing transceiver (mid={transceiver.mid}) for {track.kind}")

        logger.info(f"Adding new {track.kind} track via sender.replaceTrack for {sender_id}")
        transceiver.sender.replaceTrack(track)
        senders[track.kind] = transceiver.sender
        logger.info(f"remote_senders[{sender_id}] now: {list(senders.keys())}")

        await self.notify_renegotiation_needed()

    async def notify_renegotiation_needed(self) -> None:
        if self._renegotiation_pending:
            logger.info(f"Renegotiation already pending for {self.id}, skipping")
            return
        self._renegotiation_pending = True
        try:
            logger.info(f"Sending renegotiate to {self.id}")
            await self.websocket.send(json.dumps({"type": "renegotiate"}))
        except Exception as e:
            logger.error(f"Failed to send renegotiate notification: {e}")

    async def close(self) -> None:
        logger.info(f"Closing participant {self.id}, remote_senders: {self.remote_senders}")
        for pid, participant in self.room.participants.items():
            if pid != self.id and self.id in participant.remote_senders:
                for kind, sender in participant.remote_senders[self.id].items():
                    try:
                        if sender is not None:
                            result = sender.replaceTrack(None)
                            if result is not None and hasattr(result, "__await__"):
                                await result
                    except Exception as e:
                        logger.warning(f"Failed to stop track {kind} sender: {e}")
                del participant.remote_senders[self.id]
                await participant.notify_renegotiation_needed()

        self.room.remove_participant(self.id)
        await self.peer_connection.close()

    def __str__(self) -> str:
        return f"{self.id}"


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
            logger.info(f"Room {room_id} deleted (empty)")

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
            logger.info(f"Received message from {participant_id or '?'}: type={msg_type}")

            if msg_type == "join":
                room_id = data.get("room")
                participant_id = data.get("participant_id")
                if not room_id or not participant_id:
                    await websocket.send(json.dumps(
                        {"type": "error", "message": "room and participant_id required"}))
                    continue

                room = room_manager.get_or_create(room_id)
                participant = Participant(participant_id, room, websocket)
                room.add_participant(participant)
                await websocket.send(json.dumps({"type": "joined", "room": room_id}))
                logger.info(f"Joined: {participant_id} in room {room_id}")

            elif msg_type == "offer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue

                logger.info(f"Received offer from {participant.id}")
                offer = RTCSessionDescription(sdp=data["sdp"], type=data["type"])
                await participant.peer_connection.setRemoteDescription(offer)
                logger.info(
                    f"After setRemoteDescription, signalingState={participant.peer_connection.signalingState}")

                if not participant._tracks_initialized:
                    logger.info(f"Initializing tracks for newcomer {participant.id}")
                    await room.send_existing_tracks_to_newcomer(participant.id)
                    participant._tracks_initialized = True

                # Всегда отвечаем на offer, если состояние "have-remote-offer"
                if participant.peer_connection.signalingState == "have-remote-offer":
                    try:
                        answer = await participant.peer_connection.createAnswer()
                        await participant.peer_connection.setLocalDescription(answer)
                        await websocket.send(json.dumps({
                            "type": "answer",
                            "sdp": participant.peer_connection.localDescription.sdp,
                        }))
                        logger.info(f"Sent answer to {participant.id}")
                    except Exception as e:
                        logger.error(f"Failed to create/send answer: {e}", exc_info=True)
                        await participant.notify_renegotiation_needed()
                    finally:
                        participant._renegotiation_pending = False
                else:
                    logger.warning(
                        f"Unexpected signaling state after setRemoteDescription: {participant.peer_connection.signalingState}")

            elif msg_type == "answer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue

                answer = RTCSessionDescription(sdp=data["sdp"], type="answer")
                logger.info(f"SDP answer for {participant.id}:\n{answer.sdp}")
                await participant.peer_connection.setRemoteDescription(answer)
                participant._renegotiation_pending = False
                logger.info(f"Received answer for {participant.id}")

            elif msg_type == "ice-candidate":
                if not participant:
                    continue

                candidate_data = data.get("candidate")
                if not candidate_data or not candidate_data.get("candidate"):
                    continue

                candidate_str = candidate_data.get("candidate", "").strip()
                if not candidate_str:
                    continue

                try:
                    candidate = create_ice_candidate_from_js(candidate_data)
                    await participant.peer_connection.addIceCandidate(candidate)
                    logger.info(f"Added ICE candidate for {participant.id}: {candidate_str[:50]}")
                except Exception as e:
                    logger.warning(f"Failed to add ICE candidate: {e}")

            elif msg_type == "leave":
                break

            else:
                await websocket.send(
                    json.dumps({"type": "error", "message": f"Unknown message type: {msg_type}"}))

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
        logger.warning("SSL certificates not found, using unencrypted WebSocket (ws://)")
        ssl_context = None
        proto = "ws"

    async with websockets.serve(handle_client, "0.0.0.0", 8001, ssl=ssl_context, max_size=10 ** 7):
        logger.info(f"SFU server started on {proto}://0.0.0.0:8001")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())