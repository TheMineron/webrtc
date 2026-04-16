import asyncio
import json
import logging
import ssl
from typing import Dict, Set

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.contrib.media import MediaRelay

# ----------------------------------------------------------------------
# ПАТЧ: отключение проверки "consent freshness" для всех ICE-транспортов
import aiortc.rtcicetransport

_original_init = aiortc.rtcicetransport.RTCIceTransport.__init__

def _patched_init(self, *args, **kwargs):
    _original_init(self, *args, **kwargs)
    self._consent_timeout = None

aiortc.rtcicetransport.RTCIceTransport.__init__ = _patched_init
# ----------------------------------------------------------------------

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sfu")

relay = MediaRelay()


class Room:
    def __init__(self, room_id: str):
        self.id = room_id
        self.participants: Dict[str, "Participant"] = {}

    def add_participant(self, participant: "Participant"):
        self.participants[participant.id] = participant
        logger.info(f"Participant {participant.id} added to room {self.id}")

    def remove_participant(self, participant_id: str):
        if participant_id in self.participants:
            del self.participants[participant_id]
            logger.info(f"Participant {participant_id} removed from room {self.id}")
            if not self.participants:
                rooms.pop(self.id, None)
                logger.info(f"Room {self.id} deleted (empty)")

    async def broadcast_track(self, sender_id: str, track, replace_existing: bool = True):
        logger.debug(
            f"broadcast_track: sender={sender_id}, "
            f"kind={track.kind}, "
            f"replace={replace_existing}, "
            f"participants={list(self.participants.keys())}"
        )

        for pid, p in self.participants.items():
            if pid != sender_id:
                logger.info(f"Relaying track {track.kind} from {sender_id} to {pid}")
                relayed_track = relay.subscribe(track)
                await p.add_or_replace_track(sender_id, relayed_track, replace_existing)

    async def send_existing_tracks_to_newcomer(self, newcomer_id: str):
        newcomer = self.participants.get(newcomer_id)
        if not newcomer:
            return

        for pid, p in self.participants.items():
            if pid == newcomer_id:
                continue
            for track in p.local_tracks:
                logger.info(f"Sending existing track {track.kind} from {pid} to newcomer {newcomer_id}")
                relayed_track = relay.subscribe(track)
                await newcomer.add_or_replace_track(pid, relayed_track, replace_existing=False)


class Participant:
    def __init__(self, participant_id: str, room: Room, websocket):
        self.id = participant_id
        self.room = room
        self.websocket = websocket
        self.peer_connection = RTCPeerConnection()
        self.local_tracks: Set = set()
        self.remote_senders: Dict[str, Dict[str, object]] = {}
        self._renegotiation_pending = False
        self._tracks_initialized = False
        self._setup_peer_connection_handlers()

    def _setup_peer_connection_handlers(self):
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

    async def add_or_replace_track(self, sender_id: str, track, replace_existing: bool = True):
        logger.debug(
            f"add_or_replace_track: sender={sender_id}, kind={track.kind}, replace={replace_existing}")
        logger.debug(f"Current remote_senders for {self.id}: {list(self.remote_senders.keys())}")

        if sender_id not in self.remote_senders:
            self.remote_senders[sender_id] = {}

        senders = self.remote_senders[sender_id]
        existing_sender = senders.get(track.kind)

        if existing_sender:
            logger.debug(f"Existing sender for {sender_id}/{track.kind}: {existing_sender}")
        else:
            logger.debug(f"No existing sender for {sender_id}/{track.kind}")

        if replace_existing and existing_sender:
            logger.info(f"Replacing {track.kind} track from {sender_id} to {self.id}")
            try:
                await existing_sender.replaceTrack(track)
                # после замены не удаляем existing_sender из словаря, он остаётся
            except Exception as e:
                logger.error(f"Failed to replace track: {e}", exc_info=True)
        elif not replace_existing and existing_sender:
            logger.warning(
                f"NOT replacing existing {track.kind} track from {sender_id} to {self.id} (replace_existing=False)")
            # Здесь можно либо ничего не делать, либо добавить второй sender – это приведёт к дублю
        else:
            logger.info(f"Adding new {track.kind} track from {sender_id} to {self.id}")
            sender = self.peer_connection.addTrack(track)
            senders[track.kind] = sender

    async def notify_renegotiation_needed(self):
        if self._renegotiation_pending:
            return
        self._renegotiation_pending = True
        try:
            await self.websocket.send(json.dumps({"type": "renegotiate"}))
            logger.debug(f"Sent renegotiate notification to {self.id}")
        except Exception as e:
            logger.error(f"Failed to send renegotiate notification: {e}")

    async def close(self):
        logger.debug(f"Closing participant {self.id}, remote_senders: {self.remote_senders}")

        for pid, p in self.room.participants.items():
            if pid != self.id and self.id in p.remote_senders:
                for sender in p.remote_senders[self.id].values():
                    if sender is None:
                        logger.warning(f"Sender for {pid} is None, skipping")
                        continue
                    try:
                        logger.debug(f"Stopping sender {pid}: {sender}")
                        await sender.replaceTrack(None)
                    except Exception as e:
                        logger.warning(f"Failed to stop track sender: {e}", exc_info=True)
                del p.remote_senders[self.id]
                await p.notify_renegotiation_needed()

        self.room.remove_participant(self.id)
        await self.peer_connection.close()


rooms: Dict[str, Room] = {}


def create_ice_candidate_from_js(cand_data: dict) -> RTCIceCandidate:
    if hasattr(RTCIceCandidate, "from_js"):
        return RTCIceCandidate.from_js(cand_data)
    else:
        return RTCIceCandidate(
            component=cand_data.get("component", 1),
            foundation=cand_data.get("foundation", ""),
            ip=cand_data.get("ip", ""),
            port=cand_data.get("port", 0),
            priority=cand_data.get("priority", 0),
            protocol=cand_data.get("protocol", ""),
            type=cand_data.get("type", ""),
            relatedAddress=cand_data.get("relatedAddress"),
            relatedPort=cand_data.get("relatedPort"),
            sdpMid=cand_data.get("sdpMid"),
            sdpMLineIndex=cand_data.get("sdpMLineIndex"),
            tcpType=cand_data.get("tcpType")
        )


async def handle_client(websocket):
    participant_id = None
    room = None
    participant = None

    try:
        async for message in websocket:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "join":
                room_id = data.get("room")
                participant_id = data.get("participant_id")
                if not room_id or not participant_id:
                    await websocket.send(json.dumps({"type": "error", "message": "room and participant_id required"}))
                    continue

                if room_id not in rooms:
                    rooms[room_id] = Room(room_id)
                room = rooms[room_id]

                participant = Participant(participant_id, room, websocket)
                room.add_participant(participant)

                await websocket.send(json.dumps({"type": "joined", "room": room_id}))

            elif msg_type == "offer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue

                offer = RTCSessionDescription(sdp=data["sdp"], type=data["type"])
                await participant.peer_connection.setRemoteDescription(offer)

                if not participant._tracks_initialized:
                    await room.send_existing_tracks_to_newcomer(participant.id)
                    participant._tracks_initialized = True

                if participant.peer_connection.signalingState == "have-remote-offer":
                    answer = await participant.peer_connection.createAnswer()
                    await participant.peer_connection.setLocalDescription(answer)
                    await websocket.send(json.dumps({
                        "type": "answer",
                        "sdp": participant.peer_connection.localDescription.sdp
                    }))
                    participant._renegotiation_pending = False

            elif msg_type == "answer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue
                answer = RTCSessionDescription(sdp=data["sdp"], type="answer")
                await participant.peer_connection.setRemoteDescription(answer)
                participant._renegotiation_pending = False

            elif msg_type == "ice-candidate":
                if not participant:
                    continue
                cand_data = data["candidate"]
                if not cand_data or not cand_data.get("candidate"):
                    continue
                try:
                    candidate = create_ice_candidate_from_js(cand_data)
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
        logger.info(f"Client {participant_id} disconnected")


async def main():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    try:
        ssl_context.load_cert_chain("cert.pem", "key.pem")
        proto = "wss"
    except FileNotFoundError:
        logger.warning("SSL certificates not found, using unencrypted WebSocket (ws://)")
        ssl_context = None
        proto = "ws"

    async with websockets.serve(
        handle_client,
        "0.0.0.0",
        8001,
        ssl=ssl_context,
        max_size=10**7,
    ):
        logger.info(f"SFU server started on {proto}://0.0.0.0:8001")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
