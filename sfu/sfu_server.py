import asyncio
import json
import logging
import ssl
from typing import Dict

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.contrib.media import MediaRelay

logging.basicConfig(level=logging.INFO)
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

    async def broadcast_track(self, sender_id: str, track):
        """Переслать трек всем остальным участникам."""
        for pid, p in self.participants.items():
            if pid != sender_id:
                logger.info(f"Relaying track {track.kind} from {sender_id} to {pid}")
                relayed_track = relay.subscribe(track)
                # Добавляем трек и инициируем ре-негоциацию
                p.add_remote_track(relayed_track)
                await p.renegotiate()


class Participant:
    def __init__(self, participant_id: str, room: Room, websocket):
        self.id = participant_id
        self.room = room
        self.websocket = websocket
        self.peer_connection = RTCPeerConnection()
        self._remote_tracks = []  # список добавленных удалённых треков для отслеживания
        self._renegotiation_lock = asyncio.Lock()
        self._setup_peer_connection_handlers()

    def _setup_peer_connection_handlers(self):
        @self.peer_connection.on("track")
        async def on_track(track):
            logger.info(f"Local track received from {self.id}: {track.kind}")
            # Трек от этого участника - транслируем остальным
            await self.room.broadcast_track(self.id, track)

        @self.peer_connection.on("connectionstatechange")
        async def on_connectionstatechange():
            logger.info(f"Connection state for {self.id}: {self.peer_connection.connectionState}")
            if self.peer_connection.connectionState in ("failed", "closed"):
                await self.close()

    def add_remote_track(self, track):
        """Добавить удалённый трек для отправки этому участнику."""
        # Создаём трансивер с направлением sendonly для этого трека
        transceiver = self.peer_connection.addTransceiver(track.kind, direction="sendonly")
        transceiver.sender.replaceTrack(track)
        self._remote_tracks.append(transceiver)

    async def renegotiate(self):
        """Инициировать повторное согласование с клиентом."""
        async with self._renegotiation_lock:
            if self.peer_connection.signalingState != "stable":
                return
            try:
                offer = await self.peer_connection.createOffer()
                await self.peer_connection.setLocalDescription(offer)
                await self.websocket.send(json.dumps({
                    "type": "offer",
                    "sdp": self.peer_connection.localDescription.sdp
                }))
                logger.debug(f"Sent renegotiation offer to {self.id}")
            except Exception as e:
                logger.error(f"Renegotiation failed for {self.id}: {e}")

    async def handle_offer(self, sdp: str):
        """Обработать начальный offer от клиента."""
        offer = RTCSessionDescription(sdp=sdp, type="offer")
        await self.peer_connection.setRemoteDescription(offer)
        answer = await self.peer_connection.createAnswer()
        await self.peer_connection.setLocalDescription(answer)
        await self.websocket.send(json.dumps({
            "type": "answer",
            "sdp": self.peer_connection.localDescription.sdp
        }))

    async def handle_answer(self, sdp: str):
        """Обработать answer от клиента (ответ на ре-негоциацию)."""
        answer = RTCSessionDescription(sdp=sdp, type="answer")
        await self.peer_connection.setRemoteDescription(answer)

    async def close(self):
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
                await participant.handle_offer(data["sdp"])

            elif msg_type == "answer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue
                await participant.handle_answer(data["sdp"])

            elif msg_type == "ice-candidate":
                if not participant:
                    continue
                cand_data = data["candidate"]
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
