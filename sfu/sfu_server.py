#!/usr/bin/env python3
"""
SFU Server with fixed transceivers (one audio + one video sender per participant).
Server initiates renegotiation by sending offer.
"""

import asyncio
import json
import logging
import ssl
import uuid
from typing import Dict, Optional, Set

import websockets
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceCandidate
from aiortc.contrib.media import MediaRelay

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

relay = MediaRelay()


class Participant:
    def __init__(self, participant_id: str, room: "Room", websocket):
        self.id = participant_id
        self.room = room
        self.websocket = websocket
        self.pc = RTCPeerConnection()
        self.local_tracks: Set = set()                # треки, полученные от этого участника
        self.pending_offers: Dict[str, asyncio.Future] = {}  # для сопоставления offer/answer

        # Фиксированные трансиверы для отправки другим участникам (один на тип)
        self.audio_sender = None
        self.video_sender = None

        self._setup_handlers()
        self._create_fixed_transceivers()

    def _create_fixed_transceivers(self):
        """Создаём трансиверы для отправки (sendonly) другим участникам."""
        # Аудио-трансивер
        transceiver = self.pc.addTransceiver("audio", direction="sendonly")
        self.audio_sender = transceiver.sender
        # Видео-трансивер
        transceiver = self.pc.addTransceiver("video", direction="sendonly")
        self.video_sender = transceiver.sender
        logger.info(f"[{self.id}] Fixed sendonly transceivers created")

    def _setup_handlers(self):
        @self.pc.on("track")
        async def on_track(track):
            logger.info(f"[{self.id}] Received local track: {track.kind}")
            self.local_tracks.add(track)
            # Рассылаем этот трек всем остальным участникам комнаты
            await self.room.broadcast_track(self.id, track)

            @track.on("ended")
            def on_ended():
                logger.info(f"[{self.id}] Local track {track.kind} ended")
                self.local_tracks.discard(track)

        @self.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            state = self.pc.connectionState
            logger.info(f"[{self.id}] Connection state: {state}")
            if state in ("failed", "closed"):
                await self.close()

        @self.pc.on("icecandidate")
        async def on_icecandidate(candidate):
            if candidate:
                await self.websocket.send(json.dumps({
                    "type": "ice-candidate",
                    "candidate": {
                        "candidate": candidate.candidate,
                        "sdpMid": candidate.sdpMid,
                        "sdpMLineIndex": candidate.sdpMLineIndex,
                        "usernameFragment": candidate.usernameFragment
                    }
                }))

        @self.pc.on("signalingstatechange")
        async def on_signalingstatechange():
            logger.info(f"[{self.id}] Signaling state: {self.pc.signalingState}")

    async def handle_offer(self, sdp: str):
        """Обработка входящего offer от клиента."""
        offer = RTCSessionDescription(sdp=sdp, type="offer")
        await self.pc.setRemoteDescription(offer)
        logger.info(f"[{self.id}] Remote description set (offer)")

        # Создаём answer
        answer = await self.pc.createAnswer()
        await self.pc.setLocalDescription(answer)
        await self.websocket.send(json.dumps({
            "type": "answer",
            "sdp": self.pc.localDescription.sdp
        }))
        logger.info(f"[{self.id}] Sent answer")

    async def handle_answer(self, sdp: str):
        """Обработка answer от клиента в ответ на наш offer."""
        answer = RTCSessionDescription(sdp=sdp, type="answer")
        await self.pc.setRemoteDescription(answer)
        logger.info(f"[{self.id}] Remote answer set")

    async def add_remote_track(self, sender_id: str, track):
        """
        Добавляем трек от другого участника (sender_id) в исходящий поток
        для этого участника. Используем фиксированный sender с replaceTrack.
        """
        if track.kind == "audio":
            sender = self.audio_sender
        else:
            sender = self.video_sender

        if sender.track:
            await sender.replaceTrack(track)
            logger.info(f"[{self.id}] Replaced {track.kind} track from {sender_id}")
        else:
            # Первоначальная установка трека (replaceTrack сработает и с null)
            await sender.replaceTrack(track)
            logger.info(f"[{self.id}] Set initial {track.kind} track from {sender_id}")

        # Если состояние не stable, инициируем пересогласование (offer)
        if self.pc.signalingState == "stable":
            await self._create_and_send_offer()
        else:
            logger.info(f"[{self.id}] Deferring offer, signaling state: {self.pc.signalingState}")

    async def _create_and_send_offer(self):
        """Создаёт и отправляет offer клиенту для пересогласования."""
        if self.pc.signalingState != "stable":
            logger.warning(f"[{self.id}] Cannot create offer in state {self.pc.signalingState}")
            return

        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)
        await self.websocket.send(json.dumps({
            "type": "offer",
            "sdp": self.pc.localDescription.sdp
        }))
        logger.info(f"[{self.id}] Sent renegotiation offer")

    async def notify_existing_tracks(self):
        """Отправляем все существующие треки комнаты этому новому участнику."""
        for pid, p in self.room.participants.items():
            if pid == self.id:
                continue
            for track in p.local_tracks:
                relayed = relay.subscribe(track)
                if relayed:
                    await self.add_remote_track(pid, relayed)

    async def close(self):
        logger.info(f"[{self.id}] Closing participant")
        # Удаляем себя из комнаты
        self.room.remove_participant(self.id)
        await self.pc.close()
        # Уведомляем остальных об уходе (опционально)
        # ...


class Room:
    def __init__(self, room_id: str):
        self.id = room_id
        self.participants: Dict[str, Participant] = {}

    def add_participant(self, participant: Participant):
        self.participants[participant.id] = participant
        logger.info(f"Room {self.id}: added {participant.id}, total {len(self.participants)}")

    def remove_participant(self, participant_id: str):
        if participant_id in self.participants:
            del self.participants[participant_id]
            logger.info(f"Room {self.id}: removed {participant_id}, remaining {len(self.participants)}")

    async def broadcast_track(self, sender_id: str, track):
        """Отправляет трек от sender_id всем остальным участникам."""
        logger.info(f"Room {self.id}: broadcasting {track.kind} from {sender_id}")
        for pid, p in self.participants.items():
            if pid != sender_id:
                relayed = relay.subscribe(track)
                if relayed:
                    await p.add_remote_track(sender_id, relayed)

    def is_empty(self) -> bool:
        return len(self.participants) == 0


class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}

    def get_or_create(self, room_id: str) -> Room:
        if room_id not in self.rooms:
            self.rooms[room_id] = Room(room_id)
        return self.rooms[room_id]

    def remove_if_empty(self, room_id: str):
        room = self.rooms.get(room_id)
        if room and room.is_empty():
            del self.rooms[room_id]
            logger.info(f"Room {room_id} deleted (empty)")


room_manager = RoomManager()


async def handle_client(websocket, path):
    participant = None
    room = None
    participant_id = str(uuid.uuid4())

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"type": "error", "message": "Invalid JSON"}))
                continue

            msg_type = data.get("type")
            logger.info(f"Received {msg_type} from {participipant_id if participant else '?'}")

            if msg_type == "join":
                room_id = data.get("room")
                if not room_id:
                    await websocket.send(json.dumps({"type": "error", "message": "room required"}))
                    continue

                room = room_manager.get_or_create(room_id)
                participant = Participant(participant_id, room, websocket)
                room.add_participant(participant)
                await websocket.send(json.dumps({"type": "joined", "participant_id": participant_id}))
                logger.info(f"Participant {participant_id} joined room {room_id}")

            elif msg_type == "offer":
                if not participant:
                    await websocket.send(json.dumps({"type": "error", "message": "Not joined"}))
                    continue
                await participant.handle_offer(data["sdp"])
                # После первого answer отправляем существующие треки
                await participant.notify_existing_tracks()

            elif msg_type == "answer":
                if not participant:
                    continue
                await participant.handle_answer(data["sdp"])

            elif msg_type == "ice-candidate":
                if not participant:
                    continue
                candidate_data = data.get("candidate")
                if candidate_data and candidate_data.get("candidate"):
                    candidate = RTCIceCandidate(
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
                    await participant.pc.addIceCandidate(candidate)
                    logger.debug(f"[{participant.id}] Added ICE candidate")

            elif msg_type == "leave":
                break

            else:
                await websocket.send(json.dumps({"type": "error", "message": f"Unknown type: {msg_type}"}))

    except websockets.exceptions.ConnectionClosed:
        logger.info(f"Connection closed for {participant_id}")
    except Exception as e:
        logger.exception(f"Error handling client {participant_id}: {e}")
    finally:
        if participant:
            await participant.close()
            if room and room.is_empty():
                room_manager.remove_if_empty(room.id)
        logger.info(f"Client {participant_id} disconnected")


async def main():
    ssl_context = None
    try:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain("cert.pem", "key.pem")
        proto = "wss"
    except FileNotFoundError:
        logger.warning("SSL certificates not found, using ws://")
        proto = "ws"

    async with websockets.serve(handle_client, "0.0.0.0", 8001, ssl=ssl_context, max_size=10**7):
        logger.info(f"SFU server started on {proto}://0.0.0.0:8001")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())