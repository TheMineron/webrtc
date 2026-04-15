import asyncio
import json
import logging
import uuid
from typing import Dict, List

from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sfu")

app = FastAPI()

rooms: Dict[str, List["SFUParticipant"]] = {}


class SFUParticipant:
    def __init__(self, room_id: str, websocket: WebSocket, participant_id: str):
        self.room_id = room_id
        self.websocket = websocket
        self.id = participant_id
        self.pc = RTCPeerConnection()
        self.relay = MediaRelay()
        self.incoming_tracks = {}          # kind -> track
        self.outgoing_senders = {}          # (source_id, kind) -> RTCRtpSender
        self.renegotiation_pending = False

        @self.pc.on("track")
        async def on_track(track: MediaStreamTrack):
            logger.info(f"Participant {self.id} sent {track.kind} track")
            self.incoming_tracks[track.kind] = track
            # Ретранслируем всем другим участникам комнаты
            for other in rooms.get(self.room_id, []):
                if other.id != self.id:
                    await other.add_remote_track(self.id, track.kind, self.relay.subscribe(track))

        @self.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            if self.pc.connectionState in ["failed", "closed", "disconnected"]:
                await self.close()

    async def add_remote_track(self, source_id: str, kind: str, track: MediaStreamTrack):
        """Добавить трек от другого участника в своё соединение"""
        # Проверяем, нет ли уже такого трека
        if (source_id, kind) in self.outgoing_senders:
            logger.warning(f"Track {source_id}/{kind} already exists, skipping")
            return

        # Добавляем трансивер с направлением recvonly
        transceiver = self.pc.addTransceiver(track, direction='recvonly')
        self.outgoing_senders[(source_id, kind)] = transceiver.sender
        # Запускаем переговоры
        await self._renegotiate()

    async def _renegotiate(self):
        """Инициировать пересмотр соединения"""
        if self.renegotiation_pending:
            return
        self.renegotiation_pending = True
        try:
            # Убедимся, что состояние stable
            if self.pc.signalingState != 'stable':
                logger.info(f"Signaling state {self.pc.signalingState}, waiting...")
                # Можно подождать, но лучше просто пропустить
                return

            offer = await self.pc.createOffer()
            await self.pc.setLocalDescription(offer)

            await self.websocket.send_json({
                "type": "renegotiate",
                "sdp": self.pc.localDescription.sdp,
                "type_sdp": self.pc.localDescription.type
            })
        except Exception as e:
            logger.exception(f"Renegotiation failed: {e}")
        finally:
            self.renegotiation_pending = False

    async def handle_answer(self, sdp: str, sdp_type: str):
        """Применить answer от клиента"""
        if self.pc.signalingState == 'have-local-offer':
            answer = RTCSessionDescription(sdp=sdp, type=sdp_type)
            await self.pc.setRemoteDescription(answer)
        else:
            logger.warning(f"Unexpected signaling state for answer: {self.pc.signalingState}")

    async def close(self):
        if self.room_id in rooms:
            rooms[self.room_id].remove(self)
            if not rooms[self.room_id]:
                del rooms[self.room_id]
        await self.pc.close()
        logger.info(f"Participant {self.id} removed")


@app.websocket("/ws")
async def sfu_websocket(websocket: WebSocket):
    await websocket.accept()
    participant = None
    try:
        data = await websocket.receive_text()
        msg = json.loads(data)
        if msg.get("type") != "join":
            await websocket.close(code=1002)
            return

        room_id = msg.get("room")
        participant_id = msg.get("participant_id")
        if not room_id or not participant_id:
            await websocket.close(code=1002)
            return

        participant = SFUParticipant(room_id, websocket, participant_id)
        rooms.setdefault(room_id, []).append(participant)
        logger.info(f"Participant {participant_id} joined SFU room {room_id}")

        await websocket.send_json({"type": "ready"})

        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            msg_type = msg.get("type")

            if msg_type == "offer":
                offer = RTCSessionDescription(sdp=msg["sdp"], type=msg["type_sdp"])
                await participant.pc.setRemoteDescription(offer)
                answer = await participant.pc.createAnswer()
                await participant.pc.setLocalDescription(answer)
                await websocket.send_json({
                    "type": "answer",
                    "sdp": participant.pc.localDescription.sdp,
                    "type_sdp": participant.pc.localDescription.type
                })
            elif msg_type == "answer":
                await participant.handle_answer(msg["sdp"], msg["type_sdp"])
            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})
            elif msg_type == "leave":
                break

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.exception(f"Error: {e}")
    finally:
        if participant:
            await participant.close()
        try:
            await websocket.close()
        except:
            pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)