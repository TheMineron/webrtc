import json
import json
import logging
from typing import Dict, List

from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sfu")

app = FastAPI()

# Хранилище комнат: room_id -> список участников
rooms: Dict[str, List["SFUParticipant"]] = {}


class SFUParticipant:
    """Участник комнаты на SFU"""
    def __init__(self, room_id: str, websocket: WebSocket, participant_id: str):
        self.room_id = room_id
        self.websocket = websocket
        self.id = participant_id
        self.pc = RTCPeerConnection()
        self.relay = MediaRelay()           # для ретрансляции треков
        self.incoming_tracks = {}           # kind -> track
        self.outgoing_senders = {}           # (target_participant_id, kind) -> RTCRtpSender

        # Обработка входящих треков
        @self.pc.on("track")
        async def on_track(track: MediaStreamTrack):
            logger.info(f"Participant {self.id} sent {track.kind} track")
            self.incoming_tracks[track.kind] = track

            # Ретранслируем трек всем остальным участникам комнаты
            for other in rooms.get(self.room_id, []):
                if other.id != self.id:
                    await other.add_remote_track(self.id, track.kind, self.relay.subscribe(track))

        @self.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            if self.pc.connectionState == "failed":
                await self.close()

    async def add_remote_track(self, source_id: str, kind: str, track: MediaStreamTrack):
        """Добавить трек от другого участника в своё соединение"""
        # Добавляем трек в PeerConnection
        sender = self.pc.addTrack(track)
        self.outgoing_senders[(source_id, kind)] = sender

        # Отправляем клиенту предложение пересмотреть SDP (renegotiation)
        await self._renegotiate()

    async def _renegotiate(self):
        """Инициировать пересмотр соединения и отправить новый SDP клиенту"""
        # Создаём offer
        offer = await self.pc.createOffer()
        await self.pc.setLocalDescription(offer)

        # Отправляем клиенту сообщение с новым SDP
        await self.websocket.send_json({
            "type": "renegotiate",
            "sdp": self.pc.localDescription.sdp,
            "type_sdp": self.pc.localDescription.type
        })

        # Ждём ответа от клиента (answer)
        # Ответ придёт отдельным сообщением типа "answer"
        # Обработка в основном цикле WebSocket

    async def handle_answer(self, sdp: str, sdp_type: str):
        """Применить answer от клиента после renegotiation"""
        answer = RTCSessionDescription(sdp=sdp, type=sdp_type)
        await self.pc.setRemoteDescription(answer)

    async def close(self):
        """Закрыть соединение и удалить участника из комнаты"""
        if self.room_id in rooms:
            rooms[self.room_id].remove(self)
            if not rooms[self.room_id]:
                del rooms[self.room_id]
        await self.pc.close()
        logger.info(f"Participant {self.id} removed from room {self.room_id}")


@app.websocket("/ws")
async def sfu_websocket(websocket: WebSocket):
    await websocket.accept()
    participant = None
    try:
        # Ожидаем первое сообщение с типом "join"
        data = await websocket.receive_text()
        msg = json.loads(data)
        if msg.get("type") != "join":
            await websocket.close(code=1002, reason="First message must be join")
            return

        room_id = msg.get("room")
        participant_id = msg.get("participant_id")
        if not room_id or not participant_id:
            await websocket.close(code=1002, reason="Missing room or participant_id")
            return

        # Создаём участника
        participant = SFUParticipant(room_id, websocket, participant_id)

        # Добавляем в комнату
        if room_id not in rooms:
            rooms[room_id] = []
        rooms[room_id].append(participant)
        logger.info(f"Participant {participant_id} joined SFU room {room_id}")

        # Сообщаем клиенту, что подключение к SFU готово
        await websocket.send_json({"type": "ready"})

        # Основной цикл обработки сообщений (offer, answer)
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            msg_type = msg.get("type")

            if msg_type == "offer":
                # Клиент отправляет offer для установки соединения
                offer = RTCSessionDescription(sdp=msg["sdp"], type=msg["type_sdp"])
                await participant.pc.setRemoteDescription(offer)
                # Создаём answer
                answer = await participant.pc.createAnswer()
                await participant.pc.setLocalDescription(answer)
                await websocket.send_json({
                    "type": "answer",
                    "sdp": participant.pc.localDescription.sdp,
                    "type_sdp": participant.pc.localDescription.type
                })

            elif msg_type == "answer":
                # Ответ на renegotiation
                await participant.handle_answer(msg["sdp"], msg["type_sdp"])

            elif msg_type == "ping":
                await websocket.send_json({"type": "pong"})

            elif msg_type == "leave":
                break

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for participant {participant.id if participant else 'unknown'}")
    except Exception as e:
        logger.exception(f"Error in SFU: {e}")
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
