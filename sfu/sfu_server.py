import asyncio
import json
import logging
from typing import Dict, Set

from aiortc import RTCPeerConnection, RTCSessionDescription, MediaStreamTrack
from aiortc.contrib.media import MediaRelay
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sfu")

app = FastAPI()

rooms: Dict[str, Set[str]] = {}
participants: Dict[str, "SFUParticipant"] = {}


class SFUParticipant:
    def __init__(self, participant_id: str, room_id: str, websocket: WebSocket):
        self.id = participant_id
        self.room_id = room_id
        self.websocket = websocket
        self.pc = RTCPeerConnection()
        self.relay = MediaRelay()
        self.tracks = {}                     # kind -> track (от клиента)
        self.remote_tracks = {}              # (source_id, kind) -> track
        self.reneg_pending = False

        @self.pc.on("track")
        async def on_track(track: MediaStreamTrack):
            logger.info(f"Received {track.kind} from {self.id}")
            self.tracks[track.kind] = track
            # Рассылаем всем остальным в комнате
            for other_id in rooms.get(self.room_id, set()):
                if other_id != self.id:
                    other = participants.get(other_id)
                    if other:
                        await other.add_remote_track(self.id, track.kind, self.relay.subscribe(track))

        @self.pc.on("connectionstatechange")
        async def on_connectionstatechange():
            if self.pc.connectionState in ["failed", "closed", "disconnected"]:
                await self.close()

    async def add_remote_track(self, source_id: str, kind: str, track: MediaStreamTrack):
        if (source_id, kind) in self.remote_tracks:
            return
        self.remote_tracks[(source_id, kind)] = track
        # Добавляем трек в PeerConnection для отправки клиенту
        self.pc.addTrack(track)
        await self._renegotiate()

    async def _renegotiate(self):
        if self.reneg_pending:
            return
        self.reneg_pending = True
        try:
            if self.pc.signalingState != 'stable':
                logger.info(f"Cannot renegotiate, state={self.pc.signalingState}")
                return
            offer = await self.pc.createOffer()
            await self.pc.setLocalDescription(offer)
            await self.websocket.send_json({
                "type": "renegotiate",
                "sdp": self.pc.localDescription.sdp,
                "type_sdp": self.pc.localDescription.type
            })
        except Exception as e:
            logger.exception(f"Renegotiation error: {e}")
        finally:
            self.reneg_pending = False

    async def handle_answer(self, sdp: str, sdp_type: str):
        if self.pc.signalingState == 'have-local-offer':
            answer = RTCSessionDescription(sdp=sdp, type=sdp_type)
            await self.pc.setRemoteDescription(answer)

    async def close(self):
        if self.room_id in rooms:
            rooms[self.room_id].discard(self.id)
            if not rooms[self.room_id]:
                del rooms[self.room_id]
        await self.pc.close()
        participants.pop(self.id, None)
        logger.info(f"Participant {self.id} closed")


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

        participant = SFUParticipant(participant_id, room_id, websocket)
        participants[participant_id] = participant
        rooms.setdefault(room_id, set()).add(participant_id)
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