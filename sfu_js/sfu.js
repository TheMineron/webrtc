const WebSocket = require('ws');
const wrtc = require('wrtc');
const {v4: uuidv4} = require('uuid');

// Конфигурация ICE (можно добавить TURN)
const iceServers = [
    {urls: 'stun:stun.l.google.com:19302'},
    {urls: 'stun:stun1.l.google.com:19302'},
    {
        urls: [
            'turn:178.154.213.197:3478?transport=udp',
            'turn:178.154.213.197:3478?transport=tcp'
        ],
        username: 'webrtc',
        credential: 'webrtc_password'
    }
];


class Peer {
    constructor(id, room, ws) {
        this.id = id;
        this.room = room;
        this.ws = ws;
        this.pc = new wrtc.RTCPeerConnection({iceServers});
        this.remoteSenders = new Map(); // ключ: senderId, значение: Map<kind, RTCRtpSender>
        this.localTracks = new Set();

        this.setupHandlers();
    }

    setupHandlers() {
        // Когда получаем трек от этого пира – сохраняем и рассылаем всем остальным
        this.pc.ontrack = (event) => {
            const track = event.track;
            console.log(`[Peer ${this.id}] received track: ${track.kind}`);
            this.localTracks.add(track);
            // Рассылаем трек всем остальным участникам комнаты
            this.room.broadcastTrack(this.id, track);
            track.onended = () => this.localTracks.delete(track);
        };

        // Отправляем ICE-кандидаты через WebSocket
        this.pc.onicecandidate = (event) => {
            if (event.candidate) {
                this.ws.send(JSON.stringify({
                    type: 'ice-candidate',
                    candidate: event.candidate
                }));
            }
        };

        // Логируем состояния (для отладки)
        this.pc.onconnectionstatechange = () => {
            console.log(`[Peer ${this.id}] connection state: ${this.pc.connectionState}`);
        };
        this.pc.oniceconnectionstatechange = () => {
            console.log(`[Peer ${this.id}] ICE state: ${this.pc.iceConnectionState}`);
        };
    }

    // Добавить или заменить трек от другого участника
    async addOrReplaceTrack(senderId, track, replace = true) {
        if (!this.remoteSenders.has(senderId)) {
            this.remoteSenders.set(senderId, new Map());
        }
        const senders = this.remoteSenders.get(senderId);
        const existingSender = senders.get(track.kind);

        if (replace && existingSender) {
            console.log(`[Peer ${this.id}] replacing ${track.kind} from ${senderId}`);
            await existingSender.replaceTrack(track);
        } else if (!existingSender) {
            console.log(`[Peer ${this.id}] adding new ${track.kind} from ${senderId}`);
            const sender = this.pc.addTrack(track);
            senders.set(track.kind, sender);
            // Инициируем renegotiation
            await this.negotiate();
        }
    }

    // Запуск renegotiation (отправка offer клиенту)
    async negotiate() {
        try {
            const offer = await this.pc.createOffer();
            await this.pc.setLocalDescription(offer);
            this.ws.send(JSON.stringify({
                type: 'offer',
                sdp: this.pc.localDescription.sdp
            }));
        } catch (err) {
            console.error(`[Peer ${this.id}] negotiation failed:`, err);
        }
    }

    // Обработка SDP от клиента
    async handleSdp(type, sdp) {
        const desc = new wrtc.RTCSessionDescription({type, sdp});
        await this.pc.setRemoteDescription(desc);
        if (type === 'offer') {
            const answer = await this.pc.createAnswer();
            await this.pc.setLocalDescription(answer);
            this.ws.send(JSON.stringify({
                type: 'answer',
                sdp: this.pc.localDescription.sdp
            }));
        }
    }

    async addIceCandidate(candidate) {
        await this.pc.addIceCandidate(new wrtc.RTCIceCandidate(candidate));
    }

    async close() {
        // Останавливаем все исходящие треки от этого пира к другим
        for (const [senderId, senders] of this.remoteSenders.entries()) {
            for (const sender of senders.values()) {
                if (sender && sender.replaceTrack) {
                    await sender.replaceTrack(null);
                }
            }
        }
        this.pc.close();
        this.ws.close();
    }
}

class Room {
    constructor(id) {
        this.id = id;
        this.peers = new Map(); // id -> Peer
    }

    addPeer(peer) {
        this.peers.set(peer.id, peer);
        console.log(`[Room ${this.id}] peer ${peer.id} joined`);
    }

    removePeer(peerId) {
        const peer = this.peers.get(peerId);
        if (peer) {
            peer.close();
            this.peers.delete(peerId);
            console.log(`[Room ${this.id}] peer ${peerId} left`);
            // Оповещаем остальных, что пир ушёл (можно отправить событие "peer-left")
            for (const p of this.peers.values()) {
                p.ws.send(JSON.stringify({type: 'peer-left', peerId}));
            }
        }
        if (this.peers.size === 0) {
            rooms.delete(this.id);
            console.log(`[Room ${this.id}] deleted (empty)`);
        }
    }

    // Рассылка трека всем, кроме отправителя
    async broadcastTrack(senderId, track) {
        for (const [peerId, peer] of this.peers) {
            if (peerId !== senderId) {
                await peer.addOrReplaceTrack(senderId, track, true);
            }
        }
    }

    // Отправить новому участнику все существующие треки от других
    async sendExistingTracks(newPeerId) {
        const newPeer = this.peers.get(newPeerId);
        if (!newPeer) return;
        for (const [peerId, peer] of this.peers) {
            if (peerId === newPeerId) continue;
            for (const track of peer.localTracks) {
                await newPeer.addOrReplaceTrack(peerId, track, false);
            }
        }
    }
}

// Хранилище комнат
const rooms = new Map();

// WebSocket сервер
const wss = new WebSocket.Server({port: 8001});
console.log('SFU signaling server running on ws://0.0.0.0:8001');

wss.on('connection', (ws) => {
    let currentPeer = null;
    let currentRoom = null;

    ws.on('message', async (data) => {
        let msg;
        try {
            msg = JSON.parse(data);
        } catch (e) {
            console.error('Invalid JSON');
            return;
        }

        console.log('Received:', msg.type);

        switch (msg.type) {
            case 'join': {
                const {roomId, peerId} = msg;
                if (!rooms.has(roomId)) {
                    rooms.set(roomId, new Room(roomId));
                }
                const room = rooms.get(roomId);
                const peer = new Peer(peerId, room, ws);
                room.addPeer(peer);
                currentPeer = peer;
                currentRoom = room;

                ws.send(JSON.stringify({type: 'joined', roomId}));

                // Отправить новичку существующие треки
                await room.sendExistingTracks(peerId);
                break;
            }

            case 'offer': {
                if (currentPeer) {
                    await currentPeer.handleSdp('offer', msg.sdp);
                }
                break;
            }

            case 'answer': {
                if (currentPeer) {
                    await currentPeer.handleSdp('answer', msg.sdp);
                }
                break;
            }

            case 'ice-candidate': {
                if (currentPeer && msg.candidate) {
                    await currentPeer.addIceCandidate(msg.candidate);
                }
                break;
            }

            case 'leave': {
                if (currentRoom && currentPeer) {
                    currentRoom.removePeer(currentPeer.id);
                }
                break;
            }

            default:
                console.warn('Unknown message type:', msg.type);
        }
    });

    ws.on('close', () => {
        if (currentRoom && currentPeer) {
            currentRoom.removePeer(currentPeer.id);
        }
    });
});