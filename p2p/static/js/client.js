const joinScreen = document.getElementById('join-screen');
const videoGrid = document.getElementById('video-grid');
const nicknameInput = document.getElementById('nickname');
const roomIdInput = document.getElementById('roomId');
const joinBtn = document.getElementById('joinBtn');
const localVideo = document.getElementById('localVideo');
const remoteVideosDiv = document.getElementById('remoteVideos');
const startLatencyTestBtn = document.getElementById('startLatencyTest');
const latencyResultDiv = document.getElementById('latencyResult');
const webrtcStatsDiv = document.getElementById('webrtcStats');

let ws = null;
let localStream = null;
let currentRoomId = null;
let currentNickname = null;
let currentParticipantId = null;

const peers = new Map();
const prevStatsMap = new Map();

const pcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },

        {
            urls: [
                'turn:178.154.213.197:3478?transport=udp',
                'turn:178.154.213.197:3478?transport=tcp'
            ],
            username: 'webrtc',
            credential: 'webrtc_password'
        }
    ],
    iceCandidatePoolSize: 10
};


function connectWebSocket(roomId, nickname) {
    const wsUrl = `wss://${window.location.host}/ws`;
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('[WS] Соединение установлено');
        sendJoin(roomId, nickname);
    };

    ws.onmessage = async (event) => {
        try {
            const msg = JSON.parse(event.data);
            console.log('[WS] Получено сообщение:', msg.type, msg);
            await handleSignalingMessage(msg);
        } catch (err) {
            console.error('[WS] Ошибка обработки сообщения:', err);
        }
    };

    ws.onerror = (error) => {
        console.error('[WS] Ошибка:', error);
    };

    ws.onclose = () => {
        console.log('[WS] Закрыт');
        cleanupAllPeers();
        if (localStream) {
            localStream.getTracks().forEach(track => track.stop());
            localStream = null;
        }
        videoGrid.style.display = 'none';
        joinScreen.style.display = 'block';
    };
}

function sendJoin(roomId, nickname) {
    const msg = { type: 'join', room: roomId, nickname: nickname };
    console.log('[WS] Отправка join:', msg);
    ws.send(JSON.stringify(msg));
}

function sendSignal(targetId, data) {
    const msg = { type: 'signal', target_id: targetId, data: data };
    console.log(`[WS] Отправка signal для ${targetId}, тип данных: ${data.type}`);
    ws.send(JSON.stringify(msg));
}

function sendPing() {
    const timestamp = Date.now();
    const msg = { type: 'ping', timestamp: timestamp };
    console.log('[WS] Отправка ping');
    ws.send(JSON.stringify(msg));
    return timestamp;
}

async function handleSignalingMessage(msg) {
    switch (msg.type) {
        case 'joined':
            currentRoomId = msg.room;
            currentNickname = msg.nickname;
            currentParticipantId = msg.participant_id;
            console.log(`[JOIN] Успех: комната ${currentRoomId}, id=${currentParticipantId}`);
            joinScreen.style.display = 'none';
            videoGrid.style.display = 'flex';
            await initLocalMedia();
            break;

        case 'existing_participants':
            console.log('[EXISTING] Участники в комнате (ждём offer от них):', msg.participants);
            break;

        case 'participant_joined':
            console.log('[NEW] Новый участник, создаём активное соединение:', msg.participant);
            await createPeerConnection(msg.participant.id, true);
            break;

        case 'participant_left':
            console.log('[LEFT] Участник покинул:', msg.participant_id);
            removePeer(msg.participant_id);
            break;

        case 'signal':
            console.log(`[SIGNAL] от ${msg.from_id}, тип: ${msg.data?.type}`);
            await handleSignal(msg.from_id, msg.data);
            break;

        case 'pong':
            const rtt = Date.now() - msg.timestamp;
            console.log(`[PONG] RTT = ${rtt} мс`);
            latencyResultDiv.innerHTML = `<p>⏱️ RTT через WebSocket: ${rtt} мс</p>`;
            break;

        case 'error':
            console.error('[ERROR] Сервер:', msg.message);
            alert(`Ошибка: ${msg.message}`);
            break;

        default:
            console.warn('[WARN] Неизвестный тип сообщения:', msg.type);
    }
}

async function createPeerConnection(remoteId, isInitiator) {
    if (peers.has(remoteId)) {
        console.warn(`[PC] Peer ${remoteId} уже существует, пропускаем`);
        return;
    }

    console.log(`[PC] Создаём PeerConnection для ${remoteId}, isInitiator=${isInitiator}`);
    const pc = new RTCPeerConnection(pcConfig);
    const videoElement = document.createElement('video');
    videoElement.autoplay = true;
    videoElement.playsInline = true;
    videoElement.classList.add('remote-video');
    const container = document.createElement('div');
    container.className = 'remote-video-container';
    container.id = `remote-${remoteId}`;
    container.appendChild(videoElement);
    const label = document.createElement('div');
    label.className = 'label';
    label.textContent = `Участник ${remoteId.slice(0, 6)}`;
    container.appendChild(label);
    remoteVideosDiv.appendChild(container);

    const peerInfo = {
        pc: pc,
        videoElement: videoElement,
        stream: null,
        pendingCandidates: []
    };
    peers.set(remoteId, peerInfo);

    if (localStream) {
        console.log(`[PC] Добавляем локальные треки для ${remoteId}`);
        localStream.getTracks().forEach(track => {
            pc.addTrack(track, localStream);
            console.log(`  - Добавлен трек ${track.kind}`);
        });
    } else {
        console.log(`[PC] Локальный поток ещё не готов для ${remoteId}`);
    }

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            console.log(`[ICE] Отправка ICE-кандидата для ${remoteId}:`, event.candidate);
            sendSignal(remoteId, {
                type: 'ice-candidate',
                candidate: event.candidate
            });
        } else {
            console.log(`[ICE] Кандидаты для ${remoteId} завершены`);
        }
    };

    pc.onicegatheringstatechange = () => {
        console.log(`[ICE] Состояние сбора кандидатов для ${remoteId}: ${pc.iceGatheringState}`);
    };

    pc.ontrack = (event) => {
        console.log(`[TRACK] Получен трек от ${remoteId}, kind=${event.track.kind}, streams=${event.streams.length}`);
        if (peerInfo.videoElement.srcObject !== event.streams[0]) {
            peerInfo.videoElement.srcObject = event.streams[0];
            peerInfo.stream = event.streams[0];
            console.log(`[TRACK] Видео для ${remoteId} установлено`);
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log(`[ICE] Состояние соединения для ${remoteId}: ${pc.iceConnectionState}`);
        if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
            console.log(`[ICE] Соединение потеряно для ${remoteId}, удаляем peer`);
            removePeer(remoteId);
        }
    };

    pc.onconnectionstatechange = () => {
        console.log(`[PC] Состояние соединения для ${remoteId}: ${pc.connectionState}`);
    };

    pc.onsignalingstatechange = () => {
        console.log(`[PC] Состояние сигнализации для ${remoteId}: ${pc.signalingState}`);
    };

    if (isInitiator) {
        try {
            console.log(`[OFFER] Создаём offer для ${remoteId}`);
            const offer = await pc.createOffer();
            console.log(`[OFFER] Offer создан, устанавливаем localDescription`);
            await pc.setLocalDescription(offer);
            console.log(`[OFFER] localDescription установлен, отправляем сигнал`);
            sendSignal(remoteId, {
                type: 'offer',
                sdp: offer.sdp
            });
        } catch (err) {
            console.error(`[OFFER] Ошибка создания offer для ${remoteId}:`, err);
        }
    }
}

async function handleSignal(fromId, signalData) {
    console.log(`[SIGNAL] Обработка сигнала от ${fromId}, тип: ${signalData.type}`);
    let peerInfo = peers.get(fromId);

    if (!peerInfo && signalData.type === 'offer') {
        console.log(`[SIGNAL] Получен offer от ${fromId}, создаём пассивный peer`);
        await createPeerConnection(fromId, false);
        peerInfo = peers.get(fromId);
    }

    if (!peerInfo) {
        console.warn(`[SIGNAL] Нет peer для ${fromId}, игнорируем ${signalData.type}`);
        return;
    }

    const pc = peerInfo.pc;
    console.log(`[SIGNAL] Текущее состояние signalingState для ${fromId}: ${pc.signalingState}`);

    try {
        switch (signalData.type) {
            case 'offer':
                if (pc.signalingState !== 'stable') {
                    console.warn(`[SIGNAL] Неподходящее состояние для offer: ${pc.signalingState}, игнорируем`);
                    return;
                }
                console.log(`[SIGNAL] Устанавливаем remoteDescription (offer)`);
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: signalData.sdp }));
                console.log(`[SIGNAL] RemoteDescription установлен, создаём answer`);
                const answer = await pc.createAnswer();
                console.log(`[SIGNAL] Answer создан, устанавливаем localDescription`);
                await pc.setLocalDescription(answer);
                console.log(`[SIGNAL] LocalDescription установлен, отправляем answer`);
                sendSignal(fromId, { type: 'answer', sdp: answer.sdp });

                if (peerInfo.pendingCandidates.length) {
                    console.log(`[SIGNAL] Добавляем ${peerInfo.pendingCandidates.length} накопленных ICE-кандидатов`);
                    for (const cand of peerInfo.pendingCandidates) {
                        await pc.addIceCandidate(cand);
                    }
                    peerInfo.pendingCandidates = [];
                }
                break;

            case 'answer':
                if (pc.signalingState !== 'have-local-offer') {
                    console.warn(`[SIGNAL] Неподходящее состояние для answer: ${pc.signalingState}, игнорируем`);
                    return;
                }
                console.log(`[SIGNAL] Устанавливаем remoteDescription (answer)`);
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signalData.sdp }));
                console.log(`[SIGNAL] RemoteDescription установлен`);
                if (peerInfo.pendingCandidates.length) {
                    console.log(`[SIGNAL] Добавляем ${peerInfo.pendingCandidates.length} накопленных ICE-кандидатов`);
                    for (const cand of peerInfo.pendingCandidates) {
                        await pc.addIceCandidate(cand);
                    }
                    peerInfo.pendingCandidates = [];
                }
                break;

            case 'ice-candidate':
                if (signalData.candidate) {
                    const candidate = new RTCIceCandidate(signalData.candidate);
                    if (pc.remoteDescription) {
                        console.log(`[SIGNAL] Добавляем ICE-кандидат для ${fromId}`);
                        await pc.addIceCandidate(candidate);
                    } else {
                        console.log(`[SIGNAL] RemoteDescription ещё нет, буферизируем ICE-кандидат для ${fromId}`);
                        peerInfo.pendingCandidates.push(candidate);
                    }
                }
                break;

            default:
                console.warn('[SIGNAL] Неизвестный тип сигнала:', signalData.type);
        }
    } catch (err) {
        console.error(`[SIGNAL] Ошибка обработки сигнала от ${fromId}:`, err);
    }
}

function removePeer(remoteId) {
    const peerInfo = peers.get(remoteId);
    if (peerInfo) {
        console.log(`[REMOVE] Удаляем peer ${remoteId}`);
        peerInfo.pc.close();
        if (peerInfo.videoElement) {
            peerInfo.videoElement.srcObject = null;
        }
        const container = document.getElementById(`remote-${remoteId}`);
        if (container) container.remove();
        peers.delete(remoteId);
        prevStatsMap.delete(remoteId);
        console.log(`[REMOVE] Peer ${remoteId} удалён`);
    }
}

function cleanupAllPeers() {
    console.log('[CLEANUP] Очищаем всех пиров');
    for (const [id, peerInfo] of peers.entries()) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) peerInfo.videoElement.srcObject = null;
    }
    peers.clear();
    remoteVideosDiv.innerHTML = '';
}


async function initLocalMedia() {
    try {
        console.log('[MEDIA] Запрашиваем доступ к камере/микрофону');
        localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        localVideo.srcObject = localStream;
        console.log('[MEDIA] Локальный поток получен, треки:', localStream.getTracks().map(t => t.kind));

        for (const [remoteId, peerInfo] of peers.entries()) {
            console.log(`[MEDIA] Добавляем локальные треки к существующему peer ${remoteId}`);
            localStream.getTracks().forEach(track => {
                peerInfo.pc.addTrack(track, localStream);
            });
            if (peerInfo.pc.signalingState === 'stable' && peerInfo.pc.remoteDescription) {
                console.log(`[MEDIA] Пересылаем offer для renegotiation с ${remoteId}`);
                const offer = await peerInfo.pc.createOffer();
                await peerInfo.pc.setLocalDescription(offer);
                sendSignal(remoteId, { type: 'offer', sdp: offer.sdp });
            }
        }
    } catch (err) {
        console.error('[MEDIA] Ошибка доступа к медиа:', err);
        alert('Не удалось получить доступ к камере/микрофону');
    }
}

startLatencyTestBtn.addEventListener('click', () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        sendPing();
        latencyResultDiv.innerHTML = '<p>Измерение...</p>';
    } else {
        alert('WebSocket не подключён');
    }
});

async function collectStats() {
    let statsText = '';
    const now = Date.now();

    for (const [remoteId, peerInfo] of peers.entries()) {
        const pc = peerInfo.pc;
        if (pc.iceConnectionState !== 'connected' && pc.iceConnectionState !== 'completed') continue;

        const stats = await pc.getStats();
        let inboundRtpStats = null;
        let candidatePairStats = null;

        stats.forEach(report => {
            if (report.type === 'inbound-rtp' && report.kind === 'video') inboundRtpStats = report;
            if (report.type === 'candidate-pair' && report.nominated === true) candidatePairStats = report;
        });

        if (inboundRtpStats) {
            const currentBytes = inboundRtpStats.bytesReceived;
            const prev = prevStatsMap.get(remoteId);
            let bitrateKbps = 0;

            if (prev && prev.bytes !== undefined && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (now - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    bitrateKbps = (bytesDiff * 8) / timeDiffSec / 1000;
                }
            }

            prevStatsMap.set(remoteId, { bytes: currentBytes, timestamp: now });

            statsText += `Участник ${remoteId.slice(0,6)}:\n`;
            statsText += `  Битрейт видео: ${bitrateKbps.toFixed(2)} kbps\n`;
            statsText += `  Потеря пакетов: ${inboundRtpStats.packetsLost}\n`;
            statsText += `  Джиттер: ${inboundRtpStats.jitter?.toFixed(3)} с\n`;
        } else {
            statsText += `Участник ${remoteId.slice(0,6)}: нет видео-потока\n`;
        }

        if (candidatePairStats) {
            statsText += `  RTT (ICE): ${(candidatePairStats.currentRoundTripTime * 1000).toFixed(2)} мс\n`;
        }
        statsText += '\n';
    }

    if (statsText === '') statsText = 'Нет активных соединений';
    webrtcStatsDiv.innerHTML = `<pre>${statsText}</pre>`;
}

setInterval(() => {
    if (videoGrid.style.display !== 'none') collectStats();
}, 5000);


joinBtn.addEventListener('click', () => {
    const nickname = nicknameInput.value.trim();
    const roomId = roomIdInput.value.trim();
    if (!nickname || !roomId) {
        alert('Введите имя и ID комнаты');
        return;
    }
    if (ws) ws.close();
    connectWebSocket(roomId, nickname);
});