// client.js

// --- DOM элементы ---
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

// --- Глобальные переменные ---
let ws = null;
let localStream = null;
let currentRoomId = null;
let currentNickname = null;
let currentParticipantId = null;

// Хранилище пиров: key = remoteParticipantId, value = { pc, videoElement, stream, pendingCandidates }
const peers = new Map();

// --- Конфигурация STUN/TURN серверов ---
const pcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ]
};

// --- Функции работы с WebSocket ---
function connectWebSocket(roomId, nickname) {
    const wsUrl = `wss://${window.location.host}/ws`;
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        console.log('WebSocket соединение установлено');
        sendJoin(roomId, nickname);
    };

    ws.onmessage = async (event) => {
        try {
            const msg = JSON.parse(event.data);
            await handleSignalingMessage(msg);
        } catch (err) {
            console.error('Ошибка обработки сообщения:', err);
        }
    };

    ws.onerror = (error) => {
        console.error('WebSocket ошибка:', error);
    };

    ws.onclose = () => {
        console.log('WebSocket закрыт');
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
    ws.send(JSON.stringify({
        type: 'join',
        room: roomId,
        nickname: nickname
    }));
}

function sendSignal(targetId, data) {
    ws.send(JSON.stringify({
        type: 'signal',
        target_id: targetId,
        data: data
    }));
}

function sendPing() {
    const timestamp = Date.now();
    ws.send(JSON.stringify({ type: 'ping', timestamp: timestamp }));
    return timestamp;
}

// --- Обработка входящих сигнальных сообщений ---
async function handleSignalingMessage(msg) {
    switch (msg.type) {
        case 'joined':
            currentRoomId = msg.room;
            currentNickname = msg.nickname;
            currentParticipantId = msg.participant_id;
            console.log(`Присоединились: комната ${currentRoomId}, id=${currentParticipantId}`);
            joinScreen.style.display = 'none';
            videoGrid.style.display = 'flex';
            await initLocalMedia();
            break;

        case 'existing_participants':
            console.log('Существующие участники (мы новичок):', msg.participants);
            // Новичок НЕ инициирует offer, только создаёт PeerConnection и ждёт offer от каждого существующего
            for (const p of msg.participants) {
                await createPeerConnection(p.id, false); // isInitiator = false
            }
            break;

        case 'participant_joined':
            console.log('Новый участник (мы уже в комнате):', msg.participant);
            // Существующий участник инициирует offer для новичка
            await createPeerConnection(msg.participant.id, true);
            break;

        case 'participant_left':
            console.log('Участник покинул:', msg.participant_id);
            removePeer(msg.participant_id);
            break;

        case 'signal':
            await handleSignal(msg.from_id, msg.data);
            break;

        case 'pong':
            const rtt = Date.now() - msg.timestamp;
            latencyResultDiv.innerHTML = `<p>⏱️ RTT через WebSocket: ${rtt} мс</p>`;
            break;

        case 'error':
            console.error('Ошибка сервера:', msg.message);
            alert(`Ошибка: ${msg.message}`);
            break;

        default:
            console.warn('Неизвестный тип:', msg.type);
    }
}

// --- WebRTC: создание PeerConnection ---
async function createPeerConnection(remoteId, isInitiator) {
    if (peers.has(remoteId)) {
        console.warn(`Peer ${remoteId} уже существует`);
        return;
    }

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
        pendingCandidates: [] // буфер ICE-кандидатов до установки remote description
    };
    peers.set(remoteId, peerInfo);

    // Добавляем локальные треки, если уже есть
    if (localStream) {
        localStream.getTracks().forEach(track => {
            pc.addTrack(track, localStream);
        });
    }

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            sendSignal(remoteId, {
                type: 'ice-candidate',
                candidate: event.candidate
            });
        }
    };

    pc.ontrack = (event) => {
        console.log(`Получен трек от ${remoteId}`);
        if (peerInfo.videoElement.srcObject !== event.streams[0]) {
            peerInfo.videoElement.srcObject = event.streams[0];
            peerInfo.stream = event.streams[0];
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log(`ICE состояние для ${remoteId}: ${pc.iceConnectionState}`);
        if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
            removePeer(remoteId);
        }
    };

    // Если мы инициатор – создаём offer
    if (isInitiator) {
        try {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            sendSignal(remoteId, {
                type: 'offer',
                sdp: offer.sdp
            });
        } catch (err) {
            console.error(`Ошибка создания offer для ${remoteId}:`, err);
        }
    }
}

// --- Обработка сигналов от удалённого участника ---
async function handleSignal(fromId, signalData) {
    let peerInfo = peers.get(fromId);
    if (!peerInfo && (signalData.type === 'offer' || signalData.type === 'answer')) {
        // Если пришёл offer или answer, а пира нет – создаём пассивное соединение (как ответчик)
        await createPeerConnection(fromId, false);
        peerInfo = peers.get(fromId);
    }
    if (!peerInfo) {
        console.warn(`Нет peer для ${fromId}, игнорируем ${signalData.type}`);
        return;
    }
    const pc = peerInfo.pc;

    try {
        switch (signalData.type) {
            case 'offer':
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: signalData.sdp }));
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                sendSignal(fromId, {
                    type: 'answer',
                    sdp: answer.sdp
                });
                // Отправляем накопившиеся ICE-кандидаты
                if (peerInfo.pendingCandidates.length) {
                    for (const cand of peerInfo.pendingCandidates) {
                        await pc.addIceCandidate(cand);
                    }
                    peerInfo.pendingCandidates = [];
                }
                break;

            case 'answer':
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signalData.sdp }));
                // Отправляем накопившиеся ICE-кандидаты
                if (peerInfo.pendingCandidates.length) {
                    for (const cand of peerInfo.pendingCandidates) {
                        await pc.addIceCandidate(cand);
                    }
                    peerInfo.pendingCandidates = [];
                }
                break;

            case 'ice-candidate':
                if (signalData.candidate) {
                    const candidate = new RTCIceCandidate(signalData.candidate);
                    // Если remote description ещё не установлен – буферизируем
                    if (pc.remoteDescription) {
                        await pc.addIceCandidate(candidate);
                    } else {
                        peerInfo.pendingCandidates.push(candidate);
                    }
                }
                break;

            default:
                console.warn('Неизвестный тип сигнала:', signalData.type);
        }
    } catch (err) {
        console.error(`Ошибка обработки сигнала от ${fromId}:`, err);
    }
}

function removePeer(remoteId) {
    const peerInfo = peers.get(remoteId);
    if (peerInfo) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) {
            peerInfo.videoElement.srcObject = null;
        }
        const container = document.getElementById(`remote-${remoteId}`);
        if (container) container.remove();
        peers.delete(remoteId);
        console.log(`Peer ${remoteId} удалён`);
    }
}

function cleanupAllPeers() {
    for (const [id, peerInfo] of peers.entries()) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) peerInfo.videoElement.srcObject = null;
    }
    peers.clear();
    remoteVideosDiv.innerHTML = '';
}

// --- Локальная медиа ---
async function initLocalMedia() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        localVideo.srcObject = localStream;
        console.log('Локальный поток получен');

        // Добавляем треки во все уже созданные пиры
        for (const [remoteId, peerInfo] of peers.entries()) {
            localStream.getTracks().forEach(track => {
                peerInfo.pc.addTrack(track, localStream);
            });
            // Если соединение уже в stable и есть remote description, нужно переслать offer (renegotiation)
            if (peerInfo.pc.signalingState === 'stable' && peerInfo.pc.remoteDescription) {
                const offer = await peerInfo.pc.createOffer();
                await peerInfo.pc.setLocalDescription(offer);
                sendSignal(remoteId, { type: 'offer', sdp: offer.sdp });
            }
        }
    } catch (err) {
        console.error('Ошибка доступа к медиа:', err);
        alert('Не удалось получить доступ к камере/микрофону');
    }
}

// --- Измерение задержки ---
startLatencyTestBtn.addEventListener('click', () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        sendPing();
        latencyResultDiv.innerHTML = '<p>Измерение...</p>';
    } else {
        alert('WebSocket не подключён');
    }
});

// --- Получение статистики WebRTC (getStats) ---
async function collectStats() {
    let statsText = '';
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
            statsText += `Участник ${remoteId.slice(0,6)}:\n`;
            statsText += `  Битрейт видео: ${(inboundRtpStats.bytesReceived / 1024).toFixed(2)} KB\n`;
            statsText += `  Потеря пакетов: ${inboundRtpStats.packetsLost}\n`;
            statsText += `  Джиттер: ${inboundRtpStats.jitter?.toFixed(3)} с\n`;
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

// --- Запуск ---
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