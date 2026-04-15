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

// Хранилище пиров: key = remoteParticipantId, value = { pc, videoElement, stream }
const peers = new Map();

// --- Конфигурация STUN/TURN серверов (можно заменить на свои) ---
const pcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' }
    ]
};

// --- Функции работы с WebSocket ---
function connectWebSocket(roomId, nickname, participantId) {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${protocol}://${window.location.host}/ws`);

    ws.onopen = () => {
        console.log('WebSocket соединение установлено');
        // Отправляем команду join
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
        // При отключении очищаем все соединения
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
    const msg = {
        type: 'join',
        room: roomId,
        nickname: nickname
    };
    ws.send(JSON.stringify(msg));
}

function sendSignal(targetId, data) {
    const msg = {
        type: 'signal',
        target_id: targetId,
        data: data
    };
    ws.send(JSON.stringify(msg));
}

function sendPing() {
    const timestamp = Date.now();
    const msg = {
        type: 'ping',
        timestamp: timestamp
    };
    ws.send(JSON.stringify(msg));
    return timestamp;
}

// --- Обработка входящих сигнальных сообщений ---
async function handleSignalingMessage(msg) {
    switch (msg.type) {
        case 'joined':
            currentRoomId = msg.room;
            currentNickname = msg.nickname;
            currentParticipantId = msg.participant_id;
            console.log(`Успешно присоединились: комната ${currentRoomId}, id=${currentParticipantId}`);
            // Показываем видео сетку
            joinScreen.style.display = 'none';
            videoGrid.style.display = 'flex';
            // Запрашиваем доступ к камере и микрофону
            await initLocalMedia();
            break;

        case 'existing_participants':
            console.log('Существующие участники:', msg.participants);
            for (const p of msg.participants) {
                await createPeerConnection(p.id, true); // true означает, что мы инициируем соединение (offer)
            }
            break;

        case 'participant_joined':
            console.log('Новый участник:', msg.participant);
            await createPeerConnection(msg.participant.id, true);
            break;

        case 'participant_left':
            console.log('Участник покинул:', msg.participant_id);
            removePeer(msg.participant_id);
            break;

        case 'signal':
            // Сигнал от другого участника
            const fromId = msg.from_id;
            const signalData = msg.data;
            await handleSignal(fromId, signalData);
            break;

        case 'pong':
            const rtt = Date.now() - msg.timestamp;
            latencyResultDiv.innerHTML = `<p>⏱️ Задержка RTT (через WebSocket): ${rtt} мс</p>`;
            break;

        case 'error':
            console.error('Ошибка сервера:', msg.message);
            alert(`Ошибка: ${msg.message}`);
            break;

        default:
            console.warn('Неизвестный тип сообщения:', msg.type);
    }
}

// --- WebRTC: создание PeerConnection ---
async function createPeerConnection(remoteParticipantId, isInitiator) {
    if (peers.has(remoteParticipantId)) {
        console.warn(`Peer ${remoteParticipantId} уже существует`);
        return;
    }

    const pc = new RTCPeerConnection(pcConfig);
    const videoElement = document.createElement('video');
    videoElement.autoplay = true;
    videoElement.playsInline = true;
    videoElement.classList.add('remote-video');
    const container = document.createElement('div');
    container.className = 'remote-video-container';
    container.id = `remote-${remoteParticipantId}`;
    container.appendChild(videoElement);
    const label = document.createElement('div');
    label.className = 'label';
    label.textContent = `Участник ${remoteParticipantId.slice(0, 6)}`;
    container.appendChild(label);
    remoteVideosDiv.appendChild(container);

    // Сохраняем видеоэлемент для последующего использования
    const peerInfo = {
        pc: pc,
        videoElement: videoElement,
        stream: null
    };
    peers.set(remoteParticipantId, peerInfo);

    // Добавляем локальный трек, если он уже есть
    if (localStream) {
        localStream.getTracks().forEach(track => {
            pc.addTrack(track, localStream);
        });
    }

    // Обработка ICE-кандидатов
    pc.onicecandidate = (event) => {
        if (event.candidate) {
            sendSignal(remoteParticipantId, {
                type: 'ice-candidate',
                candidate: event.candidate
            });
        }
    };

    // Получение удалённого потока
    pc.ontrack = (event) => {
        console.log(`Получен трек от ${remoteParticipantId}`);
        if (peerInfo.videoElement.srcObject !== event.streams[0]) {
            peerInfo.videoElement.srcObject = event.streams[0];
            peerInfo.stream = event.streams[0];
        }
    };

    pc.oniceconnectionstatechange = () => {
        console.log(`ICE состояние для ${remoteParticipantId}: ${pc.iceConnectionState}`);
        if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
            removePeer(remoteParticipantId);
        }
    };

    if (isInitiator) {
        // Создаём offer
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        sendSignal(remoteParticipantId, {
            type: 'offer',
            sdp: offer.sdp
        });
    }
}

async function handleSignal(fromId, signalData) {
    let peerInfo = peers.get(fromId);
    if (!peerInfo && (signalData.type === 'offer' || signalData.type === 'answer')) {
        // Если пришёл offer или answer, а пира нет - создаём пассивное соединение
        await createPeerConnection(fromId, false);
        peerInfo = peers.get(fromId);
    }
    if (!peerInfo) {
        console.warn(`Нет peer для ${fromId}, игнорируем сигнал ${signalData.type}`);
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
                break;

            case 'answer':
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signalData.sdp }));
                break;

            case 'ice-candidate':
                if (signalData.candidate) {
                    await pc.addIceCandidate(new RTCIceCandidate(signalData.candidate));
                }
                break;

            default:
                console.warn('Неизвестный тип сигнала:', signalData.type);
        }
    } catch (err) {
        console.error(`Ошибка обработки сигнала от ${fromId}:`, err);
    }
}

function removePeer(remoteParticipantId) {
    const peerInfo = peers.get(remoteParticipantId);
    if (peerInfo) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) {
            peerInfo.videoElement.srcObject = null;
        }
        // Удаляем контейнер из DOM
        const container = document.getElementById(`remote-${remoteParticipantId}`);
        if (container) container.remove();
        peers.delete(remoteParticipantId);
        console.log(`Peer ${remoteParticipantId} удалён`);
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

        // Добавляем треки во все существующие пиры
        for (const [remoteId, peerInfo] of peers.entries()) {
            localStream.getTracks().forEach(track => {
                peerInfo.pc.addTrack(track, localStream);
            });
            // Если соединение ещё не установлено, но offer уже отправлен, пересоздавать не нужно
            // Просто добавили треки – при renegotiation понадобится offer/answer.
            // Упростим: после добавления треков нужно переслать новый offer.
            // Для простоты: перезапустим ICE (restart ice) или создадим renegotiation.
            // Но в данном примере предполагаем, что треки добавляются до установки соединения.
            // Если соединение уже установлено, то нужно создать offer заново.
            if (peerInfo.pc.signalingState === 'stable' && peerInfo.pc.remoteDescription) {
                // Небольшая хитрость: переотправляем offer с добавлением треков
                const offer = await peerInfo.pc.createOffer();
                await peerInfo.pc.setLocalDescription(offer);
                sendSignal(remoteId, {
                    type: 'offer',
                    sdp: offer.sdp
                });
            }
        }
    } catch (err) {
        console.error('Ошибка доступа к медиа:', err);
        alert('Не удалось получить доступ к камере/микрофону');
    }
}

// --- Измерение задержки (RTT) ---
startLatencyTestBtn.addEventListener('click', () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        const pingTime = sendPing();
        latencyResultDiv.innerHTML = '<p>Измерение...</p>';
        // Можно также измерить задержку через getStats, но здесь используем ping/pong
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
            if (report.type === 'inbound-rtp' && report.kind === 'video') {
                inboundRtpStats = report;
            }
            if (report.type === 'candidate-pair' && report.nominated === true) {
                candidatePairStats = report;
            }
        });
        if (inboundRtpStats) {
            statsText += `Участник ${remoteId.slice(0,6)}:\n`;
            statsText += `  - Битрейт видео: ${(inboundRtpStats.bytesReceived / 1024).toFixed(2)} KB\n`;
            statsText += `  - Потеря пакетов: ${inboundRtpStats.packetsLost}\n`;
            statsText += `  - Джиттер: ${inboundRtpStats.jitter?.toFixed(3)} с\n`;
        }
        if (candidatePairStats) {
            statsText += `  - RTT (ICE): ${(candidatePairStats.currentRoundTripTime * 1000).toFixed(2)} мс\n`;
        }
        statsText += '\n';
    }
    if (statsText === '') statsText = 'Нет активных соединений или статистика недоступна';
    webrtcStatsDiv.innerHTML = `<pre>${statsText}</pre>`;
}

// Периодический сбор статистики (раз в 5 секунд)
setInterval(() => {
    if (videoGrid.style.display !== 'none') {
        collectStats();
    }
}, 5000);

// --- Запуск при подключении ---
joinBtn.addEventListener('click', () => {
    const nickname = nicknameInput.value.trim();
    const roomId = roomIdInput.value.trim();
    if (!nickname || !roomId) {
        alert('Введите имя и ID комнаты');
        return;
    }
    // Закрываем предыдущее соединение, если есть
    if (ws) ws.close();
    connectWebSocket(roomId, nickname);
});