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

let callStartTime = null;
let firstVideoFrameTime = null;
let e2eTestInProgress = false;
let e2eStartTime = null;
let e2eExpectedColor = null;
let e2eRemoteCallback = null;

let lipSyncResults = [];

const pcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        {
            urls: [
                'turn:178.154.215.177:3478?transport=udp',
                'turn:178.154.215.177:3478?transport=tcp'
            ],
            username: 'webrtc',
            credential: 'webrtc_password'
        }
    ],
    iceCandidatePoolSize: 10
};

let canvasOverlay = null;

function startColorFlash(color, duration = 500) {
    if (!localVideo.srcObject) return;
    if (!canvasOverlay) {
        canvasOverlay = document.createElement('canvas');
        canvasOverlay.style.position = 'absolute';
        canvasOverlay.style.top = 0;
        canvasOverlay.style.left = 0;
        canvasOverlay.style.width = '100%';
        canvasOverlay.style.height = '100%';
        canvasOverlay.style.pointerEvents = 'none';
        canvasOverlay.style.zIndex = 10;
        localVideo.parentElement.style.position = 'relative';
        localVideo.parentElement.appendChild(canvasOverlay);
    }
    canvasOverlay.width = localVideo.videoWidth || 640;
    canvasOverlay.height = localVideo.videoHeight || 480;
    const ctx = canvasOverlay.getContext('2d');
    ctx.fillStyle = color;
    ctx.fillRect(0, 0, canvasOverlay.width, canvasOverlay.height);
    setTimeout(() => {
        if (canvasOverlay) {
            const ctx2 = canvasOverlay.getContext('2d');
            ctx2.clearRect(0, 0, canvasOverlay.width, canvasOverlay.height);
        }
    }, duration);
}

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
        // Сброс метрик
        callStartTime = null;
        firstVideoFrameTime = null;
        latencyResultDiv.innerHTML = '';
        webrtcStatsDiv.innerHTML = '';
    };
}

function sendJoin(roomId, nickname) {
    const msg = { type: 'join', room: roomId, nickname: nickname };
    ws.send(JSON.stringify(msg));
}

function sendSignal(targetId, data) {
    const msg = { type: 'signal', target_id: targetId, data: data };
    ws.send(JSON.stringify(msg));
}

function sendPing() {
    const timestamp = Date.now();
    const msg = { type: 'ping', timestamp: timestamp };
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

        case 'e2e_start':
            if (msg.from_id !== currentParticipantId) {
                startE2eReceiver(msg.from_id);
            }
            break;
        case 'e2e_color_change':
            if (msg.from_id !== currentParticipantId && e2eTestInProgress) {
                const receivedTime = Date.now();
                const sendTime = msg.timestamp;
                const delay = receivedTime - sendTime;
                latencyResultDiv.innerHTML += `<p>🎬 End-to-end задержка видео: ${delay} мс (приблизительно)</p>`;
                e2eTestInProgress = false;
                if (e2eRemoteCallback) e2eRemoteCallback(delay);
            }
            break;
        case 'e2e_result':
            if (msg.from_id !== currentParticipantId && e2eRemoteCallback) {
                e2eRemoteCallback(msg.delay);
                e2eRemoteCallback = null;
            }
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
        pendingCandidates: [],
        dataChannel: null
    };
    peers.set(remoteId, peerInfo);

    if (isInitiator) {
        const dc = pc.createDataChannel('metrics');
        setupDataChannel(dc, remoteId);
        peerInfo.dataChannel = dc;
    }

    pc.ondatachannel = (event) => {
        console.log(`[DC] Получен data channel от ${remoteId}`);
        setupDataChannel(event.channel, remoteId);
        peerInfo.dataChannel = event.channel;
    };

    if (localStream) {
        console.log(`[PC] Добавляем локальные треки для ${remoteId}`);
        localStream.getTracks().forEach(track => {
            pc.addTrack(track, localStream);
        });
    }

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            sendSignal(remoteId, { type: 'ice-candidate', candidate: event.candidate });
        }
    };

    pc.ontrack = (event) => {
        console.log(`[TRACK] Получен трек от ${remoteId}, kind=${event.track.kind}`);
        if (peerInfo.videoElement.srcObject !== event.streams[0]) {
            peerInfo.videoElement.srcObject = event.streams[0];
            peerInfo.stream = event.streams[0];

            if (firstVideoFrameTime === null && callStartTime !== null) {
                firstVideoFrameTime = performance.now();
                const setupTime = firstVideoFrameTime - callStartTime;
                console.log(`[METRIC] Call Setup Time = ${setupTime.toFixed(2)} ms`);
                latencyResultDiv.innerHTML += `<p>📞 Время установления соединения: ${setupTime.toFixed(2)} мс</p>`;
            }
        }
    };

    pc.oniceconnectionstatechange = () => {
        if (pc.iceConnectionState === 'disconnected' || pc.iceConnectionState === 'failed') {
            removePeer(remoteId);
        }
    };

    if (isInitiator) {
        try {
            const offer = await pc.createOffer();
            await pc.setLocalDescription(offer);
            sendSignal(remoteId, { type: 'offer', sdp: offer.sdp });
        } catch (err) {
            console.error(`[OFFER] Ошибка:`, err);
        }
    }
}

function setupDataChannel(dc, remoteId) {
    dc.onopen = () => console.log(`[DC] Канал с ${remoteId} открыт`);
    dc.onclose = () => console.log(`[DC] Канал с ${remoteId} закрыт`);
    dc.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.type === 'e2e_ping') {
            // Ответ на end-to-end тест (через data channel)
            dc.send(JSON.stringify({ type: 'e2e_pong', sendTime: data.sendTime, recvTime: Date.now() }));
        } else if (data.type === 'e2e_pong') {
            const now = Date.now();
            const rtt = now - data.sendTime;
            latencyResultDiv.innerHTML += `<p>📡 Задержка data channel (RTT): ${rtt} мс</p>`;
        }
    };
}

async function handleSignal(fromId, signalData) {
    let peerInfo = peers.get(fromId);
    if (!peerInfo && signalData.type === 'offer') {
        await createPeerConnection(fromId, false);
        peerInfo = peers.get(fromId);
    }
    if (!peerInfo) return;

    const pc = peerInfo.pc;
    try {
        switch (signalData.type) {
            case 'offer':
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: signalData.sdp }));
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                sendSignal(fromId, { type: 'answer', sdp: answer.sdp });
                for (const cand of peerInfo.pendingCandidates) {
                    await pc.addIceCandidate(cand);
                }
                peerInfo.pendingCandidates = [];
                break;
            case 'answer':
                await pc.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: signalData.sdp }));
                for (const cand of peerInfo.pendingCandidates) {
                    await pc.addIceCandidate(cand);
                }
                peerInfo.pendingCandidates = [];
                break;
            case 'ice-candidate':
                if (signalData.candidate) {
                    const candidate = new RTCIceCandidate(signalData.candidate);
                    if (pc.remoteDescription) {
                        await pc.addIceCandidate(candidate);
                    } else {
                        peerInfo.pendingCandidates.push(candidate);
                    }
                }
                break;
        }
    } catch (err) {
        console.error(`[SIGNAL] Ошибка:`, err);
    }
}

function removePeer(remoteId) {
    const peerInfo = peers.get(remoteId);
    if (peerInfo) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) peerInfo.videoElement.srcObject = null;
        const container = document.getElementById(`remote-${remoteId}`);
        if (container) container.remove();
        peers.delete(remoteId);
        prevStatsMap.delete(remoteId);
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

async function initLocalMedia() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        localVideo.srcObject = localStream;
        for (const [remoteId, peerInfo] of peers.entries()) {
            localStream.getTracks().forEach(track => {
                peerInfo.pc.addTrack(track, localStream);
            });
            if (peerInfo.pc.signalingState === 'stable' && peerInfo.pc.remoteDescription) {
                const offer = await peerInfo.pc.createOffer();
                await peerInfo.pc.setLocalDescription(offer);
                sendSignal(remoteId, { type: 'offer', sdp: offer.sdp });
            }
        }
    } catch (err) {
        console.error('[MEDIA] Ошибка:', err);
        alert('Не удалось получить доступ к камере/микрофону');
    }
}

function startE2eVideoLatencyTest() {
    if (peers.size === 0) {
        alert('Нет удалённых участников для теста');
        return;
    }
    if (e2eTestInProgress) return;
    e2eTestInProgress = true;

    const remoteId = peers.keys().next().value;

    sendSignal(remoteId, { type: 'e2e_start' });

    setTimeout(() => {
        if (!localStream) return;
        e2eStartTime = Date.now();

        const colors = ['red', 'lime', 'blue', 'yellow', 'magenta'];
        e2eExpectedColor = colors[Math.floor(Math.random() * colors.length)];
        startColorFlash(e2eExpectedColor, 300);
        sendSignal(remoteId, {
            type: 'e2e_color_change',
            timestamp: e2eStartTime,
            color: e2eExpectedColor
        });

        setTimeout(() => {
            if (e2eTestInProgress) {
                latencyResultDiv.innerHTML += '<p>⚠️ Тест E2E не завершился (таймаут)</p>';
                e2eTestInProgress = false;
            }
        }, 5000);
    }, 500);
}

function startE2eReceiver(senderId) {
    console.log('[E2E] Начинаем мониторинг видео для end-to-end теста');
    const peerInfo = peers.get(senderId);
    if (!peerInfo || !peerInfo.videoElement) return;

    let lastColor = null;
    const interval = setInterval(() => {
        if (!e2eTestInProgress) {
            clearInterval(interval);
            return;
        }
        const video = peerInfo.videoElement;
        if (!video.videoWidth || !video.videoHeight) return;
        const canvas = document.createElement('canvas');
        canvas.width = video.videoWidth;
        canvas.height = video.videoHeight;
        const ctx = canvas.getContext('2d');
        ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
        const centerX = canvas.width / 2;
        const centerY = canvas.height / 2;
        const pixel = ctx.getImageData(centerX, centerY, 1, 1).data;
        const r = pixel[0], g = pixel[1], b = pixel[2];
        let dominant = '';
        if (r > 200 && g < 100 && b < 100) dominant = 'red';
        else if (g > 200 && r < 100 && b < 100) dominant = 'lime';
        else if (b > 200 && r < 100 && g < 100) dominant = 'blue';
        else if (r > 200 && g > 200 && b < 100) dominant = 'yellow';
        else if (r > 200 && g < 100 && b > 200) dominant = 'magenta';
        if (dominant && dominant !== lastColor) {
            lastColor = dominant;
            const detectTime = Date.now();

            sendSignal(senderId, { type: 'e2e_result', delay: detectTime - e2eStartTime });
            clearInterval(interval);
            e2eTestInProgress = false;
        }
    }, 50);
}

async function collectStats() {
    let statsText = '';
    const now = Date.now();

    for (const [remoteId, peerInfo] of peers.entries()) {
        const pc = peerInfo.pc;
        if (pc.iceConnectionState !== 'connected' && pc.iceConnectionState !== 'completed') continue;

        const stats = await pc.getStats();
        let inboundVideo = null, inboundAudio = null, candidatePair = null;

        stats.forEach(report => {
            if (report.type === 'inbound-rtp' && report.kind === 'video') inboundVideo = report;
            if (report.type === 'inbound-rtp' && report.kind === 'audio') inboundAudio = report;
            if (report.type === 'candidate-pair' && report.nominated === true) candidatePair = report;
        });

        if (inboundVideo) {
            const currentBytes = inboundVideo.bytesReceived;
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
            statsText += `  Потеря пакетов: ${inboundVideo.packetsLost}\n`;
            statsText += `  Джиттер: ${(inboundVideo.jitter * 1000).toFixed(2)} мс\n`;
        } else {
            statsText += `Участник ${remoteId.slice(0,6)}: нет видео-потока\n`;
        }

        if (candidatePair) {
            statsText += `  RTT (ICE): ${(candidatePair.currentRoundTripTime * 1000).toFixed(2)} мс\n`;
        }

        // Анализ синхронизации аудио/видео (lip sync)
        if (inboundVideo && inboundAudio && inboundVideo.timestamp && inboundAudio.timestamp) {
            const videoTime = inboundVideo.timestamp / 1000;   // seconds
            const audioTime = inboundAudio.timestamp / 1000;
            const diff = Math.abs(videoTime - audioTime) * 1000; // ms
            if (diff < 500) { // игнорируем выбросы
                lipSyncResults.push(diff);
                if (lipSyncResults.length > 10) lipSyncResults.shift();
                const avgLipSync = lipSyncResults.reduce((a,b) => a+b,0) / lipSyncResults.length;
                statsText += `  Расхождение аудио/видео (приблиз.): ${avgLipSync.toFixed(2)} мс\n`;
            }
        }
        statsText += '\n';
    }

    if (statsText === '') statsText = 'Нет активных соединений';
    webrtcStatsDiv.innerHTML = `<pre>${statsText}</pre>`;
}

startLatencyTestBtn.addEventListener('click', () => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        if (peers.size > 0) {
            const firstPeer = peers.values().next().value;
            if (firstPeer.dataChannel && firstPeer.dataChannel.readyState === 'open') {
                const sendTime = Date.now();
                firstPeer.dataChannel.send(JSON.stringify({ type: 'e2e_ping', sendTime }));
                latencyResultDiv.innerHTML = '<p>Измерение через DataChannel...</p>';
                // Ждём ответа в setupDataChannel
            } else {
                sendPing(); // fallback на WebSocket ping
            }
        } else {
            sendPing();
        }
    } else {
        alert('WebSocket не подключён');
    }
});


if (!document.getElementById('e2eVideoTestBtn')) {
    const e2eBtn = document.createElement('button');
    e2eBtn.id = 'e2eVideoTestBtn';
    e2eBtn.textContent = 'Замерить E2E задержку видео (мигание)';
    e2eBtn.style.marginLeft = '10px';
    startLatencyTestBtn.parentNode.insertBefore(e2eBtn, startLatencyTestBtn.nextSibling);
    e2eBtn.addEventListener('click', startE2eVideoLatencyTest);
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
    callStartTime = performance.now();
    firstVideoFrameTime = null;
    if (ws) ws.close();
    connectWebSocket(roomId, nickname);
});