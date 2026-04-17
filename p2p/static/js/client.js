const joinScreen = document.getElementById('join-screen');
const videoGrid = document.getElementById('video-grid');
const nicknameInput = document.getElementById('nickname');
const roomIdInput = document.getElementById('roomId');
const joinBtn = document.getElementById('joinBtn');
const localVideo = document.getElementById('localVideo');
const remoteVideosDiv = document.getElementById('remoteVideos');
const latencyResultDiv = document.getElementById('latencyResult');
const localStatsContent = document.getElementById('localStatsContent');
const webrtcStatsContent = document.getElementById('webrtcStatsContent');

let ws = null;
let localStream = null;
let currentRoomId = null;
let currentNickname = null;
let currentParticipantId = null;
let conferenceStartTime = null;

const peers = new Map();              // remoteId -> { pc, videoElement, stream, dataChannel }
const prevStatsMap = new Map();       // для удалённых inbound метрик (remoteId -> { bytes, timestamp })
let prevOutboundStats = {             // для локальной исходящей статистики
    video: {bytes: 0, timestamp: 0, packets: 0},
    audio: {bytes: 0, timestamp: 0, packets: 0}
};

let originalVideoTrack = null;
let canvasStream = null;
let callStartTime = null;
let firstVideoFrameTime = null;

let e2eTestInProgress = false;
let e2eStartTime = null;
let e2eRemoteMonitor = false;
let e2eMonitorInterval = null;

let collectStatsInterval = null;
let e2eTestInterval = null;
let dataChannelPingInterval = null;
let websocketPingInterval = null;

let statsHistory = {
    bitrate: [],
    jitter: [],
    rttIce: [],
    rttDataChannel: [],
    rttWebsocket: [],
    lipSync: []
};

let lastDataChannelRtt = null;

const pcConfig = {
    iceServers: [
        {urls: 'stun:stun.l.google.com:19302'},
        {urls: 'stun:stun1.l.google.com:19302'},
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

// === Вспомогательные функции ===
async function flashColorOnStream(color, duration = 300) {
    if (!localStream) return;
    const videoTrack = localStream.getVideoTracks()[0];
    if (!videoTrack) return;
    originalVideoTrack = videoTrack;

    const canvas = document.createElement('canvas');
    canvas.width = 640;
    canvas.height = 480;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = color;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    canvasStream = canvas.captureStream(30);
    const colorTrack = canvasStream.getVideoTracks()[0];

    localStream.removeTrack(videoTrack);
    localStream.addTrack(colorTrack);

    for (const [, peerInfo] of peers.entries()) {
        const senders = peerInfo.pc.getSenders();
        const videoSender = senders.find(s => s.track?.kind === 'video');
        if (videoSender) await videoSender.replaceTrack(colorTrack);
    }

    setTimeout(async () => {
        localStream.removeTrack(colorTrack);
        localStream.addTrack(originalVideoTrack);
        for (const [, peerInfo] of peers.entries()) {
            const senders = peerInfo.pc.getSenders();
            const videoSender = senders.find(s => s.track?.kind === 'video');
            if (videoSender) await videoSender.replaceTrack(originalVideoTrack);
        }
        colorTrack.stop();
        canvasStream = null;
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
        callStartTime = null;
        firstVideoFrameTime = null;
        conferenceStartTime = null;
        latencyResultDiv.innerHTML = '';
        localStatsContent.innerHTML = '— соединение потеряно —';
        webrtcStatsContent.innerHTML = '— нет соединений —';
        if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
        e2eTestInProgress = false;
        e2eRemoteMonitor = false;
        if (collectStatsInterval) clearInterval(collectStatsInterval);
        if (e2eTestInterval) clearInterval(e2eTestInterval);
        if (dataChannelPingInterval) clearInterval(dataChannelPingInterval);
        if (websocketPingInterval) clearInterval(websocketPingInterval);
        statsHistory = {bitrate: [], jitter: [], rttIce: [], rttDataChannel: [], rttWebsocket: [], lipSync: []};
        prevOutboundStats = {video: {bytes: 0, timestamp: 0, packets: 0}, audio: {bytes: 0, timestamp: 0, packets: 0}};
    };
}

function sendJoin(roomId, nickname) {
    const msg = {type: 'join', room: roomId, nickname: nickname};
    ws.send(JSON.stringify(msg));
}

function sendSignal(targetId, data) {
    const msg = {type: 'signal', target_id: targetId, data: data};
    ws.send(JSON.stringify(msg));
}

function sendPing() {
    const timestamp = Date.now();
    const msg = {type: 'ping', timestamp: timestamp};
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
            conferenceStartTime = Date.now();
            await initLocalMedia();
            startPeriodicTasks();
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
            const signal = msg.data;
            if (signal.type === 'e2e_test_start') {
                startE2eReceiver(msg.from_id);
            } else if (signal.type === 'e2e_color_change') {
                if (msg.from_id !== currentParticipantId && e2eRemoteMonitor) {
                    e2eStartTime = signal.timestamp;
                    console.log(`[E2E] Ожидаем цвет ${signal.color}, отправлено в ${e2eStartTime}`);
                }
            } else if (signal.type === 'e2e_result') {
                if (msg.from_id !== currentParticipantId && e2eTestInProgress) {
                    const delay = signal.delay;
                    latencyResultDiv.innerHTML += `<p>🎬 End-to-end задержка видео: ${delay} мс</p>`;
                    e2eTestInProgress = false;
                    if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
                    e2eRemoteMonitor = false;
                }
            } else {
                await handleSignal(msg.from_id, signal);
            }
            break;

        case 'pong': {
            const rtt = Date.now() - msg.timestamp;
            console.log(`[PONG] RTT = ${rtt} мс`);
            statsHistory.rttWebsocket.push(rtt);
            if (statsHistory.rttWebsocket.length > 50) statsHistory.rttWebsocket.shift();
            latencyResultDiv.innerHTML = `<p>⏱️ RTT через WebSocket: ${rtt} мс</p>`;
            break;
        }

        case 'e2e_test_start':
            if (msg.from_id !== currentParticipantId) startE2eReceiver(msg.from_id);
            break;
        case 'e2e_color_change':
            if (msg.from_id !== currentParticipantId && e2eRemoteMonitor) {
                e2eStartTime = msg.timestamp;
                console.log(`[E2E] Ожидаем цвет ${msg.color}, отправлено в ${e2eStartTime}`);
            }
            break;
        case 'e2e_result':
            if (msg.from_id !== currentParticipantId && e2eTestInProgress) {
                const delay = msg.delay;
                latencyResultDiv.innerHTML += `<p>🎬 End-to-end задержка видео: ${delay} мс</p>`;
                e2eTestInProgress = false;
                if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
                e2eRemoteMonitor = false;
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
            sendSignal(remoteId, {type: 'ice-candidate', candidate: event.candidate});
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
            sendSignal(remoteId, {type: 'offer', sdp: offer.sdp});
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
            dc.send(JSON.stringify({type: 'e2e_pong', sendTime: data.sendTime}));
        } else if (data.type === 'e2e_pong') {
            const rtt = Date.now() - data.sendTime;
            statsHistory.rttDataChannel.push(rtt);
            if (statsHistory.rttDataChannel.length > 50) statsHistory.rttDataChannel.shift();
            lastDataChannelRtt = rtt;
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
                await pc.setRemoteDescription(new RTCSessionDescription({type: 'offer', sdp: signalData.sdp}));
                const answer = await pc.createAnswer();
                await pc.setLocalDescription(answer);
                sendSignal(fromId, {type: 'answer', sdp: answer.sdp});
                for (const cand of peerInfo.pendingCandidates) {
                    await pc.addIceCandidate(cand);
                }
                peerInfo.pendingCandidates = [];
                break;
            case 'answer':
                await pc.setRemoteDescription(new RTCSessionDescription({type: 'answer', sdp: signalData.sdp}));
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
    for (const [, peerInfo] of peers.entries()) {
        peerInfo.pc.close();
        if (peerInfo.videoElement) peerInfo.videoElement.srcObject = null;
    }
    peers.clear();
    remoteVideosDiv.innerHTML = '';
}

async function initLocalMedia() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({video: true, audio: true});
        localVideo.srcObject = localStream;
        for (const [remoteId, peerInfo] of peers.entries()) {
            localStream.getTracks().forEach(track => {
                peerInfo.pc.addTrack(track, localStream);
            });
            if (peerInfo.pc.signalingState === 'stable' && peerInfo.pc.remoteDescription) {
                const offer = await peerInfo.pc.createOffer();
                await peerInfo.pc.setLocalDescription(offer);
                sendSignal(remoteId, {type: 'offer', sdp: offer.sdp});
            }
        }
    } catch (err) {
        console.error('[MEDIA] Ошибка:', err);
        alert('Не удалось получить доступ к камере/микрофону');
    }
}

function startPeriodicTasks() {
    if (collectStatsInterval) clearInterval(collectStatsInterval);
    if (e2eTestInterval) clearInterval(e2eTestInterval);
    if (dataChannelPingInterval) clearInterval(dataChannelPingInterval);

    collectStatsInterval = setInterval(() => {
        if (videoGrid.style.display !== 'none') collectStats();
    }, 3000);

    e2eTestInterval = setInterval(() => {
        if (peers.size > 0 && !e2eTestInProgress && conferenceStartTime) {
            startE2eVideoLatencyTest();
        }
    }, 30000);

    dataChannelPingInterval = setInterval(() => {
        if (peers.size > 0) {
            for (const [, peerInfo] of peers.entries()) {
                if (peerInfo.dataChannel && peerInfo.dataChannel.readyState === 'open') {
                    const sendTime = Date.now();
                    peerInfo.dataChannel.send(JSON.stringify({type: 'e2e_ping', sendTime}));
                    break;
                }
            }
        }
    }, 3000);

    websocketPingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
            sendPing();
        }
    }, 3000);
}

function startE2eVideoLatencyTest() {
    if (peers.size === 0) return;
    if (e2eTestInProgress) return;
    e2eTestInProgress = true;

    const remoteId = peers.keys().next().value;
    sendSignal(remoteId, {type: 'e2e_test_start'});

    setTimeout(async () => {
        if (!localStream) {
            e2eTestInProgress = false;
            return;
        }
        e2eStartTime = Date.now();
        const colors = ['red', 'lime', 'blue', 'yellow', 'magenta'];
        const color = colors[Math.floor(Math.random() * colors.length)];
        await flashColorOnStream(color, 300);
        sendSignal(remoteId, {
            type: 'e2e_color_change',
            timestamp: e2eStartTime,
            color: color
        });

        setTimeout(() => {
            if (e2eTestInProgress) {
                latencyResultDiv.innerHTML += '<p>⚠️ Тест E2E не завершился (таймаут)</p>';
                e2eTestInProgress = false;
                if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
            }
        }, 10000);
    }, 500);
}

function startE2eReceiver(senderId) {
    if (e2eRemoteMonitor) return;
    e2eRemoteMonitor = true;
    const peerInfo = peers.get(senderId);
    if (!peerInfo || !peerInfo.videoElement) {
        e2eRemoteMonitor = false;
        return;
    }

    let lastColor = null;
    e2eMonitorInterval = setInterval(() => {
        if (!e2eRemoteMonitor) {
            clearInterval(e2eMonitorInterval);
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
        if (dominant && dominant !== lastColor && e2eStartTime) {
            lastColor = dominant;
            const detectTime = Date.now();
            const delay = detectTime - e2eStartTime;
            sendSignal(senderId, {type: 'e2e_result', delay: delay});
            e2eRemoteMonitor = false;
            clearInterval(e2eMonitorInterval);
        }
    }, 50);
}

// === Сбор статистики: удалённая + локальная ===
async function collectStats() {
    // ---- 1. Локальная статистика (исходящие потоки) ----
    let localStatsText = '';
    if (peers.size > 0 && localStream) {
        // Берём первого активного пира для анализа outbound-треков
        const anyPeer = peers.values().next().value;
        if (anyPeer && anyPeer.pc) {
            const pc = anyPeer.pc;
            const stats = await pc.getStats();
            let outVideo = null, outAudio = null;

            stats.forEach(report => {
                if (report.type === 'outbound-rtp') {
                    if (report.kind === 'video') outVideo = report;
                    if (report.kind === 'audio') outAudio = report;
                }
            });

            const now = Date.now();

            // Видео (исходящий)
            if (outVideo) {
                const currentBytes = outVideo.bytesSent;
                const currentPackets = outVideo.packetsSent;
                const prevVideo = prevOutboundStats.video;
                let videoBitrate = 0;
                if (prevVideo.bytes && prevVideo.timestamp) {
                    const bytesDiff = currentBytes - prevVideo.bytes;
                    const timeDiffSec = (now - prevVideo.timestamp) / 1000;
                    if (timeDiffSec > 0 && bytesDiff >= 0) {
                        videoBitrate = (bytesDiff * 8) / timeDiffSec / 1000; // kbps
                    }
                }
                prevOutboundStats.video = {bytes: currentBytes, timestamp: now, packets: currentPackets};

                const fps = outVideo.framesPerSecond || '—';
                const width = outVideo.frameWidth || '—';
                const height = outVideo.frameHeight || '—';
                const codec = outVideo.codecId ? outVideo.codecId.split('/').pop() : '—';

                localStatsText += `🎥 Видео (исх.):\n`;
                localStatsText += `   Битрейт: ${videoBitrate.toFixed(2)} kbps\n`;
                localStatsText += `   Разрешение: ${width}×${height}\n`;
                localStatsText += `   FPS: ${fps}\n`;
                localStatsText += `   Отправлено пакетов: ${currentPackets}\n`;
                localStatsText += `   Кодек: ${codec}\n`;
            } else {
                localStatsText += `🎥 Видео: нет активного outbound-трека\n`;
            }

            // Аудио (исходящий)
            if (outAudio) {
                const currentBytes = outAudio.bytesSent;
                const prevAudio = prevOutboundStats.audio;
                let audioBitrate = 0;
                if (prevAudio.bytes && prevAudio.timestamp) {
                    const bytesDiff = currentBytes - prevAudio.bytes;
                    const timeDiffSec = (now - prevAudio.timestamp) / 1000;
                    if (timeDiffSec > 0 && bytesDiff >= 0) {
                        audioBitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    }
                }
                prevOutboundStats.audio = {bytes: currentBytes, timestamp: now, packets: outAudio.packetsSent};
                localStatsText += `🎙️ Аудио (исх.):\n`;
                localStatsText += `   Битрейт: ${audioBitrate.toFixed(2)} kbps\n`;
                localStatsText += `   Отправлено пакетов: ${outAudio.packetsSent}\n`;
            } else {
                localStatsText += `🎙️ Аудио: нет активного outbound-трека\n`;
            }

            // Доп. информация о локальном захвате
            const videoTrack = localStream.getVideoTracks()[0];
            if (videoTrack) {
                const settings = videoTrack.getSettings();
                localStatsText += `📷 Камера: ${settings.width || '?'}×${settings.height || '?'} @ ${settings.frameRate || '?'} fps\n`;
            }
        } else {
            localStatsText = 'Ожидание установки соединения...';
        }
    } else {
        localStatsText = 'Нет активных пиров для анализа исходящего трафика';
    }
    localStatsContent.innerHTML = localStatsText || '— данные недоступны —';

    // ---- 2. Удалённая статистика (входящие потоки) ----
    let remoteStatsText = '';
    const nowRemote = Date.now();
    let hasRemoteData = false;
    let avgBitrate = 0, avgJitter = 0, avgRttIce = 0, avgLipSync = 0;

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
            let currentBitrate = 0;
            if (prev && prev.bytes !== undefined && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (nowRemote - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    currentBitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsHistory.bitrate.push(currentBitrate);
                    if (statsHistory.bitrate.length > 50) statsHistory.bitrate.shift();
                }
            }
            prevStatsMap.set(remoteId, {bytes: currentBytes, timestamp: nowRemote});

            const currentJitter = (inboundVideo.jitter * 1000);
            statsHistory.jitter.push(currentJitter);
            if (statsHistory.jitter.length > 50) statsHistory.jitter.shift();

            remoteStatsText += `👤 Участник ${remoteId.slice(0, 6)}:\n`;
            remoteStatsText += `   Битрейт видео (вх.): ${currentBitrate.toFixed(2)} kbps\n`;
            remoteStatsText += `   Потеря пакетов: ${inboundVideo.packetsLost}\n`;
            remoteStatsText += `   Джиттер: ${currentJitter.toFixed(2)} мс\n`;
            hasRemoteData = true;
        } else {
            remoteStatsText += `👤 Участник ${remoteId.slice(0, 6)}: нет видео-потока\n`;
        }

        if (candidatePair) {
            const currentRttIce = candidatePair.currentRoundTripTime * 1000;
            statsHistory.rttIce.push(currentRttIce);
            if (statsHistory.rttIce.length > 50) statsHistory.rttIce.shift();
            remoteStatsText += `   RTT (ICE): ${currentRttIce.toFixed(2)} мс\n`;
        }

        if (inboundVideo && inboundAudio && inboundVideo.mediaTime !== undefined && inboundAudio.mediaTime !== undefined) {
            const diff = Math.abs(inboundVideo.mediaTime - inboundAudio.mediaTime) * 1000;
            if (diff < 500) {
                statsHistory.lipSync.push(diff);
                if (statsHistory.lipSync.length > 50) statsHistory.lipSync.shift();
                remoteStatsText += `   Расхождение аудио/видео: ${diff.toFixed(2)} мс\n`;
            }
        }
        remoteStatsText += '\n';
    }

    if (!hasRemoteData) {
        remoteStatsText = 'Нет активных соединений с удалёнными участниками\n';
    }

    // Средние значения за конференцию
    const avgBitrateVal = statsHistory.bitrate.length ? (statsHistory.bitrate.reduce((a, b) => a + b, 0) / statsHistory.bitrate.length).toFixed(2) : '—';
    const avgJitterVal = statsHistory.jitter.length ? (statsHistory.jitter.reduce((a, b) => a + b, 0) / statsHistory.jitter.length).toFixed(2) : '—';
    const avgRttIceVal = statsHistory.rttIce.length ? (statsHistory.rttIce.reduce((a, b) => a + b, 0) / statsHistory.rttIce.length).toFixed(2) : '—';
    const avgRttDataVal = statsHistory.rttDataChannel.length ? (statsHistory.rttDataChannel.reduce((a, b) => a + b, 0) / statsHistory.rttDataChannel.length).toFixed(2) : '—';
    const avgLipSyncVal = statsHistory.lipSync.length ? (statsHistory.lipSync.reduce((a, b) => a + b, 0) / statsHistory.lipSync.length).toFixed(2) : '—';
    const avgRttWebsocketVal = statsHistory.rttWebsocket.length ? (statsHistory.rttWebsocket.reduce((a, b) => a + b, 0) / statsHistory.rttWebsocket.length).toFixed(2) : '—';
    let conferenceDuration = '—';
    if (conferenceStartTime) {
        const durationSec = (Date.now() - conferenceStartTime) / 1000;
        const minutes = Math.floor(durationSec / 60);
        const seconds = Math.floor(durationSec % 60);
        conferenceDuration = `${minutes}м ${seconds}с`;
    }

    remoteStatsText += `\n📈 СРЕДНИЕ ЗА ВРЕМЯ КОНФЕРЕНЦИИ (${conferenceDuration}):\n`;
    remoteStatsText += `   Битрейт видео (вх.): ${avgBitrateVal} kbps\n`;
    remoteStatsText += `   Джиттер: ${avgJitterVal} мс\n`;
    remoteStatsText += `   RTT ICE: ${avgRttIceVal} мс\n`;
    remoteStatsText += `   RTT WebSocket: ${avgRttWebsocketVal} мс\n`;
    remoteStatsText += `   RTT DataChannel: ${avgRttDataVal} мс\n`;
    remoteStatsText += `   Расхождение аудио/видео: ${avgLipSyncVal} мс\n`;

    webrtcStatsContent.innerHTML = remoteStatsText;
}

// === Запуск ===
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