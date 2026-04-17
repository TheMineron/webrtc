const joinScreen = document.getElementById('join-screen');
const videoGrid = document.getElementById('video-grid');
const nicknameInput = document.getElementById('nickname');
const roomIdInput = document.getElementById('roomId');
const joinBtn = document.getElementById('joinBtn');
const localVideo = document.getElementById('localVideo');
const remoteVideosDiv = document.getElementById('remoteVideos');
const latencyResultDiv = document.getElementById('latencyResult');
const webrtcStatsDiv = document.getElementById('webrtcStats');

let ws = null;
let localStream = null;
let currentRoomId = null;
let currentNickname = null;
let currentParticipantId = null;
let conferenceStartTime = null;

const peers = new Map();
const prevStatsMap = new Map();      // для входящих байтов
const prevOutStatsMap = new Map();   // для исходящих байтов

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
    bitrateIn: [],
    bitrateOut: [],
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
        webrtcStatsDiv.innerHTML = '';
        if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
        e2eTestInProgress = false;
        e2eRemoteMonitor = false;
        if (collectStatsInterval) clearInterval(collectStatsInterval);
        if (e2eTestInterval) clearInterval(e2eTestInterval);
        if (dataChannelPingInterval) clearInterval(dataChannelPingInterval);
        if (websocketPingInterval) clearInterval(websocketPingInterval);
        statsHistory = {bitrateIn: [], bitrateOut: [], jitter: [], rttIce: [], rttDataChannel: [], rttWebsocket: [], lipSync: []};
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
            if (msg.from_id !== currentParticipantId) {
                startE2eReceiver(msg.from_id);
            }
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
        prevOutStatsMap.delete(remoteId);
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

async function collectStats() {
    let statsText = '';
    const now = Date.now();
    let currentBitrateIn = 0, currentBitrateOut = 0, currentJitter = 0, currentRttIce = 0, currentLipSync = 0;
    let hasData = false;

    // --- Локальная статистика (исходящие потоки) ---
    let localStatsText = '';
    let localVideoTrack = localStream ? localStream.getVideoTracks()[0] : null;
    if (localVideoTrack) {
        const settings = localVideoTrack.getSettings();
        const resolution = settings.width && settings.height ? `${settings.width}x${settings.height}` : 'неизвестно';
        const fps = settings.frameRate || '?';
        localStatsText = `🎥 Вы (локально): ${resolution} @ ${fps} fps\n`;
    } else {
        localStatsText = `🎥 Вы (локально): видео не активно\n`;
    }

    // Сбор исходящей статистики (outbound-rtp) по каждому пиру
    let totalOutBitrate = 0;
    let outCount = 0;
    let totalOutPacketsLost = 0;

    for (const [remoteId, peerInfo] of peers.entries()) {
        const pc = peerInfo.pc;
        if (pc.iceConnectionState !== 'connected' && pc.iceConnectionState !== 'completed') continue;

        const stats = await pc.getStats();
        let outVideo = null, outAudio = null;

        stats.forEach(report => {
            if (report.type === 'outbound-rtp' && report.kind === 'video') outVideo = report;
            if (report.type === 'outbound-rtp' && report.kind === 'audio') outAudio = report;
        });

        if (outVideo) {
            const currentBytes = outVideo.bytesSent;
            const prev = prevOutStatsMap.get(remoteId);
            if (prev && prev.bytes !== undefined && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (now - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    const bitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    totalOutBitrate += bitrate;
                    outCount++;
                    statsHistory.bitrateOut.push(bitrate);
                    if (statsHistory.bitrateOut.length > 50) statsHistory.bitrateOut.shift();
                    currentBitrateOut = bitrate;
                }
            }
            prevOutStatsMap.set(remoteId, {bytes: currentBytes, timestamp: now});
            if (outVideo.packetsLost !== undefined) totalOutPacketsLost += outVideo.packetsLost;
        }
    }

    if (outCount > 0) {
        const avgOutBitrate = (totalOutBitrate / outCount).toFixed(2);
        localStatsText += `  📤 Исходящий битрейт (видео): ${avgOutBitrate} kbps\n`;
        localStatsText += `  📤 Потеряно пакетов (суммарно): ${totalOutPacketsLost}\n`;
    } else {
        localStatsText += `  📤 Нет активных исходящих потоков\n`;
    }

    statsText += localStatsText + '\n';

    // --- Статистика по удалённым участникам (входящие + исходящие к ним) ---
    for (const [remoteId, peerInfo] of peers.entries()) {
        const pc = peerInfo.pc;
        if (pc.iceConnectionState !== 'connected' && pc.iceConnectionState !== 'completed') continue;

        const stats = await pc.getStats();
        let inboundVideo = null, inboundAudio = null, candidatePair = null;
        let outVideo = null, outAudio = null;

        stats.forEach(report => {
            if (report.type === 'inbound-rtp' && report.kind === 'video') inboundVideo = report;
            if (report.type === 'inbound-rtp' && report.kind === 'audio') inboundAudio = report;
            if (report.type === 'candidate-pair' && report.nominated === true) candidatePair = report;
            if (report.type === 'outbound-rtp' && report.kind === 'video') outVideo = report;
            if (report.type === 'outbound-rtp' && report.kind === 'audio') outAudio = report;
        });

        statsText += `👤 Участник ${remoteId.slice(0, 6)}:\n`;

        // Входящий видео битрейт
        if (inboundVideo) {
            const currentBytes = inboundVideo.bytesReceived;
            const prev = prevStatsMap.get(remoteId);
            if (prev && prev.bytes !== undefined && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (now - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    currentBitrateIn = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsHistory.bitrateIn.push(currentBitrateIn);
                    if (statsHistory.bitrateIn.length > 50) statsHistory.bitrateIn.shift();
                }
            }
            prevStatsMap.set(remoteId, {bytes: currentBytes, timestamp: now});

            currentJitter = (inboundVideo.jitter * 1000);
            statsHistory.jitter.push(currentJitter);
            if (statsHistory.jitter.length > 50) statsHistory.jitter.shift();

            statsText += `  📥 Входящий битрейт видео: ${currentBitrateIn.toFixed(2)} kbps\n`;
            statsText += `  📥 Потеря пакетов (вх): ${inboundVideo.packetsLost}\n`;
            statsText += `  📥 Джиттер: ${currentJitter.toFixed(2)} мс\n`;
            hasData = true;
        } else {
            statsText += `  📥 Нет входящего видео-потока\n`;
        }

        // Исходящий битрейт к этому участнику
        if (outVideo) {
            const currentBytesOut = outVideo.bytesSent;
            const prevOut = prevOutStatsMap.get(remoteId);
            if (prevOut && prevOut.bytes !== undefined && prevOut.timestamp) {
                const bytesDiff = currentBytesOut - prevOut.bytes;
                const timeDiffSec = (now - prevOut.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    const outBitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsText += `  📤 Исходящий битрейт видео: ${outBitrate.toFixed(2)} kbps\n`;
                }
            }
            prevOutStatsMap.set(remoteId, {bytes: currentBytesOut, timestamp: now});
            statsText += `  📤 Потеря пакетов (исх): ${outVideo.packetsLost}\n`;
        } else {
            statsText += `  📤 Нет исходящего видео-потока\n`;
        }

        // ICE RTT
        if (candidatePair) {
            currentRttIce = candidatePair.currentRoundTripTime * 1000;
            statsHistory.rttIce.push(currentRttIce);
            if (statsHistory.rttIce.length > 50) statsHistory.rttIce.shift();
            statsText += `  🔄 RTT (ICE): ${currentRttIce.toFixed(2)} мс\n`;
        }

        // Расхождение аудио/видео
        if (inboundVideo && inboundAudio && inboundVideo.mediaTime !== undefined && inboundAudio.mediaTime !== undefined) {
            const videoTime = inboundVideo.mediaTime;
            const audioTime = inboundAudio.mediaTime;
            const diff = Math.abs(videoTime - audioTime) * 1000;
            if (diff < 500) {
                currentLipSync = diff;
                statsHistory.lipSync.push(currentLipSync);
                if (statsHistory.lipSync.length > 50) statsHistory.lipSync.shift();
                statsText += `  🎞️ Расхождение аудио/видео: ${currentLipSync.toFixed(2)} мс\n`;
            }
        }
        statsText += '\n';
    }

    if (!hasData && peers.size === 0) {
        statsText += 'Нет активных удалённых соединений\n';
    }

    // --- Средние значения за конференцию ---
    const avgBitrateIn = statsHistory.bitrateIn.length ? (statsHistory.bitrateIn.reduce((a, b) => a + b, 0) / statsHistory.bitrateIn.length).toFixed(2) : '—';
    const avgBitrateOut = statsHistory.bitrateOut.length ? (statsHistory.bitrateOut.reduce((a, b) => a + b, 0) / statsHistory.bitrateOut.length).toFixed(2) : '—';
    const avgJitter = statsHistory.jitter.length ? (statsHistory.jitter.reduce((a, b) => a + b, 0) / statsHistory.jitter.length).toFixed(2) : '—';
    const avgRttIce = statsHistory.rttIce.length ? (statsHistory.rttIce.reduce((a, b) => a + b, 0) / statsHistory.rttIce.length).toFixed(2) : '—';
    const avgRttData = statsHistory.rttDataChannel.length ? (statsHistory.rttDataChannel.reduce((a, b) => a + b, 0) / statsHistory.rttDataChannel.length).toFixed(2) : '—';
    const avgLipSync = statsHistory.lipSync.length ? (statsHistory.lipSync.reduce((a, b) => a + b, 0) / statsHistory.lipSync.length).toFixed(2) : '—';
    const avgRttWebsocket = statsHistory.rttWebsocket.length
        ? (statsHistory.rttWebsocket.reduce((a, b) => a + b, 0) / statsHistory.rttWebsocket.length).toFixed(2)
        : '—';
    let conferenceDuration = '—';
    if (conferenceStartTime) {
        const durationSec = (Date.now() - conferenceStartTime) / 1000;
        const minutes = Math.floor(durationSec / 60);
        const seconds = Math.floor(durationSec % 60);
        conferenceDuration = `${minutes}м ${seconds}с`;
    }

    statsText += `\n--- СРЕДНИЕ ЗА ВРЕМЯ КОНФЕРЕНЦИИ (${conferenceDuration}) ---\n`;
    statsText += `Средний входящий битрейт: ${avgBitrateIn} kbps\n`;
    statsText += `Средний исходящий битрейт: ${avgBitrateOut} kbps\n`;
    statsText += `Средний джиттер: ${avgJitter} мс\n`;
    statsText += `Средний RTT ICE: ${avgRttIce} мс\n`;
    statsText += `Средний RTT WebSocket: ${avgRttWebsocket} мс\n`;
    statsText += `Средний RTT DataChannel: ${avgRttData} мс\n`;
    statsText += `Среднее расхождение аудио/видео: ${avgLipSync} мс\n`;

    webrtcStatsDiv.innerHTML = `<pre>${statsText}</pre>`;
}

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