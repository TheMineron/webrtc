const signalingUrl = 'wss://130.193.46.12:8000/ws';
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

let signalingSocket = null;
let sfuSocket = null;
let sfuPeerConnection = null;
let localStream = null;
let roomId = '';
let participantId = '';
let nickname = '';
let renegotiationInProgress = false;
const remoteVideoElements = new Map(); // key = stream.id, value = video element
let renegotiateNeeded = false;
let renegotiateRunning = false;

// --- E2E тест переменные ---
let e2eTestInProgress = false;
let e2eStartTime = null;
let e2eRemoteMonitor = false;
let e2eMonitorInterval = null;
let originalVideoTrack = null;
let canvasStream = null;
let lastE2eResult = null;

// --- Метрики и статистика ---
let conferenceStartTime = null;
let callStartTime = null;
let firstVideoFrameTime = null;
let prevOutboundStats = {
    video: {bytes: 0, timestamp: 0, packets: 0},
    audio: {bytes: 0, timestamp: 0, packets: 0}
};
let prevInboundStats = {bytes: 0, timestamp: 0};
let statsHistory = {
    bitrateOutVideo: [],
    bitrateOutAudio: [],
    bitrateInVideo: [],
    jitter: [],
    rttIce: [],
    lipSync: [],
    rttWebsocket: [],
    e2eDelay: []
};

let statsInterval = null;
let websocketPingInterval = null;
let e2eTestInterval = null;
let lastWebsocketRtt = null;

// --- Элементы DOM ---
const videosContainer = document.getElementById('videos');
const statusDiv = document.getElementById('status');
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const roomInput = document.getElementById('roomInput');
const nameInput = document.getElementById('nameInput');
const startLatencyTestBtn = document.getElementById('startLatencyTest');
const latencyResultDiv = document.getElementById('latencyResult');
const localStatsContent = document.getElementById('localStatsContent');
const remoteStatsContent = document.getElementById('remoteStatsContent');

function updateStatus(text) {
    statusDiv.textContent = text;
    console.log(text);
}

function addVideoElement(stream, label, isLocal = false) {
    const video = document.createElement('video');
    video.srcObject = stream;
    video.autoplay = true;
    video.playsInline = true;
    video.muted = isLocal;
    const container = document.createElement('div');
    container.className = 'video-container';
    container.id = `video-${stream.id}`;
    const labelDiv = document.createElement('div');
    labelDiv.className = 'label';
    labelDiv.textContent = label;
    container.appendChild(video);
    container.appendChild(labelDiv);
    videosContainer.appendChild(container);
    return video;
}

// --- E2E тест: вспышка цветом ---
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

    const senders = sfuPeerConnection.getSenders();
    const videoSender = senders.find(s => s.track?.kind === 'video');
    if (videoSender) await videoSender.replaceTrack(colorTrack);

    setTimeout(async () => {
        localStream.removeTrack(colorTrack);
        localStream.addTrack(originalVideoTrack);
        if (videoSender) await videoSender.replaceTrack(originalVideoTrack);
        colorTrack.stop();
        canvasStream = null;
    }, duration);
}

// --- Отправка сигнала через signaling ---
function sendSignal(targetId, data) {
    if (signalingSocket && signalingSocket.readyState === WebSocket.OPEN) {
        signalingSocket.send(JSON.stringify({
            type: 'signal',
            target_id: targetId,
            data: data
        }));
        console.log(`[E2E] Сигнал отправлен ${targetId}:`, data.type);
    } else {
        console.warn('[E2E] Signaling socket не готов');
    }
}

// --- Запуск E2E теста (отправитель) ---
function startE2eVideoLatencyTest() {
    if (e2eTestInProgress) {
        console.log('[E2E] Тест уже идёт, пропускаем');
        return;
    }
    if (!window.remoteParticipantIds || window.remoteParticipantIds.length === 0) {
        console.log('[E2E] Нет удалённых участников');
        return;
    }
    const targetId = window.remoteParticipantIds[0];
    console.log(`[E2E] Запуск теста для участника ${targetId}`);
    e2eTestInProgress = true;

    sendSignal(targetId, {type: 'e2e_test_start'});

    setTimeout(async () => {
        if (!localStream) {
            console.warn('[E2E] Нет локального потока');
            e2eTestInProgress = false;
            return;
        }
        e2eStartTime = Date.now();
        const colors = ['red', 'lime', 'blue', 'yellow', 'magenta'];
        const color = colors[Math.floor(Math.random() * colors.length)];
        console.log(`[E2E] Отправляем цвет ${color} в ${e2eStartTime}`);
        await flashColorOnStream(color, 300);
        sendSignal(targetId, {
            type: 'e2e_color_change',
            timestamp: e2eStartTime,
            color: color
        });

        setTimeout(() => {
            if (e2eTestInProgress) {
                console.warn('[E2E] Таймаут теста (10 секунд)');
                latencyResultDiv.innerHTML += '<p>⚠️ Тест E2E не завершился (таймаут)</p>';
                e2eTestInProgress = false;
                if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
            }
        }, 10000);
    }, 500);
}

async function processRenegotiation() {
    if (renegotiateRunning) return;
    if (!renegotiateNeeded) return;
    if (!sfuPeerConnection) return;
    if (sfuPeerConnection.signalingState !== 'stable') return;

    renegotiateRunning = true;
    renegotiateNeeded = false;

    try {
        console.log('[SFU] Starting renegotiation');

        const offer = await sfuPeerConnection.createOffer();
        await sfuPeerConnection.setLocalDescription(offer);

        sfuSocket.send(JSON.stringify({
            type: 'offer',
            sdp: offer.sdp
        }));

    } catch (err) {
        console.error('[SFU] Renegotiation failed:', err);
    } finally {
        renegotiateRunning = false;

        // 🔥 если пока мы делали offer пришёл новый renegotiate
        if (renegotiateNeeded) {
            setTimeout(processRenegotiation, 0);
        }
    }
}

// --- Приёмник E2E теста (использует первый попавшийся удалённый видеоэлемент) ---
function startE2eReceiver(senderId) {
    if (e2eRemoteMonitor) {
        console.log('[E2E] Уже в режиме мониторинга');
        return;
    }
    console.log(`[E2E] Запуск приёмника для отправителя ${senderId}`);

    // Берём первый доступный удалённый видеоэлемент
    if (remoteVideoElements.size === 0) {
        console.warn('[E2E] Нет удалённых видеоэлементов');
        return;
    }
    const videoElement = Array.from(remoteVideoElements.values())[0];
    console.log('[E2E] Используем видеоэлемент:', videoElement);

    e2eRemoteMonitor = true;
    let lastColor = null;
    e2eStartTime = null; // будет установлен из e2e_color_change

    if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
    e2eMonitorInterval = setInterval(() => {
        if (!e2eRemoteMonitor) {
            clearInterval(e2eMonitorInterval);
            return;
        }
        if (!e2eStartTime) return; // ещё не получили цвет

        const video = videoElement;
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
            const delay = detectTime - e2eStartTime;
            console.log(`[E2E] Обнаружен цвет ${dominant}, задержка = ${delay} мс`);
            sendSignal(senderId, {type: 'e2e_result', delay: delay});
            lastE2eResult = delay;
            statsHistory.e2eDelay.push(delay);
            if (statsHistory.e2eDelay.length > 10) statsHistory.e2eDelay.shift();
            latencyResultDiv.innerHTML += `<p>🎬 End-to-end задержка видео: ${delay} мс</p>`;
            e2eRemoteMonitor = false;
            clearInterval(e2eMonitorInterval);
        }
    }, 50);
}

// --- WebSocket ping ---
function sendPing() {
    if (signalingSocket && signalingSocket.readyState === WebSocket.OPEN) {
        const timestamp = Date.now();
        signalingSocket.send(JSON.stringify({
            type: 'ping',
            timestamp: timestamp
        }));
        if (!latencyResultDiv.innerHTML.includes('Измерение')) {
            latencyResultDiv.innerHTML = '<p>⏱️ Измерение RTT...</p>';
        }
    }
}

// --- Сбор статистики (с выводом E2E) ---
async function collectFullStats() {
    if (!sfuPeerConnection) {
        localStatsContent.innerText = '— нет соединения —';
        remoteStatsContent.innerText = '— нет соединения —';
        return;
    }

    if (sfuPeerConnection.iceConnectionState !== 'connected' &&
        sfuPeerConnection.iceConnectionState !== 'completed') {
        localStatsContent.innerText = `Ожидание соединения (ICE: ${sfuPeerConnection.iceConnectionState})`;
        remoteStatsContent.innerText = `Ожидание соединения (ICE: ${sfuPeerConnection.iceConnectionState})`;
        return;
    }

    try {
        const stats = await sfuPeerConnection.getStats();
        const now = Date.now();

        let outVideo = null, outAudio = null;
        stats.forEach(report => {
            if (report.type === 'outbound-rtp') {
                if (report.kind === 'video') outVideo = report;
                if (report.kind === 'audio') outAudio = report;
            }
        });

        let localText = '';
        if (outVideo) {
            const currentBytes = outVideo.bytesSent;
            const currentPackets = outVideo.packetsSent;
            const prevVideo = prevOutboundStats.video;
            let videoBitrate = 0;
            if (prevVideo.bytes && prevVideo.timestamp) {
                const bytesDiff = currentBytes - prevVideo.bytes;
                const timeDiffSec = (now - prevVideo.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    videoBitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsHistory.bitrateOutVideo.push(videoBitrate);
                    if (statsHistory.bitrateOutVideo.length > 50) statsHistory.bitrateOutVideo.shift();
                }
            }
            prevOutboundStats.video = {bytes: currentBytes, timestamp: now, packets: currentPackets};

            const fps = outVideo.framesPerSecond || '—';
            const width = outVideo.frameWidth || '—';
            const height = outVideo.frameHeight || '—';
            const codec = outVideo.codecId ? outVideo.codecId.split('/').pop() : '—';

            localText += `🎥 Видео (исх.):\n`;
            localText += `   Битрейт: ${videoBitrate.toFixed(2)} kbps\n`;
            localText += `   Разрешение: ${width}×${height}\n`;
            localText += `   FPS: ${fps}\n`;
            localText += `   Отправлено пакетов: ${currentPackets}\n`;
            localText += `   Кодек: ${codec}\n`;
        } else {
            localText += `🎥 Видео: нет активного outbound-трека\n`;
        }

        if (outAudio) {
            const currentBytes = outAudio.bytesSent;
            const prevAudio = prevOutboundStats.audio;
            let audioBitrate = 0;
            if (prevAudio.bytes && prevAudio.timestamp) {
                const bytesDiff = currentBytes - prevAudio.bytes;
                const timeDiffSec = (now - prevAudio.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    audioBitrate = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsHistory.bitrateOutAudio.push(audioBitrate);
                    if (statsHistory.bitrateOutAudio.length > 50) statsHistory.bitrateOutAudio.shift();
                }
            }
            prevOutboundStats.audio = {bytes: currentBytes, timestamp: now, packets: outAudio.packetsSent};
            localText += `🎙️ Аудио (исх.):\n`;
            localText += `   Битрейт: ${audioBitrate.toFixed(2)} kbps\n`;
            localText += `   Отправлено пакетов: ${outAudio.packetsSent}\n`;
        } else {
            localText += `🎙️ Аудио: нет активного outbound-трека\n`;
        }

        if (localStream) {
            const videoTrack = localStream.getVideoTracks()[0];
            if (videoTrack) {
                const settings = videoTrack.getSettings();
                localText += `📷 Камера: ${settings.width || '?'}×${settings.height || '?'} @ ${settings.frameRate || '?'} fps\n`;
            }
        }
        localStatsContent.innerText = localText || '— нет данных —';

        let inboundVideo = null, inboundAudio = null, candidatePair = null, remoteInbound = null;
        stats.forEach(report => {
            if (report.type === 'inbound-rtp' && report.kind === 'video') inboundVideo = report;
            if (report.type === 'inbound-rtp' && report.kind === 'audio') inboundAudio = report;
            if (report.type === 'candidate-pair' && report.nominated === true) candidatePair = report;
            if (report.type === 'remote-inbound-rtp' && report.kind === 'video') remoteInbound = report;
        });

        let remoteText = '';
        if (inboundVideo) {
            const currentBytes = inboundVideo.bytesReceived;
            const prev = prevInboundStats;
            let bitrateIn = 0;
            if (prev.bytes && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (now - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    bitrateIn = (bytesDiff * 8) / timeDiffSec / 1000;
                    statsHistory.bitrateInVideo.push(bitrateIn);
                    if (statsHistory.bitrateInVideo.length > 50) statsHistory.bitrateInVideo.shift();
                }
            }
            prevInboundStats = {bytes: currentBytes, timestamp: now};

            const jitterMs = (inboundVideo.jitter || 0) * 1000;
            statsHistory.jitter.push(jitterMs);
            if (statsHistory.jitter.length > 50) statsHistory.jitter.shift();

            remoteText += `📥 Видео (вх.):\n`;
            remoteText += `   Битрейт: ${bitrateIn.toFixed(2)} kbps\n`;
            remoteText += `   Потеря пакетов: ${inboundVideo.packetsLost || 0}\n`;
            remoteText += `   Джиттер: ${jitterMs.toFixed(2)} мс\n`;
            remoteText += `   Декодировано кадров: ${inboundVideo.framesDecoded || 0}\n`;
        } else {
            remoteText += `📥 Нет входящего видео\n`;
        }

        let rttMs = null;
        if (candidatePair && candidatePair.currentRoundTripTime) {
            rttMs = candidatePair.currentRoundTripTime * 1000;
        } else if (remoteInbound && remoteInbound.roundTripTime) {
            rttMs = remoteInbound.roundTripTime * 1000;
        }
        if (rttMs !== null) {
            statsHistory.rttIce.push(rttMs);
            if (statsHistory.rttIce.length > 50) statsHistory.rttIce.shift();
            remoteText += `📡 RTT (ICE): ${rttMs.toFixed(2)} мс\n`;
        } else {
            remoteText += `📡 RTT (ICE): неизвестно\n`;
        }

        if (inboundVideo && inboundAudio && inboundVideo.mediaTime !== undefined && inboundAudio.mediaTime !== undefined) {
            const diff = Math.abs(inboundVideo.mediaTime - inboundAudio.mediaTime) * 1000;
            if (diff < 500) {
                statsHistory.lipSync.push(diff);
                if (statsHistory.lipSync.length > 50) statsHistory.lipSync.shift();
                remoteText += `🎞️ Расхождение A/V: ${diff.toFixed(2)} мс\n`;
            }
        }

        if (lastWebsocketRtt !== null) {
            remoteText += `🕒 RTT (WebSocket): ${lastWebsocketRtt} мс\n`;
        }

        if (callStartTime && firstVideoFrameTime) {
            const setupTime = (firstVideoFrameTime - callStartTime).toFixed(2);
            remoteText += `📞 Время установления соединения: ${setupTime} мс\n`;
        }

        if (lastE2eResult !== null) {
            remoteText += `🎬 End-to-end задержка видео: ${lastE2eResult} мс (последний тест)\n`;
        }

        let durationText = '—';
        if (conferenceStartTime) {
            const sec = (Date.now() - conferenceStartTime) / 1000;
            const minutes = Math.floor(sec / 60);
            const seconds = Math.floor(sec % 60);
            durationText = `${minutes}м ${seconds}с`;
        }

        const avgOutVideo = statsHistory.bitrateOutVideo.length ?
            (statsHistory.bitrateOutVideo.reduce((a, b) => a + b, 0) / statsHistory.bitrateOutVideo.length).toFixed(2) : '—';
        const avgOutAudio = statsHistory.bitrateOutAudio.length ?
            (statsHistory.bitrateOutAudio.reduce((a, b) => a + b, 0) / statsHistory.bitrateOutAudio.length).toFixed(2) : '—';
        const avgInVideo = statsHistory.bitrateInVideo.length ?
            (statsHistory.bitrateInVideo.reduce((a, b) => a + b, 0) / statsHistory.bitrateInVideo.length).toFixed(2) : '—';
        const avgJitter = statsHistory.jitter.length ?
            (statsHistory.jitter.reduce((a, b) => a + b, 0) / statsHistory.jitter.length).toFixed(2) : '—';
        const avgRttIce = statsHistory.rttIce.length ?
            (statsHistory.rttIce.reduce((a, b) => a + b, 0) / statsHistory.rttIce.length).toFixed(2) : '—';
        const avgLipSync = statsHistory.lipSync.length ?
            (statsHistory.lipSync.reduce((a, b) => a + b, 0) / statsHistory.lipSync.length).toFixed(2) : '—';
        const avgRttWs = statsHistory.rttWebsocket.length ?
            (statsHistory.rttWebsocket.reduce((a, b) => a + b, 0) / statsHistory.rttWebsocket.length).toFixed(2) : '—';
        const avgE2e = statsHistory.e2eDelay.length ?
            (statsHistory.e2eDelay.reduce((a, b) => a + b, 0) / statsHistory.e2eDelay.length).toFixed(2) : '—';

        remoteText += `\n📈 СРЕДНИЕ ЗА КОНФЕРЕНЦИЮ (${durationText}):\n`;
        remoteText += `   Исх. битрейт видео: ${avgOutVideo} kbps\n`;
        remoteText += `   Исх. битрейт аудио: ${avgOutAudio} kbps\n`;
        remoteText += `   Вх. битрейт видео: ${avgInVideo} kbps\n`;
        remoteText += `   Джиттер: ${avgJitter} мс\n`;
        remoteText += `   RTT ICE: ${avgRttIce} мс\n`;
        remoteText += `   RTT WebSocket: ${avgRttWs} мс\n`;
        remoteText += `   Расхождение A/V: ${avgLipSync} мс\n`;
        remoteText += `   E2E задержка видео: ${avgE2e} мс\n`;

        remoteStatsContent.innerText = remoteText;

    } catch (err) {
        console.error('Ошибка сбора статистики:', err);
        localStatsContent.innerText = 'Ошибка получения статистики';
        remoteStatsContent.innerText = 'Ошибка получения статистики';
    }
}

// --- Обработка сигнальных сообщений ---
async function joinRoom() {
    roomId = roomInput.value.trim();
    nickname = nameInput.value.trim();
    if (!roomId || !nickname) {
        alert('Room and name are required');
        return;
    }

    callStartTime = performance.now();
    firstVideoFrameTime = null;

    joinBtn.disabled = true;
    updateStatus('Connecting to signaling server...');

    signalingSocket = new WebSocket(signalingUrl);
    signalingSocket.onopen = () => {
        updateStatus('Signaling connected, joining room...');
        signalingSocket.send(JSON.stringify({
            type: 'join',
            room: roomId,
            nickname: nickname
        }));
    };

    signalingSocket.onmessage = async (event) => {
        const msg = JSON.parse(event.data);
        console.log('Signaling message:', msg.type, msg);

        switch (msg.type) {
            case 'joined':
                participantId = msg.participant_id;
                const sfuUrl = msg.sfu_url;
                updateStatus(`Joined room ${roomId}, connecting to SFU...`);
                conferenceStartTime = Date.now();
                await connectToSFU(sfuUrl);
                break;

            case 'existing_participants':
                window.remoteParticipantIds = msg.participants.map(p => p.id);
                console.log('[E2E] Список удалённых участников:', window.remoteParticipantIds);
                break;

            case 'participant_joined':
                updateStatus(`Participant ${msg.participant.name} joined`);
                if (window.remoteParticipantIds) {
                    window.remoteParticipantIds.push(msg.participant.id);
                } else {
                    window.remoteParticipantIds = [msg.participant.id];
                }
                break;

            case 'participant_left':
                updateStatus(`Participant ${msg.participant_id} left`);
                if (window.remoteParticipantIds) {
                    const idx = window.remoteParticipantIds.indexOf(msg.participant_id);
                    if (idx !== -1) window.remoteParticipantIds.splice(idx, 1);
                }
                break;

            case 'signal':
                console.log('[E2E] Получен signal от', msg.from_id, msg.data);
                if (msg.data.type === 'e2e_test_start') {
                    startE2eReceiver(msg.from_id);
                } else if (msg.data.type === 'e2e_color_change') {
                    if (e2eRemoteMonitor) {
                        e2eStartTime = msg.data.timestamp;
                        console.log(`[E2E] Ожидаем цвет ${msg.data.color}, отправлено в ${e2eStartTime}`);
                    } else {
                        console.log('[E2E] Игнорируем e2e_color_change, мониторинг не активен');
                    }
                } else if (msg.data.type === 'e2e_result') {
                    if (e2eTestInProgress) {
                        const delay = msg.data.delay;
                        console.log(`[E2E] Получен результат задержки: ${delay} мс`);
                        lastE2eResult = delay;
                        statsHistory.e2eDelay.push(delay);
                        if (statsHistory.e2eDelay.length > 10) statsHistory.e2eDelay.shift();
                        latencyResultDiv.innerHTML += `<p>🎬 End-to-end задержка видео: ${delay} мс</p>`;
                        e2eTestInProgress = false;
                        if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
                        e2eRemoteMonitor = false;
                    }
                }
                break;

            case 'pong':
                const rtt = Date.now() - msg.timestamp;
                lastWebsocketRtt = rtt;
                statsHistory.rttWebsocket.push(rtt);
                if (statsHistory.rttWebsocket.length > 50) statsHistory.rttWebsocket.shift();
                latencyResultDiv.innerHTML = `<p>✅ RTT через WebSocket: ${rtt} мс (авто)</p>`;
                console.log(`[PONG] RTT = ${rtt} мс`);
                break;

            case 'error':
                console.error('Signaling error:', msg.message);
                updateStatus(`Error: ${msg.message}`);
                break;

            default:
                console.warn('Unknown signaling message type:', msg.type);
        }
    };

    signalingSocket.onerror = (err) => {
        console.error('Signaling WebSocket error:', err);
        updateStatus('Signaling connection error');
    };

    signalingSocket.onclose = () => {
        updateStatus('Signaling connection closed');
        joinBtn.disabled = false;
        leaveBtn.disabled = true;
        if (statsInterval) clearInterval(statsInterval);
        if (websocketPingInterval) clearInterval(websocketPingInterval);
        if (e2eTestInterval) clearInterval(e2eTestInterval);
        localStatsContent.innerText = '— соединение потеряно —';
        remoteStatsContent.innerText = '— соединение потеряно —';
    };
}

async function connectToSFU(sfuUrl) {
    try {
        sfuSocket = new WebSocket(sfuUrl);
        sfuSocket.onopen = async () => {
            updateStatus('SFU connected, setting up WebRTC...');
            sfuSocket.send(JSON.stringify({
                type: 'join',
                room: roomId,
                participant_id: participantId
            }));

            try {
                await setupSFUPeerConnection();
                await startLocalStream();
                await createAndSendOffer();

                if (statsInterval) clearInterval(statsInterval);
                statsInterval = setInterval(() => collectFullStats(), 5000);

                if (websocketPingInterval) clearInterval(websocketPingInterval);
                websocketPingInterval = setInterval(() => sendPing(), 3000);

                if (e2eTestInterval) clearInterval(e2eTestInterval);
                e2eTestInterval = setInterval(() => {
                    if (window.remoteParticipantIds && window.remoteParticipantIds.length > 0 && !e2eTestInProgress && conferenceStartTime) {
                        startE2eVideoLatencyTest();
                    }
                }, 30000);
            } catch (err) {
                console.error('Setup error:', err);
                updateStatus(`Setup failed: ${err.message}`);
            }
        };

        sfuSocket.onmessage = async (event) => {
            const msg = JSON.parse(event.data);
            console.log('SFU message received:', msg.type, msg);

            if (msg.type === 'joined') {
                updateStatus('Joined SFU room');
            } else if (msg.type === 'renegotiate') {
                renegotiateNeeded = true;
                await processRenegotiation();
            } else if (msg.type === 'answer') {
                try {
                    const answer = new RTCSessionDescription({
                        type: 'answer',
                        sdp: msg.sdp
                    });
                    await sfuPeerConnection.setRemoteDescription(answer);
                    processRenegotiation();
                    updateStatus('WebRTC connection established');
                } catch (err) {
                    console.error('Failed to set remote description:', err);
                } finally {
                    renegotiationInProgress = false;
                }
            } else if (msg.type === 'ice-candidate') {
                const candidate = new RTCIceCandidate(msg.candidate);
                await sfuPeerConnection.addIceCandidate(candidate);
            } else if (msg.type === 'error') {
                console.error('SFU error:', msg.message);
                updateStatus(`SFU error: ${msg.message}`);
            }
        };

        sfuSocket.onerror = (err) => {
            console.error('SFU WebSocket error:', err);
            updateStatus('SFU connection error');
        };

        sfuSocket.onclose = () => {
            updateStatus('SFU connection closed');
            cleanup();
        };
    } catch (err) {
        console.error('Failed to connect to SFU:', err);
        updateStatus('Failed to connect to SFU');
    }
}

async function setupSFUPeerConnection() {
    sfuPeerConnection = new RTCPeerConnection(pcConfig);

    const transceivers = sfuPeerConnection.getTransceivers();
    for (const transceiver of transceivers) {
        if (transceiver.receiver && transceiver.receiver.track && transceiver.receiver.track.kind === 'video') {
            const codecs = RTCRtpReceiver.getCapabilities('video').codecs;
            const vp8 = codecs.find(c => c.mimeType === 'video/VP8');
            if (vp8) {
                await transceiver.setCodecPreferences([vp8]);
            }
        }
    }

    sfuPeerConnection.onicecandidate = (event) => {
        if (event.candidate && sfuSocket && sfuSocket.readyState === WebSocket.OPEN) {
            sfuSocket.send(JSON.stringify({
                type: 'ice-candidate',
                candidate: event.candidate
            }));
        }
    };

    sfuPeerConnection.onsignalingstatechange = () => {
        console.log('[SFU] signalingState:', sfuPeerConnection.signalingState);

        if (sfuPeerConnection.signalingState === 'stable') {
            processRenegotiation();
        }
    };

    sfuPeerConnection.ontrack = (event) => {
        console.log('Remote track:', event.track.kind);
        let stream;
        if (event.streams && event.streams[0]) {
            stream = event.streams[0];
        } else {
            stream = new MediaStream([event.track]);
        }
        if (remoteVideoElements.has(stream.id)) return;
        const video = document.createElement('video');
        video.srcObject = stream;
        video.autoplay = true;
        video.playsInline = true;
        const container = document.createElement('div');
        container.className = 'video-container';
        container.appendChild(video);
        videosContainer.appendChild(container);
        remoteVideoElements.set(stream.id, video);
    };

    sfuPeerConnection.onconnectionstatechange = () => {
        updateStatus(`SFU connection state: ${sfuPeerConnection.connectionState}`);
        if (sfuPeerConnection.connectionState === 'disconnected' || sfuPeerConnection.connectionState === 'failed') {
            remoteStatsContent.innerText = 'Соединение с SFU потеряно';
        }
    };

    sfuPeerConnection.oniceconnectionstatechange = () => {
        console.log('ICE connection state:', sfuPeerConnection.iceConnectionState);
    };
}

async function startLocalStream() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({
            video: true,
            audio: true
        });
        addVideoElement(localStream, `${nickname} (You)`, true);
        if (!sfuPeerConnection) throw new Error('PeerConnection not initialized');
        localStream.getTracks().forEach(track => {
            sfuPeerConnection.addTrack(track, localStream);
        });
        updateStatus('Local stream started');
    } catch (err) {
        console.error('Failed to get local media:', err);
        updateStatus('Failed to access camera/microphone');
        throw err;
    }
}

async function createAndSendOffer() {
    if (!sfuPeerConnection) throw new Error('PeerConnection not initialized');
    const offer = await sfuPeerConnection.createOffer();
    await sfuPeerConnection.setLocalDescription(offer);
    sfuSocket.send(JSON.stringify({
        type: 'offer',
        sdp: sfuPeerConnection.localDescription.sdp
    }));
    updateStatus('Offer sent to SFU');
    leaveBtn.disabled = false;
}

function cleanup() {
    if (statsInterval) clearInterval(statsInterval);
    if (websocketPingInterval) clearInterval(websocketPingInterval);
    if (e2eTestInterval) clearInterval(e2eTestInterval);
    if (e2eMonitorInterval) clearInterval(e2eMonitorInterval);
    if (sfuPeerConnection) {
        sfuPeerConnection.close();
        sfuPeerConnection = null;
    }
    if (localStream) {
        localStream.getTracks().forEach(track => track.stop());
        localStream = null;
    }
    if (sfuSocket) {
        sfuSocket.close();
        sfuSocket = null;
    }
    videosContainer.innerHTML = '';
    joinBtn.disabled = false;
    leaveBtn.disabled = true;
    renegotiationInProgress = false;
    remoteVideoElements.clear();
    prevOutboundStats = {video: {bytes: 0, timestamp: 0, packets: 0}, audio: {bytes: 0, timestamp: 0, packets: 0}};
    prevInboundStats = {bytes: 0, timestamp: 0};
    statsHistory = {
        bitrateOutVideo: [],
        bitrateOutAudio: [],
        bitrateInVideo: [],
        jitter: [],
        rttIce: [],
        lipSync: [],
        rttWebsocket: [],
        e2eDelay: []
    };
    conferenceStartTime = null;
    callStartTime = null;
    firstVideoFrameTime = null;
    lastWebsocketRtt = null;
    e2eTestInProgress = false;
    e2eRemoteMonitor = false;
    e2eStartTime = null;
    lastE2eResult = null;
    localStatsContent.innerText = '— соединение закрыто —';
    remoteStatsContent.innerText = '— соединение закрыто —';
    latencyResultDiv.innerHTML = '';
}

function leaveRoom() {
    if (signalingSocket) {
        signalingSocket.send(JSON.stringify({type: 'leave'}));
        signalingSocket.close();
        signalingSocket = null;
    }
    cleanup();
    updateStatus('Disconnected');
}

startLatencyTestBtn.onclick = () => {
    sendPing();
    latencyResultDiv.innerHTML = '<p>⏱️ Измерение RTT (ручное)...</p>';
};
joinBtn.onclick = joinRoom;
leaveBtn.onclick = leaveRoom;