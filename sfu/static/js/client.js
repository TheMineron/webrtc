const signalingUrl = 'wss://130.193.46.12:8000/ws';
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

let signalingSocket = null;
let sfuSocket = null;
let sfuPeerConnection = null;
let localStream = null;
let roomId = '';
let participantId = '';
let nickname = '';
let renegotiationInProgress = false;
const remoteVideoElements = new Map();

// --- Элементы DOM ---
const videosContainer = document.getElementById('videos');
const statusDiv = document.getElementById('status');
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const roomInput = document.getElementById('roomInput');
const nameInput = document.getElementById('nameInput');
const startLatencyTestBtn = document.getElementById('startLatencyTest');
const latencyResultDiv = document.getElementById('latencyResult');
const webrtcStatsDiv = document.getElementById('webrtcStats');

// --- Переменные для сбора статистики ---
const prevStatsMap = new Map(); // remoteId (тут только SFU) -> { bytes, timestamp }

joinBtn.onclick = joinRoom;
leaveBtn.onclick = leaveRoom;
startLatencyTestBtn.onclick = sendPing;

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

// --- WebSocket ping для измерения RTT ---
function sendPing() {
    if (signalingSocket && signalingSocket.readyState === WebSocket.OPEN) {
        const timestamp = Date.now();
        signalingSocket.send(JSON.stringify({
            type: 'ping',
            timestamp: timestamp
        }));
        latencyResultDiv.innerHTML = '<p>⏱️ Измерение...</p>';
        console.log('[PING] Отправлен ping');
    } else {
        alert('Сигнальный WebSocket не подключён');
    }
}

// --- Периодический сбор статистики WebRTC ---
async function collectStats() {
    if (!sfuPeerConnection) {
        webrtcStatsDiv.innerHTML = '<pre>Нет активного соединения с SFU</pre>';
        return;
    }

    if (sfuPeerConnection.iceConnectionState !== 'connected' &&
        sfuPeerConnection.iceConnectionState !== 'completed') {
        webrtcStatsDiv.innerHTML = `<pre>Состояние ICE: ${sfuPeerConnection.iceConnectionState}</pre>`;
        return;
    }

    try {
        const stats = await sfuPeerConnection.getStats();
        const now = Date.now();
        let inboundRtpStats = null;
        let candidatePairStats = null;
        let remoteInboundStats = null;

        stats.forEach(report => {
            // Входящий видеопоток (от SFU)
            if (report.type === 'inbound-rtp' && report.kind === 'video') {
                inboundRtpStats = report;
            }
            // Активная ICE-пара
            if (report.type === 'candidate-pair' && report.nominated === true) {
                candidatePairStats = report;
            }
            // Статистика удалённого входящего потока (для RTT)
            if (report.type === 'remote-inbound-rtp' && report.kind === 'video') {
                remoteInboundStats = report;
            }
        });

        let statsText = '📊 Статистика WebRTC (SFU):\n';

        if (inboundRtpStats) {
            const currentBytes = inboundRtpStats.bytesReceived || 0;
            const prev = prevStatsMap.get('sfu');
            let bitrateKbps = 0;

            if (prev && prev.bytes !== undefined && prev.timestamp) {
                const bytesDiff = currentBytes - prev.bytes;
                const timeDiffSec = (now - prev.timestamp) / 1000;
                if (timeDiffSec > 0 && bytesDiff >= 0) {
                    bitrateKbps = (bytesDiff * 8) / timeDiffSec / 1000;
                }
            }

            prevStatsMap.set('sfu', { bytes: currentBytes, timestamp: now });

            statsText += `  Битрейт видео: ${bitrateKbps.toFixed(2)} kbps\n`;
            statsText += `  Потеря пакетов: ${inboundRtpStats.packetsLost || 0}\n`;
            statsText += `  Джиттер: ${(inboundRtpStats.jitter || 0).toFixed(4)} с\n`;
            statsText += `  Декодировано кадров: ${inboundRtpStats.framesDecoded || 0}\n`;
        } else {
            statsText += `  Нет данных о входящем видео\n`;
        }

        // RTT из ICE-пары
        if (candidatePairStats && candidatePairStats.currentRoundTripTime) {
            statsText += `  RTT (ICE): ${(candidatePairStats.currentRoundTripTime * 1000).toFixed(2)} мс\n`;
        } else if (remoteInboundStats && remoteInboundStats.roundTripTime) {
            statsText += `  RTT (RTCP): ${(remoteInboundStats.roundTripTime * 1000).toFixed(2)} мс\n`;
        } else {
            statsText += `  RTT: неизвестно\n`;
        }

        webrtcStatsDiv.innerHTML = `<pre>${statsText}</pre>`;
    } catch (err) {
        console.error('Ошибка сбора статистики:', err);
        webrtcStatsDiv.innerHTML = '<pre>Ошибка получения статистики</pre>';
    }
}

// Запускаем сбор статистики каждые 5 секунд
setInterval(() => {
    if (sfuPeerConnection) {
        collectStats();
    }
}, 5000);

// --- Обработка сигнальных сообщений (добавлен ping/pong) ---
async function joinRoom() {
    roomId = roomInput.value.trim();
    nickname = nameInput.value.trim();
    if (!roomId || !nickname) {
        alert('Room and name are required');
        return;
    }

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
        console.log('Signaling message:', msg);

        switch (msg.type) {
            case 'joined':
                participantId = msg.participant_id;
                const sfuUrl = msg.sfu_url;
                updateStatus(`Joined room ${roomId}, connecting to SFU...`);
                await connectToSFU(sfuUrl);
                break;

            case 'participant_joined':
                updateStatus(`Participant ${msg.participant.name} joined`);
                break;

            case 'participant_left':
                updateStatus(`Participant ${msg.participant_id} left`);
                break;

            case 'pong':
                const rtt = Date.now() - msg.timestamp;
                latencyResultDiv.innerHTML = `<p>✅ RTT через WebSocket: ${rtt} мс</p>`;
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
        webrtcStatsDiv.innerHTML = '<pre>Соединение с сигнальным сервером закрыто</pre>';
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
                if (!sfuPeerConnection || renegotiationInProgress) return;
                renegotiationInProgress = true;
                try {
                    const offer = await sfuPeerConnection.createOffer();
                    console.log('Creating renegotiation offer, current signalingState:', sfuPeerConnection.signalingState);
                    await sfuPeerConnection.setLocalDescription(offer);
                    sfuSocket.send(JSON.stringify({
                        type: 'offer',
                        sdp: offer.sdp
                    }));
                    updateStatus('Sent renegotiation offer');
                } catch (err) {
                    console.error('Failed to create renegotiation offer:', err);
                    renegotiationInProgress = false;
                }
            } else if (msg.type === 'answer') {
                console.log('Received answer, resetting renegotiation flag');
                try {
                    const answer = new RTCSessionDescription({
                        type: 'answer',
                        sdp: msg.sdp
                    });
                    await sfuPeerConnection.setRemoteDescription(answer);
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

    // Опционально: предпочтение кодека VP8
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

    sfuPeerConnection.ontrack = (event) => {
        console.log('Remote track received:', event.track.kind, 'streams:', event.streams);
        const stream = event.streams[0];
        if (!stream) {
            console.warn('No stream associated with track');
            return;
        }

        if (remoteVideoElements.has(stream.id)) {
            console.log(`Stream ${stream.id} already exists, track added automatically`);
            return;
        }

        const video = document.createElement('video');
        video.srcObject = stream;
        video.autoplay = true;
        video.playsInline = true;

        const container = document.createElement('div');
        container.className = 'video-container';
        container.id = `video-${stream.id}`;

        const labelDiv = document.createElement('div');
        labelDiv.className = 'label';
        const remoteParticipantId = stream.id.replace('remote-', '');
        labelDiv.textContent = `Remote (${remoteParticipantId.slice(0, 8)})`;

        container.appendChild(video);
        container.appendChild(labelDiv);
        videosContainer.appendChild(container);

        remoteVideoElements.set(stream.id, video);
    };

    sfuPeerConnection.onconnectionstatechange = () => {
        updateStatus(`SFU connection state: ${sfuPeerConnection.connectionState}`);
        // При разрыве очищаем статистику
        if (sfuPeerConnection.connectionState === 'disconnected' || sfuPeerConnection.connectionState === 'failed') {
            webrtcStatsDiv.innerHTML = '<pre>Соединение с SFU потеряно</pre>';
        }
    };

    sfuPeerConnection.oniceconnectionstatechange = () => {
        console.log('ICE connection state:', sfuPeerConnection.iceConnectionState);
    };

    sfuPeerConnection.onicegatheringstatechange = () => {
        console.log('ICE gathering state:', sfuPeerConnection.iceGatheringState);
    };

    sfuPeerConnection.onsignalingstatechange = () => {
        console.log('Signaling state:', sfuPeerConnection.signalingState);
    };
}

async function startLocalStream() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({
            video: true,
            audio: true
        });
        addVideoElement(localStream, `${nickname} (You)`, true);
        if (!sfuPeerConnection) {
            throw new Error('PeerConnection not initialized');
        }
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
    if (!sfuPeerConnection) {
        throw new Error('PeerConnection not initialized');
    }
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
    remoteVideoElements.forEach((video, streamId) => {
        const container = document.getElementById(`video-${streamId}`);
        if (container) container.remove();
    });
    remoteVideoElements.clear();
    prevStatsMap.clear();
    webrtcStatsDiv.innerHTML = '<pre>Соединение закрыто</pre>';
}

function leaveRoom() {
    if (signalingSocket) {
        signalingSocket.send(JSON.stringify({ type: 'leave' }));
        signalingSocket.close();
        signalingSocket = null;
    }
    cleanup();
    updateStatus('Disconnected');
}