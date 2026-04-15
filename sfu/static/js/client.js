let signalingWs = null;
let sfuWs = null;
let localStream = null;
let currentRoomId = null;
let currentNickname = null;
let sfuPc = null;
let remoteVideos = new Map(); // trackId -> {video, container, label, stream}

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

let prevStatsMap = new Map();
setInterval(() => {
    if (sfuPc && sfuPc.connectionState === 'connected') collectStats();
}, 5000);

async function collectStats() {
    if (!sfuPc) return;
    const stats = await sfuPc.getStats();
    let statsText = '';
    let inboundRtpMap = new Map();
    let candidatePair = null;
    stats.forEach(report => {
        if (report.type === 'inbound-rtp' && report.kind === 'video') {
            inboundRtpMap.set(report.id, report);
        }
        if (report.type === 'candidate-pair' && report.nominated === true) {
            candidatePair = report;
        }
    });
    let totalBitrate = 0;
    const now = Date.now();
    for (let [id, report] of inboundRtpMap) {
        const prev = prevStatsMap.get(id);
        let bitrate = 0;
        if (prev && prev.bytes !== undefined && prev.timestamp) {
            const bytesDiff = report.bytesReceived - prev.bytes;
            const timeDiff = (now - prev.timestamp) / 1000;
            if (timeDiff > 0) bitrate = (bytesDiff * 8) / timeDiff / 1000;
        }
        prevStatsMap.set(id, {bytes: report.bytesReceived, timestamp: now});
        totalBitrate += bitrate;
        statsText += `📹 Bitrate: ${bitrate.toFixed(2)} kbps\n`;
        statsText += `📦 Packets lost: ${report.packetsLost}\n`;
        statsText += `⏱️ Jitter: ${(report.jitter || 0).toFixed(3)} s\n`;
    }
    if (candidatePair && candidatePair.currentRoundTripTime) {
        statsText += `🔄 ICE RTT: ${(candidatePair.currentRoundTripTime * 1000).toFixed(2)} ms\n`;
    }
    if (statsText === '') statsText = 'No active video streams';
    document.getElementById('webrtcStats').innerHTML = `<pre>${statsText}</pre>`;
}

// ---------- Signaling ----------
async function connectSignaling(roomId, nickname) {
    const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;
    signalingWs = new WebSocket(wsUrl);
    signalingWs.onopen = () => {
        signalingWs.send(JSON.stringify({type: 'join', room: roomId, nickname}));
        // Heartbeat: send ping every 30 seconds
        setInterval(() => {
            if (signalingWs.readyState === WebSocket.OPEN) {
                signalingWs.send(JSON.stringify({type: 'ping', timestamp: Date.now()}));
            }
        }, 30000);
    };
    signalingWs.onmessage = async (e) => {
        const msg = JSON.parse(e.data);
        console.log('[Signaling]', msg.type, msg);
        if (msg.type === 'joined') {
            currentRoomId = msg.room;
            currentNickname = msg.nickname;
            document.getElementById('join-screen').style.display = 'none';
            document.getElementById('video-grid').style.display = 'flex';
            await initLocalMedia();
            connectToSFU(msg.sfu_url, msg.room);
        } else if (msg.type === 'pong') {
            const rtt = Date.now() - msg.timestamp;
            document.getElementById('latencyResult').innerHTML = `<pre>⏱️ Signaling RTT: ${rtt} ms</pre>`;
        } else if (msg.type === 'error') {
            alert(msg.message);
        } else if (msg.type === 'participant_joined') {
            console.log('Participant joined:', msg.participant);
            // Можно показать уведомление, но SFU сам управляет потоками
        } else if (msg.type === 'participant_left') {
            console.log('Participant left:', msg.participant_id);
        }
    };
    signalingWs.onerror = (err) => console.error('Signaling error', err);
    signalingWs.onclose = () => console.log('Signaling closed');
}

// ---------- SFU Connection ----------
function connectToSFU(sfuUrl, roomId) {
    sfuWs = new WebSocket(sfuUrl);
    sfuWs.onopen = () => {
        sfuWs.send(JSON.stringify({type: 'join', room: roomId}));
        // Heartbeat for SFU
        setInterval(() => {
            if (sfuWs.readyState === WebSocket.OPEN) {
                sfuWs.send(JSON.stringify({type: 'ping', timestamp: Date.now()}));
            }
        }, 30000);
    };
    sfuWs.onmessage = async (e) => {
        const msg = JSON.parse(e.data);
        console.log('[SFU]', msg.type, msg);
        if (msg.type === 'joined') {
            await createSFUPeerConnection();
        } else if (msg.type === 'answer') {
            await sfuPc.setRemoteDescription(new RTCSessionDescription({type: 'answer', sdp: msg.sdp}));
        } else if (msg.type === 'ice-candidate') {
            await sfuPc.addIceCandidate(new RTCIceCandidate(msg.candidate));
        } else if (msg.type === 'pong') {
            const rtt = Date.now() - msg.timestamp;
            console.log(`SFU RTT: ${rtt} ms`);
        } else if (msg.type === 'error') {
            alert('SFU error: ' + msg.message);
        }
    };
    sfuWs.onerror = (err) => console.error('SFU WebSocket error', err);
    sfuWs.onclose = () => {
        console.log('SFU WebSocket closed');
        if (sfuPc) sfuPc.close();
    };
}

async function createSFUPeerConnection() {
    sfuPc = new RTCPeerConnection(pcConfig);
    // Добавляем локальные треки
    if (localStream) {
        localStream.getTracks().forEach(track => {
            sfuPc.addTrack(track, localStream);
            console.log(`Added local ${track.kind} track to SFU PC`);
        });
    }
    sfuPc.ontrack = (event) => {
        const track = event.track;
        if (track.kind !== 'video') return;
        const stream = event.streams[0];
        console.log(`[SFU] ontrack: ${track.kind}, id=${track.id}, stream id=${stream.id}`);
        if (!remoteVideos.has(track.id)) {
            const container = document.createElement('div');
            container.className = 'video-container';
            const video = document.createElement('video');
            video.autoplay = true;
            video.playsInline = true;
            video.srcObject = stream;
            const label = document.createElement('div');
            label.className = 'label';
            label.textContent = `Peer ${track.id.slice(0, 6)}`;
            container.appendChild(video);
            container.appendChild(label);
            document.getElementById('remoteVideos').appendChild(container);
            remoteVideos.set(track.id, {video, container, label, stream});
        } else {
            // Обновляем, если stream изменился (редко)
            const entry = remoteVideos.get(track.id);
            if (entry.video.srcObject !== stream) {
                entry.video.srcObject = stream;
            }
        }
    };

    sfuPc.onicecandidate = (event) => {
        if (event.candidate) {
            sfuWs.send(JSON.stringify({
                type: 'ice-candidate',
                candidate: {
                    candidate: event.candidate.candidate,  // SDP-строка кандидата
                    sdpMid: event.candidate.sdpMid,
                    sdpMLineIndex: event.candidate.sdpMLineIndex
                }
            }));
        } else {
            // Сигнал о завершении ICE-сбора
            sfuWs.send(JSON.stringify({
                type: 'ice-candidate',
                candidate: null
            }));
        }
    };
    sfuPc.oniceconnectionstatechange = () => {
        console.log(`ICE connection state: ${sfuPc.iceConnectionState}`);
    };
    sfuPc.onconnectionstatechange = () => {
        console.log(`Connection state: ${sfuPc.connectionState}`);
    };
    const offer = await sfuPc.createOffer();
    await sfuPc.setLocalDescription(offer);
    sfuWs.send(JSON.stringify({type: 'offer', sdp: offer.sdp}));
}

// ---------- Local Media ----------
async function initLocalMedia() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({video: true, audio: true});
        document.getElementById('localVideo').srcObject = localStream;
        console.log('Local stream obtained');
    } catch (err) {
        console.error('Media error:', err);
        alert('Cannot access camera/microphone');
    }
}

// UI controls
document.getElementById('joinBtn').onclick = () => {
    const nickname = document.getElementById('nickname').value.trim();
    const roomId = document.getElementById('roomId').value.trim();
    if (!nickname || !roomId) return alert('Fill all fields');
    connectSignaling(roomId, nickname);
};
document.getElementById('startLatencyTestBtn').onclick = () => {
    if (signalingWs && signalingWs.readyState === WebSocket.OPEN) {
        signalingWs.send(JSON.stringify({type: 'ping', timestamp: Date.now()}));
        document.getElementById('latencyResult').innerHTML = '<pre>Measuring...</pre>';
    } else {
        alert('Not connected');
    }
};
let micEnabled = true, camEnabled = true;
document.getElementById('toggleMicBtn').onclick = () => {
    if (localStream) {
        const audioTracks = localStream.getAudioTracks();
        if (audioTracks.length) {
            micEnabled = !micEnabled;
            audioTracks[0].enabled = micEnabled;
            document.getElementById('toggleMicBtn').textContent = micEnabled ? '🎤 Mute Mic' : '🎤 Unmute Mic';
        }
    }
};
document.getElementById('toggleCamBtn').onclick = () => {
    if (localStream) {
        const videoTracks = localStream.getVideoTracks();
        if (videoTracks.length) {
            camEnabled = !camEnabled;
            videoTracks[0].enabled = camEnabled;
            document.getElementById('toggleCamBtn').textContent = camEnabled ? '📷 Mute Camera' : '📷 Unmute Camera';
        }
    }
};
