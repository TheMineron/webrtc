const signalingUrl = 'wss://130.193.35.201:8000/ws';
const pcConfig = {
    iceServers: [
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

const videosContainer = document.getElementById('videos');
const statusDiv = document.getElementById('status');
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const roomInput = document.getElementById('roomInput');
const nameInput = document.getElementById('nameInput');

joinBtn.onclick = joinRoom;
leaveBtn.onclick = leaveRoom;

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

        if (msg.type === 'joined') {
            participantId = msg.participant_id;
            const sfuUrl = msg.sfu_url;
            updateStatus(`Joined room ${roomId}, connecting to SFU...`);
            await connectToSFU(sfuUrl);
        } else if (msg.type === 'participant_joined') {
            updateStatus(`Participant ${msg.participant.name} joined`);
        } else if (msg.type === 'participant_left') {
            updateStatus(`Participant ${msg.participant_id} left`);
        } else if (msg.type === 'error') {
            console.error('Signaling error:', msg.message);
            updateStatus(`Error: ${msg.message}`);
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
    const transceivers = sfuPeerConnection.getTransceivers();
    for (const transceiver of transceivers) {
        if (transceiver.receiver.track.kind === 'video') {
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
        console.log('ontrack:', event.track.kind, event.streams[0]?.id);
        const stream = event.streams[0];
        if (!stream) {
            console.warn('No stream associated with track');
            return;
        }

        // Если для этого потока уже есть видеоэлемент – ничего не создаём
        if (remoteVideoElements.has(stream.id)) {
            console.log(`Stream ${stream.id} already exists, track added automatically`);
            return;
        }

        // Создаём новый видеоэлемент и контейнер
        const video = document.createElement('video');
        video.srcObject = stream;
        video.autoplay = true;
        video.playsInline = true;

        const container = document.createElement('div');
        container.className = 'video-container';
        container.id = `video-${stream.id}`;

        const labelDiv = document.createElement('div');
        labelDiv.className = 'label';
        // Из stream.id можно извлечь participant_id (сервер задаёт "remote-<participant_id>")
        const participantId = stream.id.replace('remote-', '');
        labelDiv.textContent = `Remote (${participantId.slice(0, 8)})`;

        container.appendChild(video);
        container.appendChild(labelDiv);
        videosContainer.appendChild(container);

        remoteVideoElements.set(stream.id, video);
    };

    sfuPeerConnection.onconnectionstatechange = () => {
        updateStatus(`SFU connection state: ${sfuPeerConnection.connectionState}`);
    };

    sfuPeerConnection.onicecandidate = (event) => {
        if (event.candidate && event.candidate.candidate && event.candidate.candidate.trim() !== "") {
            sfuSocket.send(JSON.stringify({
                type: 'ice-candidate',
                candidate: event.candidate
            }));
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