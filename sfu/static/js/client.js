// Конфигурация
const signalingUrl = 'wss://81.26.178.64:8000/ws'; // ваш signalling сервер
const pcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        {
            urls: [
                'turn:81.26.180.114:3478?transport=udp',
                'turn:81.26.180.114:3478?transport=tcp'
            ],
            username: 'webrtc',
            credential: 'webrtc_password'
        }
    ],
    iceCandidatePoolSize: 10
};

// Глобальные переменные
let signalingSocket = null;
let sfuSocket = null;
let sfuPeerConnection = null;
let localStream = null;
let roomId = '';
let participantId = '';
let nickname = '';
let renegotiationInProgress = false; // больше не используется, оставлен для совместимости

// DOM элементы
const videosContainer = document.getElementById('videos');
const statusDiv = document.getElementById('status');
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const roomInput = document.getElementById('roomInput');
const nameInput = document.getElementById('nameInput');
const localStatsContent = document.getElementById('localStatsContent');
const remoteStatsContent = document.getElementById('remoteStatsContent');
const latencyResultDiv = document.getElementById('latencyResult');

// Вспомогательные функции
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

// Подключение к signalling серверу и вход в комнату
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
        console.log('Signaling message:', msg.type, msg);

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

            case 'signal':
                // Обработка сигналов от других участников (E2E тесты)
                if (msg.data.type === 'e2e_test_start') {
                    // можно реализовать тест при необходимости
                }
                break;

            case 'error':
                console.error('Signaling error:', msg.message);
                updateStatus(`Error: ${msg.message}`);
                break;

            default:
                console.warn('Unknown signaling message:', msg.type);
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
        cleanup();
    };
}

// Подключение к SFU и настройка WebRTC
async function connectToSFU(sfuUrl) {
    try {
        sfuSocket = new WebSocket(sfuUrl);
        sfuSocket.onopen = async () => {
            updateStatus('SFU connected, setting up WebRTC...');
            // Отправляем join на SFU
            sfuSocket.send(JSON.stringify({
                type: 'join',
                room: roomId,
                participant_id: participantId
            }));

            await setupSFUPeerConnection();
            await startLocalStream();
            await createAndSendInitialOffer();

            leaveBtn.disabled = false;
        };

        sfuSocket.onmessage = async (event) => {
            const msg = JSON.parse(event.data);
            console.log('SFU message:', msg.type, msg);

            if (msg.type === 'joined') {
                updateStatus('Joined SFU room');
            } else if (msg.type === 'offer') {
                // Сервер отправил offer (пересогласование)
                await handleSFUOffer(msg.sdp);
            } else if (msg.type === 'answer') {
                // Ответ на наш offer
                await handleSFUAnswer(msg.sdp);
            } else if (msg.type === 'ice-candidate') {
                try {
                    const candidate = new RTCIceCandidate(msg.candidate);
                    await sfuPeerConnection.addIceCandidate(candidate);
                } catch (err) {
                    console.warn('Failed to add ICE candidate:', err);
                }
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

    sfuPeerConnection.onicecandidate = (event) => {
        if (event.candidate && sfuSocket && sfuSocket.readyState === WebSocket.OPEN) {
            sfuSocket.send(JSON.stringify({
                type: 'ice-candidate',
                candidate: event.candidate
            }));
        }
    };

    sfuPeerConnection.ontrack = (event) => {
        console.log('[SFU] ontrack:', event.track.kind, 'track.id=', event.track.id);
        const stream = event.streams[0];
        if (stream) {
            // Если поток уже отображается, добавляем трек
            for (let container of videosContainer.children) {
                const video = container.querySelector('video');
                if (video.srcObject === stream) {
                    return; // уже есть
                }
            }
            addVideoElement(stream, `Remote (${stream.id.slice(0,8)})`, false);
        } else {
            // Для обратной совместимости, если stream не передан
            const newStream = new MediaStream([event.track]);
            addVideoElement(newStream, `Remote track`, false);
        }
    };

    sfuPeerConnection.onconnectionstatechange = () => {
        updateStatus(`SFU connection state: ${sfuPeerConnection.connectionState}`);
    };

    sfuPeerConnection.oniceconnectionstatechange = () => {
        console.log('ICE connection state:', sfuPeerConnection.iceConnectionState);
    };

    sfuPeerConnection.onsignalingstatechange = () => {
        console.log('[SFU] signalingState:', sfuPeerConnection.signalingState);
    };
}

async function startLocalStream() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
        addVideoElement(localStream, `${nickname} (You)`, true);

        // Добавляем треки в PeerConnection
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

async function createAndSendInitialOffer() {
    const offer = await sfuPeerConnection.createOffer();
    await sfuPeerConnection.setLocalDescription(offer);
    sfuSocket.send(JSON.stringify({
        type: 'offer',
        sdp: sfuPeerConnection.localDescription.sdp
    }));
    updateStatus('Initial offer sent to SFU');
}

async function handleSFUOffer(sdp) {
    console.log('[SFU] Received offer from SFU');
    try {
        await sfuPeerConnection.setRemoteDescription(new RTCSessionDescription({
            type: 'offer',
            sdp: sdp
        }));
        const answer = await sfuPeerConnection.createAnswer();
        await sfuPeerConnection.setLocalDescription(answer);
        sfuSocket.send(JSON.stringify({
            type: 'answer',
            sdp: answer.sdp
        }));
        console.log('[SFU] Sent answer in response to SFU offer');
    } catch (err) {
        console.error('Error handling SFU offer:', err);
    }
}

async function handleSFUAnswer(sdp) {
    console.log('[SFU] Received answer from SFU');
    try {
        await sfuPeerConnection.setRemoteDescription(new RTCSessionDescription({
            type: 'answer',
            sdp: sdp
        }));
    } catch (err) {
        console.error('Error handling SFU answer:', err);
    }
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
    localStatsContent.innerText = '— соединение закрыто —';
    remoteStatsContent.innerText = '— соединение закрыто —';
    latencyResultDiv.innerHTML = '';
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

// Привязка событий
joinBtn.onclick = joinRoom;
leaveBtn.onclick = leaveRoom;