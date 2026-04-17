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

// --- Метрики и статистика ---
let conferenceStartTime = null;             // момент входа в комнату (для длительности)
let callStartTime = null;                  // момент нажатия Join (для Call Setup Time)
let firstVideoFrameTime = null;             // момент получения первого видео-трека
let prevOutboundStats = {
  video: { bytes: 0, timestamp: 0, packets: 0 },
  audio: { bytes: 0, timestamp: 0, packets: 0 }
};
let prevInboundStats = { bytes: 0, timestamp: 0 };
let statsHistory = {
  bitrateOutVideo: [],
  bitrateOutAudio: [],
  bitrateInVideo: [],
  jitter: [],
  rttIce: [],
  lipSync: [],
  rttWebsocket: []          // для автоматических измерений RTT через WebSocket
};

// --- Таймеры ---
let statsInterval = null;
let websocketPingInterval = null;
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

// --- WebSocket ping для измерения RTT (ручной и автоматический) ---
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

// --- Сбор ВСЕЙ статистики (локальной и удалённой) ---
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

    // ---- 1. Локальная статистика (исходящие потоки) ----
    let outVideo = null, outAudio = null;
    stats.forEach(report => {
      if (report.type === 'outbound-rtp') {
        if (report.kind === 'video') outVideo = report;
        if (report.kind === 'audio') outAudio = report;
      }
    });

    let localText = '';

    // Исходящее видео
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
      prevOutboundStats.video = { bytes: currentBytes, timestamp: now, packets: currentPackets };

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

    // Исходящее аудио
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
      prevOutboundStats.audio = { bytes: currentBytes, timestamp: now, packets: outAudio.packetsSent };
      localText += `🎙️ Аудио (исх.):\n`;
      localText += `   Битрейт: ${audioBitrate.toFixed(2)} kbps\n`;
      localText += `   Отправлено пакетов: ${outAudio.packetsSent}\n`;
    } else {
      localText += `🎙️ Аудио: нет активного outbound-трека\n`;
    }

    // Параметры захвата с камеры
    if (localStream) {
      const videoTrack = localStream.getVideoTracks()[0];
      if (videoTrack) {
        const settings = videoTrack.getSettings();
        localText += `📷 Камера: ${settings.width || '?'}×${settings.height || '?'} @ ${settings.frameRate || '?'} fps\n`;
      }
    }
    localStatsContent.innerText = localText || '— нет данных —';

    // ---- 2. Удалённая статистика (входящие потоки от SFU) ----
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
      prevInboundStats = { bytes: currentBytes, timestamp: now };

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

    // RTT (ICE / remote-inbound)
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

    // Расхождение аудио/видео
    if (inboundVideo && inboundAudio && inboundVideo.mediaTime !== undefined && inboundAudio.mediaTime !== undefined) {
      const diff = Math.abs(inboundVideo.mediaTime - inboundAudio.mediaTime) * 1000;
      if (diff < 500) {
        statsHistory.lipSync.push(diff);
        if (statsHistory.lipSync.length > 50) statsHistory.lipSync.shift();
        remoteText += `🎞️ Расхождение A/V: ${diff.toFixed(2)} мс\n`;
      }
    }

    // RTT WebSocket (последнее автоматическое измерение)
    if (lastWebsocketRtt !== null) {
      remoteText += `🕒 RTT (WebSocket): ${lastWebsocketRtt} мс\n`;
    }

    // Время установления соединения (Call Setup Time)
    if (callStartTime && firstVideoFrameTime) {
      const setupTime = (firstVideoFrameTime - callStartTime).toFixed(2);
      remoteText += `📞 Время установления соединения: ${setupTime} мс\n`;
    }

    // Средние значения за конференцию
    let durationText = '—';
    if (conferenceStartTime) {
      const sec = (Date.now() - conferenceStartTime) / 1000;
      const minutes = Math.floor(sec / 60);
      const seconds = Math.floor(sec % 60);
      durationText = `${minutes}м ${seconds}с`;
    }

    const avgOutVideo = statsHistory.bitrateOutVideo.length ?
        (statsHistory.bitrateOutVideo.reduce((a,b)=>a+b,0)/statsHistory.bitrateOutVideo.length).toFixed(2) : '—';
    const avgOutAudio = statsHistory.bitrateOutAudio.length ?
        (statsHistory.bitrateOutAudio.reduce((a,b)=>a+b,0)/statsHistory.bitrateOutAudio.length).toFixed(2) : '—';
    const avgInVideo = statsHistory.bitrateInVideo.length ?
        (statsHistory.bitrateInVideo.reduce((a,b)=>a+b,0)/statsHistory.bitrateInVideo.length).toFixed(2) : '—';
    const avgJitter = statsHistory.jitter.length ?
        (statsHistory.jitter.reduce((a,b)=>a+b,0)/statsHistory.jitter.length).toFixed(2) : '—';
    const avgRttIce = statsHistory.rttIce.length ?
        (statsHistory.rttIce.reduce((a,b)=>a+b,0)/statsHistory.rttIce.length).toFixed(2) : '—';
    const avgLipSync = statsHistory.lipSync.length ?
        (statsHistory.lipSync.reduce((a,b)=>a+b,0)/statsHistory.lipSync.length).toFixed(2) : '—';
    const avgRttWs = statsHistory.rttWebsocket.length ?
        (statsHistory.rttWebsocket.reduce((a,b)=>a+b,0)/statsHistory.rttWebsocket.length).toFixed(2) : '—';

    remoteText += `\n📈 СРЕДНИЕ ЗА КОНФЕРЕНЦИЮ (${durationText}):\n`;
    remoteText += `   Исх. битрейт видео: ${avgOutVideo} kbps\n`;
    remoteText += `   Исх. битрейт аудио: ${avgOutAudio} kbps\n`;
    remoteText += `   Вх. битрейт видео: ${avgInVideo} kbps\n`;
    remoteText += `   Джиттер: ${avgJitter} мс\n`;
    remoteText += `   RTT ICE: ${avgRttIce} мс\n`;
    remoteText += `   RTT WebSocket: ${avgRttWs} мс\n`;
    remoteText += `   Расхождение A/V: ${avgLipSync} мс\n`;

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

  // Засекаем время начала установления соединения
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
    console.log('Signaling message:', msg);

    switch (msg.type) {
      case 'joined':
        participantId = msg.participant_id;
        const sfuUrl = msg.sfu_url;
        updateStatus(`Joined room ${roomId}, connecting to SFU...`);
        conferenceStartTime = Date.now();
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

        // Запускаем сбор статистики
        if (statsInterval) clearInterval(statsInterval);
        statsInterval = setInterval(() => collectFullStats(), 5000);

        // Запускаем автоматический пинг WebSocket каждые 3 секунды
        if (websocketPingInterval) clearInterval(websocketPingInterval);
        websocketPingInterval = setInterval(() => sendPing(), 3000);
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

  // Предпочтение VP8
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
    console.log('Remote track received:', event.track.kind);
    const stream = event.streams[0];
    if (!stream) return;

    // Фиксируем время получения первого видео-кадра
    if (event.track.kind === 'video' && !firstVideoFrameTime && callStartTime) {
      firstVideoFrameTime = performance.now();
      const setupTime = firstVideoFrameTime - callStartTime;
      console.log(`[METRIC] Call Setup Time = ${setupTime.toFixed(2)} ms`);
      // Выведем в панель при следующем сборе статистики
    }

    if (remoteVideoElements.has(stream.id)) return;

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
  prevOutboundStats = { video: { bytes: 0, timestamp: 0, packets: 0 }, audio: { bytes: 0, timestamp: 0, packets: 0 } };
  prevInboundStats = { bytes: 0, timestamp: 0 };
  statsHistory = { bitrateOutVideo: [], bitrateOutAudio: [], bitrateInVideo: [], jitter: [], rttIce: [], lipSync: [], rttWebsocket: [] };
  conferenceStartTime = null;
  callStartTime = null;
  firstVideoFrameTime = null;
  lastWebsocketRtt = null;
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

startLatencyTestBtn.onclick = () => {
  sendPing();
  // Вручную показываем индикацию, ответ придёт в onmessage
  latencyResultDiv.innerHTML = '<p>⏱️ Измерение RTT (ручное)...</p>';
};
joinBtn.onclick = joinRoom;
leaveBtn.onclick = leaveRoom;