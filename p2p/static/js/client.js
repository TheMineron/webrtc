let localStream = null;
let peerConnections = new Map();
let ws = null;
let currentRoom = null;
let myId = null;
let myName = null;
let polite = false;

let incomingOffers = new Set();

const iceServers = {
    iceServers: [
        {urls: "stun:stun.l.google.com:19302"},
        {urls: "stun:stun1.l.google.com:19302"}
    ]
};

const joinScreen = document.getElementById("join-screen");
const videoGrid = document.getElementById("video-grid");
const localVideo = document.getElementById("localVideo");
const remoteVideosDiv = document.getElementById("remoteVideos");
const statsLog = document.getElementById("statsLog");
const joinBtn = document.getElementById("joinBtn");

let callSetupTimes = new Map();
let lastStats = new Map();
let e2eLatencyTestActive = false;

function logStat(msg) {
    const timestamp = new Date().toISOString();
    statsLog.innerText += timestamp + " " + msg + "\n";
    statsLog.scrollTop = statsLog.scrollHeight;
}

async function initLocalStream() {
    try {
        localStream = await navigator.mediaDevices.getUserMedia({video: true, audio: true});
        localVideo.srcObject = localStream;
        logStat("Локальный поток получен");
    } catch (err) {
        console.error("Ошибка доступа к медиа:", err);
        alert("Не удалось получить доступ к камере/микрофону");
    }
}

function createPeerConnection(targetId, targetName) {
    const dataChannel = pc.createDataChannel("metrics");
    const pc = new RTCPeerConnection(iceServers);
    const callStartTime = performance.now();

    const entry = {
        pc, targetName, callStartTime,
        dataChannel: null,
        processedSignals: new Set(),
        pendingCandidates: []
    };
    peerConnections.set(targetId, entry);

    if (localStream) {
        localStream.getTracks().forEach(track => {
            pc.addTrack(track, localStream);
        });
    }

    pc.ondatachannel = (event) => {
        const channel = event.channel;
        channel.onmessage = handleDataChannelMessage;
        channel.onopen = () => logStat(`DataChannel с ${targetName} открыт`);
        const entry = peerConnections.get(targetId);
        if (entry) entry.dataChannel = channel;
    };

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            ws.send(JSON.stringify({
                type: "signal",
                target_id: targetId,
                data: {type: "ice", candidate: event.candidate}
            }));
        }
    };

    pc.ontrack = (event) => {
        const remoteVideoId = `remoteVideo_${targetId}`;
        let remoteVideo = document.getElementById(remoteVideoId);
        if (!remoteVideo) {
            const container = document.createElement("div");
            container.className = "remote-video-container";
            remoteVideo = document.createElement("video");
            remoteVideo.id = remoteVideoId;
            remoteVideo.autoplay = true;
            remoteVideo.playsInline = true;
            const label = document.createElement("div");
            label.className = "label";
            label.innerText = targetName;
            container.appendChild(remoteVideo);
            container.appendChild(label);
            remoteVideosDiv.appendChild(container);

            const setupTime = performance.now() - callStartTime;
            callSetupTimes.set(targetId, setupTime);
            logStat(`[МЕТРИКА] Call Setup Time для ${targetName}: ${setupTime.toFixed(2)} ms`);
            sendMetricsToServer({type: "call_setup_time", targetId, targetName, value: setupTime});
        }
        remoteVideo.srcObject = event.streams[0];
        logStat(`Установлен видеопоток от ${targetName} (${targetId})`);
    };

    pc.onconnectionstatechange = () => {
        logStat(`Соединение с ${targetName}: ${pc.connectionState}`);
    };

    pc.onnegotiationneeded = async () => {
        logStat(`onnegotiationneeded для ${targetName}`);
        try {
            if (polite && pc.signalingState !== "stable") {
                logStat(`Вежливый пир, но состояние не stable, пропускаем.`);
                return;
            }
            await pc.setLocalDescription(await pc.createOffer());
            ws.send(JSON.stringify({
                type: "signal",
                target_id: targetId,
                data: {type: "offer", sdp: pc.localDescription.sdp}
            }));
            logStat(`Отправлен offer для ${targetName}`);
        } catch (err) {
            console.error("Ошибка в onnegotiationneeded:", err);
        }
    };

    if (dataChannel) {
        dataChannel.onopen = () => logStat(`DataChannel с ${targetName} открыт (инициатор)`);
        dataChannel.onmessage = handleDataChannelMessage;
    }

    const originalSetRemoteDesc = pc.setRemoteDescription;
    pc.setRemoteDescription = function (desc) {
        return originalSetRemoteDesc.call(pc, desc).then(() => {
            if (entry.pendingCandidates && entry.pendingCandidates.length) {
                logStat(`Добавляем ${entry.pendingCandidates.length} отложенных ICE для ${targetName}`);
                entry.pendingCandidates.forEach(c => pc.addIceCandidate(new RTCIceCandidate(c)));
                entry.pendingCandidates = [];
            }
        });
    };

    return pc;
}

function handleDataChannelMessage(event) {
    const data = JSON.parse(event.data);
    if (data.type === "video_e2e_request") {
        const senderId = data.sender_id;
        const remoteVideo = document.getElementById(`remoteVideo_${senderId}`);
        if (!remoteVideo || !remoteVideo.videoWidth) {
            logStat("E2E: видеоэлемент не найден или не готов");
            return;
        }
        const flashTimeSender = data.flash_time;
        logStat(`E2E: получен запрос, начинаем детекцию маркера на видео от ${senderId}`);

        let frameCount = 0;
        const checkFrame = () => {
            const canvas = document.createElement("canvas");
            canvas.width = remoteVideo.videoWidth;
            canvas.height = remoteVideo.videoHeight;
            const ctx = canvas.getContext("2d");
            ctx.drawImage(remoteVideo, 0, 0, canvas.width, canvas.height);
            const centerX = Math.floor(canvas.width / 2);
            const centerY = Math.floor(canvas.height / 2);
            const pixel = ctx.getImageData(centerX, centerY, 1, 1).data;
            if (pixel[0] > 200 && pixel[1] < 100 && pixel[2] < 100) {
                const detectionTime = performance.now();
                logStat(`E2E: маркер обнаружен на кадре, задержка от вспышки на отправителе: ${(detectionTime - flashTimeSender).toFixed(2)} ms (без коррекции часов)`);
                if (event.target && event.target.readyState === "open") {
                    event.target.send(JSON.stringify({
                        type: "video_e2e_response",
                        detection_time: detectionTime,
                        sender_flash_time: flashTimeSender
                    }));
                }
                return;
            }
            frameCount++;
            if (frameCount < 300) {
                remoteVideo.requestVideoFrameCallback(checkFrame);
            } else {
                logStat("E2E: маркер не обнаружен за 5 секунд");
            }
        };
        remoteVideo.requestVideoFrameCallback(checkFrame);
    } else if (data.type === "video_e2e_response") {
        const detectionTime = data.detection_time;
        const senderFlashTime = data.sender_flash_time;
        const now = performance.now();
        const rtt = now - senderFlashTime;
        const oneWayLatency = (detectionTime - senderFlashTime) - (rtt / 2);
        logStat(`[МЕТРИКА] E2E задержка видео (оценка): ${oneWayLatency.toFixed(2)} ms (RTC коррекция: RTT=${rtt.toFixed(2)} ms)`);
        const latencyResultDiv = document.getElementById("latencyResult");
        if (latencyResultDiv) latencyResultDiv.innerHTML = `E2E задержка видео: ${oneWayLatency.toFixed(2)} ms`;
        sendMetricsToServer({type: "e2e_video_latency", value: oneWayLatency, rtt: rtt});
    }
}

async function callParticipant(targetId, targetName) {
    if (peerConnections.has(targetId)) return;

    if (incomingOffers.has(targetId)) {
        logStat(`Не отправляем offer для ${targetName}, уже получен входящий offer`);
        return;
    }

    createPeerConnection(targetId, targetName);
    const entry = peerConnections.get(targetId);
    const pcInstance = entry.pc;

    try {
        const offer = await pcInstance.createOffer();
        await pcInstance.setLocalDescription(offer);
        ws.send(JSON.stringify({
            type: "signal",
            target_id: targetId,
            data: {type: "offer", sdp: pcInstance.localDescription.sdp}
        }));
        logStat(`Отправлен offer для ${targetName}`);
    } catch (err) {
        console.error("Ошибка создания offer:", err);
    }
}

async function collectWebRTCStats() {
    for (const [targetId, entry] of peerConnections.entries()) {
        const {pc, targetName} = entry;
        if (!pc) continue;
        try {
            const stats = await pc.getStats();
            let inboundVideo = null;
            let inboundAudio = null;
            stats.forEach(report => {
                if (report.type === 'inbound-rtp' && report.kind === 'video') inboundVideo = report;
                if (report.type === 'inbound-rtp' && report.kind === 'audio') inboundAudio = report;
            });
            if (inboundVideo) {
                const jitter = inboundVideo.jitter ? (inboundVideo.jitter * 1000).toFixed(2) : 'N/A';
                const packetsLost = inboundVideo.packetsLost;
                const framesDecoded = inboundVideo.framesDecoded;
                let lostPerSecond = 0;
                if (lastStats.has(targetId)) {
                    const prev = lastStats.get(targetId);
                    const deltaLost = packetsLost - prev.packetsLost;
                    if (deltaLost >= 0) {
                        const deltaTime = (performance.now() - prev.timestamp) / 1000;
                        if (deltaTime > 0) lostPerSecond = deltaLost / deltaTime;
                    }
                }
                lastStats.set(targetId, {packetsLost, timestamp: performance.now()});

                const statsText = `[${targetName}] Video jitter: ${jitter} ms, packetLoss: ${packetsLost} (${lostPerSecond.toFixed(2)} pkt/s), frames: ${framesDecoded}`;
                logStat(statsText);
                const webrtcStatsDiv = document.getElementById("webrtcStats");
                if (webrtcStatsDiv) webrtcStatsDiv.innerHTML = statsText;
                sendMetricsToServer({
                    type: "webrtc_stats",
                    targetId,
                    jitter_ms: parseFloat(jitter),
                    packetsLost,
                    lostPerSecond
                });
            }
            if (inboundAudio) {
                const audioJitter = inboundAudio.jitter ? (inboundAudio.jitter * 1000).toFixed(2) : 'N/A';
                logStat(`[${targetName}] Audio jitter: ${audioJitter} ms`);
            }
        } catch (err) {
            console.error("Ошибка getStats:", err);
        }
    }
}

setInterval(collectWebRTCStats, 5000);

async function runE2ELatencyTest() {
    if (e2eLatencyTestActive) return;
    e2eLatencyTestActive = true;
    logStat("Запуск теста E2E задержки видео...");

    const entries = Array.from(peerConnections.values());
    if (entries.length === 0) {
        alert("Нет активных соединений");
        e2eLatencyTestActive = false;
        return;
    }
    const targetEntry = entries[0];
    const dataChannel = targetEntry.dataChannel;
    if (!dataChannel || dataChannel.readyState !== "open") {
        alert("DataChannel не готов");
        e2eLatencyTestActive = false;
        return;
    }

    let markerDiv = document.createElement("div");
    markerDiv.style.position = "absolute";
    markerDiv.style.top = "50%";
    markerDiv.style.left = "50%";
    markerDiv.style.width = "100px";
    markerDiv.style.height = "100px";
    markerDiv.style.backgroundColor = "red";
    markerDiv.style.transform = "translate(-50%, -50%)";
    markerDiv.style.zIndex = "1000";
    markerDiv.style.borderRadius = "10px";
    localVideo.parentElement.style.position = "relative";
    localVideo.parentElement.appendChild(markerDiv);
    setTimeout(() => markerDiv.remove(), 200);

    const flashTime = performance.now();
    dataChannel.send(JSON.stringify({
        type: "video_e2e_request",
        flash_time: flashTime,
        sender_id: myId
    }));
    logStat(`E2E: маркер показан в ${flashTime}, запрос отправлен`);

    setTimeout(() => {
        e2eLatencyTestActive = false;
    }, 10000);
}

function sendMetricsToServer(metricData) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
            type: "metrics",
            data: metricData
        }));
    }
}

async function handleSignal(fromId, fromName, data) {
    console.log("Получен сигнал:", {fromId, fromName, data});
    let entry = peerConnections.get(fromId);
    let pc = entry ? entry.pc : null;

    if (!pc) {
        pc = createPeerConnection(fromId, fromName);
        entry = peerConnections.get(fromId);
    }

    if (data.type === "offer") {
        polite = (myId > fromId);

        if (pc.signalingState !== "stable") {
            logStat(`Соединение с ${fromName} не в stable состоянии, игнорируем offer.`);
            return;
        }
        await pc.setRemoteDescription(new RTCSessionDescription(data));
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        ws.send(JSON.stringify({
            type: "signal",
            target_id: fromId,
            data: {type: "answer", sdp: pc.localDescription.sdp}
        }));
        logStat(`Отправлен answer для ${fromName}`);
    } else if (data.type === "answer") {

        if (pc.signalingState === "have-local-offer") {
            await pc.setRemoteDescription(new RTCSessionDescription(data));
        } else if (polite && pc.signalingState === "have-remote-offer") {
            logStat(`Коллизия! Откатываем localDescription для ${fromName}`);
            await pc.setLocalDescription({type: "rollback"});
            await pc.setRemoteDescription(new RTCSessionDescription(data));
        } else {
            logStat(`Answer получен в неожиданном состоянии: ${pc.signalingState}`);
        }
    } else if (data.type === "ice") {
        if (data.candidate) {
            try {
                if (pc.remoteDescription) {
                    await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
                    logStat(`Добавлен ICE-кандидат от ${fromName}`);
                } else {
                    logStat(`ICE кандидат отложен, ждем remoteDescription для ${fromName}`);
                    // Сохраняем кандидата, чтобы добавить позже
                    if (!entry.pendingCandidates) entry.pendingCandidates = [];
                    entry.pendingCandidates.push(data.candidate);
                }
            } catch (err) {
                console.warn("Ошибка добавления ICE-кандидата:", err);
            }
        }
    }
}

async function joinRoom(roomId, nickname) {
    const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    ws = new WebSocket(`${protocol}://${window.location.host}/ws`);
    ws.onopen = () => {
        ws.send(JSON.stringify({type: "join", room: roomId, nickname: nickname}));
    };

    ws.onmessage = async (event) => {
        const msg = JSON.parse(event.data);
        console.log("WebSocket сообщение:", msg);
        switch (msg.type) {
            case "joined":
                myId = msg.participant_id;
                currentRoom = roomId;
                myName = nickname;
                joinScreen.style.display = "none";
                videoGrid.style.display = "flex";
                document.getElementById("stats").style.display = "block";
                logStat(`Присоединились к комнате ${roomId} как ${nickname}`);
                await initLocalStream();
                break;

            case "existing_participants":
                for (let p of msg.participants) {
                    await callParticipant(p.id, p.name);
                }
                break;

            case "participant_joined":
                await callParticipant(msg.participant.id, msg.participant.name);
                break;

            case "signal":
                await handleSignal(msg.from_id, msg.from_name, msg.data);
                break;

            case "participant_left":
                const entry = peerConnections.get(msg.participant_id);
                if (entry) {
                    entry.pc.close();
                    peerConnections.delete(msg.participant_id);
                    callSetupTimes.delete(msg.participant_id);
                    lastStats.delete(msg.participant_id);
                    incomingOffers.delete(msg.participant_id);
                    const vidElem = document.getElementById(`remoteVideo_${msg.participant_id}`);
                    if (vidElem) vidElem.parentElement?.remove();
                    logStat(`Участник ${msg.participant_id} покинул комнату`);
                }
                break;

            case "pong":
                const rtt = Date.now() - msg.timestamp;
                logStat(`WebSocket RTT: ${rtt} ms`);
                sendMetricsToServer({type: "ws_rtt", value: rtt});
                break;
        }
    };

    ws.onerror = (err) => {
        console.error("WebSocket ошибка:", err);
        alert("Ошибка соединения с сервером");
    };
}

setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({type: "ping", timestamp: Date.now()}));
    }
}, 5000);

const latencyBtn = document.getElementById("startLatencyTest");
if (latencyBtn) latencyBtn.onclick = runE2ELatencyTest;

joinBtn.onclick = async () => {
    const nickname = document.getElementById("nickname").value.trim();
    const roomId = document.getElementById("roomId").value.trim();
    if (!nickname || !roomId) {
        alert("Введите имя и ID комнаты");
        return;
    }
    await joinRoom(roomId, nickname);
};