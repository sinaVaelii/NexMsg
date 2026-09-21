// NexMsg — Frontend Application
(() => {
    'use strict';

    // ===== State =====
    let ws = null;
    let username = '';
    let currentRoom = '';
    let joinedRooms = new Set();
    let typingTimeout = null;
    let typingIndicatorTimeout = null;

    // ===== DOM =====
    const $ = (sel) => document.querySelector(sel);
    const loginModal = $('#login-modal');
    const loginForm = $('#login-form');
    const usernameInput = $('#username-input');
    const app = $('#app');
    const roomList = $('#room-list');
    const newRoomBtn = $('#new-room-btn');
    const newRoomPanel = $('#new-room-panel');
    const newRoomInput = $('#new-room-input');
    const createRoomBtn = $('#create-room-btn');
    const emptyState = $('#empty-state');
    const chatView = $('#chat-view');
    const chatRoomName = $('#chat-room-name');
    const chatRoomMembers = $('#chat-room-members');
    const messagesContainer = $('#messages-container');
    const messages = $('#messages');
    const messageForm = $('#message-form');
    const messageInput = $('#message-input');
    const typingIndicator = $('#typing-indicator');
    const typingUser = $('#typing-user');
    const userAvatar = $('#user-avatar');
    const userDisplay = $('#user-display');
    const connectionStatus = $('#connection-status');

    // ===== Color Palette for Avatars =====
    const avatarColors = [
        '#e57373', '#f06292', '#ba68c8', '#9575cd',
        '#7986cb', '#64b5f6', '#4fc3f7', '#4dd0e1',
        '#4db6ac', '#81c784', '#aed581', '#dce775',
        '#ffd54f', '#ffb74d', '#ff8a65', '#a1887f',
    ];

    function getColor(name) {
        let hash = 0;
        for (let i = 0; i < name.length; i++) {
            hash = name.charCodeAt(i) + ((hash << 5) - hash);
        }
        return avatarColors[Math.abs(hash) % avatarColors.length];
    }

    function getInitials(name) {
        return name.charAt(0).toUpperCase();
    }

    // ===== Room Icons =====
    const roomIcons = {
        'general': '🌐',
        'random': '🎲',
        'tech': '💻',
    };

    function getRoomIcon(name) {
        return roomIcons[name] || '💬';
    }

    // ===== Login =====
    loginForm.addEventListener('submit', (e) => {
        e.preventDefault();
        username = usernameInput.value.trim();
        if (!username) return;

        loginModal.classList.add('hidden');
        app.classList.remove('hidden');

        userAvatar.textContent = getInitials(username);
        userAvatar.style.background = getColor(username);
        userDisplay.textContent = username;

        connectWebSocket();
        loadRooms();
    });

    // ===== WebSocket =====
    function connectWebSocket() {
        const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(`${protocol}//${location.host}/ws?username=${encodeURIComponent(username)}`);

        ws.onopen = () => {
            updateConnectionStatus(true);
            console.log('[ws] connected');
        };

        ws.onmessage = (evt) => {
            try {
                const data = JSON.parse(evt.data);
                handleServerEvent(data);
            } catch (err) {
                console.error('[ws] parse error:', err);
            }
        };

        ws.onclose = () => {
            updateConnectionStatus(false);
            console.log('[ws] disconnected, reconnecting in 3s...');
            setTimeout(connectWebSocket, 3000);
        };

        ws.onerror = (err) => {
            console.error('[ws] error:', err);
        };
    }

    function wsSend(obj) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify(obj));
        }
    }

    function updateConnectionStatus(connected) {
        const dot = connectionStatus.querySelector('.status-dot');
        const text = connectionStatus.querySelector('.status-text');
        if (connected) {
            dot.classList.remove('disconnected');
            text.textContent = 'Connected';
        } else {
            dot.classList.add('disconnected');
            text.textContent = 'Reconnecting...';
        }
    }

    // ===== Handle Server Events =====
    function handleServerEvent(data) {
        switch (data.type) {
            case 'message':
                addMessage(data);
                break;
            case 'user_joined':
                addSystemMessage(data.room, `${data.username} joined`);
                break;
            case 'user_left':
                addSystemMessage(data.room, `${data.username} left`);
                break;
            case 'typing':
                if (data.username !== username && data.room === currentRoom) {
                    showTyping(data.username);
                }
                break;
        }
    }

    // ===== Rooms =====
    async function loadRooms() {
        try {
            const res = await fetch('/api/rooms');
            const data = await res.json();
            renderRooms(data.rooms || []);
        } catch (err) {
            console.error('[rooms] load error:', err);
        }
    }

    function renderRooms(rooms) {
        roomList.innerHTML = '';
        rooms.forEach(room => {
            const el = document.createElement('div');
            el.className = 'room-item' + (room === currentRoom ? ' active' : '');
            el.dataset.room = room;

            const color = getColor(room);
            el.innerHTML = `
                <div class="room-icon" style="background: ${color}">${getRoomIcon(room)}</div>
                <div class="room-info">
                    <div class="room-name"># ${room}</div>
                    <div class="room-preview">Click to join</div>
                </div>
            `;

            el.addEventListener('click', () => selectRoom(room));
            roomList.appendChild(el);
        });
    }

    function selectRoom(room) {
        if (currentRoom === room) return;

        // leave old room
        if (currentRoom && joinedRooms.has(currentRoom)) {
            wsSend({ type: 'leave', room: currentRoom });
            joinedRooms.delete(currentRoom);
        }

        currentRoom = room;

        // update sidebar active state
        document.querySelectorAll('.room-item').forEach(el => {
            el.classList.toggle('active', el.dataset.room === room);
        });

        // show chat
        emptyState.classList.add('hidden');
        chatView.classList.remove('hidden');
        chatRoomName.textContent = `# ${room}`;
        messages.innerHTML = '';

        // join
        wsSend({ type: 'join', room });
        joinedRooms.add(room);

        // load history
        loadMessages(room);

        messageInput.focus();
    }

    async function loadMessages(room) {
        try {
            const res = await fetch(`/api/rooms/${encodeURIComponent(room)}/messages?limit=100`);
            const data = await res.json();
            if (data.messages && currentRoom === room) {
                messages.innerHTML = '';
                data.messages.forEach(msg => addMessage(msg, false));
                scrollToBottom();
            }
        } catch (err) {
            console.error('[messages] load error:', err);
        }
    }

    // ===== New Room =====
    newRoomBtn.addEventListener('click', () => {
        newRoomPanel.classList.toggle('hidden');
        if (!newRoomPanel.classList.contains('hidden')) {
            newRoomInput.focus();
        }
    });

    createRoomBtn.addEventListener('click', createRoom);
    newRoomInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') createRoom();
    });

    async function createRoom() {
        const name = newRoomInput.value.trim().toLowerCase().replace(/\s+/g, '-');
        if (!name) return;

        try {
            await fetch('/api/rooms', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name }),
            });
            newRoomInput.value = '';
            newRoomPanel.classList.add('hidden');
            await loadRooms();
            selectRoom(name);
        } catch (err) {
            console.error('[room] create error:', err);
        }
    }

    // ===== Messages =====
    function addMessage(data, scroll = true) {
        if (data.room && data.room !== currentRoom) return;

        const isOwn = data.username === username;
        const el = document.createElement('div');
        el.className = `message ${isOwn ? 'own' : 'other'}`;

        const time = data.timestamp
            ? new Date(typeof data.timestamp === 'number' ? data.timestamp : data.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            : '';

        el.innerHTML = `
            <div class="message-bubble">
                ${!isOwn ? `<div class="message-sender" style="color: ${getColor(data.username)}">${escapeHtml(data.username)}</div>` : ''}
                <div class="message-text">${escapeHtml(data.content)}</div>
                <div class="message-time">${time}</div>
            </div>
        `;

        messages.appendChild(el);
        if (scroll) scrollToBottom();
    }

    function addSystemMessage(room, text) {
        if (room !== currentRoom) return;

        const el = document.createElement('div');
        el.className = 'system-message';
        el.innerHTML = `<span>${escapeHtml(text)}</span>`;
        messages.appendChild(el);
        scrollToBottom();
    }

    // ===== Send Message =====
    messageForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const content = messageInput.value.trim();
        if (!content || !currentRoom) return;

        wsSend({ type: 'message', room: currentRoom, content });
        messageInput.value = '';
    });

    // ===== Typing =====
    messageInput.addEventListener('input', () => {
        if (!currentRoom) return;

        if (!typingTimeout) {
            wsSend({ type: 'typing', room: currentRoom });
        }
        clearTimeout(typingTimeout);
        typingTimeout = setTimeout(() => {
            typingTimeout = null;
        }, 2000);
    });

    function showTyping(user) {
        typingUser.textContent = `${user} is typing...`;
        typingIndicator.classList.remove('hidden');

        clearTimeout(typingIndicatorTimeout);
        typingIndicatorTimeout = setTimeout(() => {
            typingIndicator.classList.add('hidden');
        }, 3000);
    }

    // ===== Helpers =====
    function scrollToBottom() {
        requestAnimationFrame(() => {
            messagesContainer.scrollTop = messagesContainer.scrollHeight;
        });
    }

    function escapeHtml(str) {
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }
})();
