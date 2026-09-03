/**
 * SmartJER Chess Multiplayer - Pelayan WebSocket Relay
 * ======================================================
 * TUJUAN: Urus presence pemain online, cabaran, kod jemputan, dan relay
 * gerakan antara 2 pemain semasa permainan aktif. TIDAK simpan sejarah
 * permainan/rating secara kekal - itu diserahkan kepada SmartJER PHP
 * (webhook dipanggil bila permainan tamat).
 *
 * Ini JAUH lebih ringan dari pelayan video (SFU) - cuma relay mesej JSON
 * kecil (gerakan catur), bukan strim media besar.
 */
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const SMARTJER_WEBHOOK_URL = process.env.SMARTJER_WEBHOOK_URL || ''; // https://smartjer.com/api/chess-multiplayer-result.php
const SMARTJER_WEBHOOK_SECRET = process.env.SMARTJER_WEBHOOK_SECRET || '';

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('SmartJER Chess Multiplayer Relay - OK\n');
});
const wss = new WebSocket.Server({ server });

// State dalam memori (ephemeral - reset bila pelayan restart, TAK kritikal
// sebab ini cuma untuk sesi aktif, bukan rekod kekal)
const players = new Map();   // playerId -> { ws, name, rating, countryCode, status: 'online'|'in_game' }
const games = new Map();     // gameId -> { player1Id, player2Id, player1Color, moves: [] }
const invites = new Map();   // code -> playerId

function broadcastOnlineList() {
    const list = [...players.entries()]
        .filter(([id, p]) => p.status !== 'in_game')
        .map(([id, p]) => ({ playerId: id, name: p.name, rating: p.rating, countryCode: p.countryCode }));
    const msg = JSON.stringify({ type: 'online_list', players: list });
    players.forEach(p => { if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg); });
}

function sendTo(playerId, data) {
    const p = players.get(playerId);
    if (p && p.ws.readyState === WebSocket.OPEN) p.ws.send(JSON.stringify(data));
}

async function reportGameResult(gameId, winnerId, loserId, isDraw) {
    if (!SMARTJER_WEBHOOK_URL) return;
    try {
        await fetch(SMARTJER_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-webhook-secret': SMARTJER_WEBHOOK_SECRET },
            body: JSON.stringify({ game_id: gameId, winner_player_id: winnerId, loser_player_id: loserId, is_draw: isDraw }),
        });
    } catch (e) {
        console.error('Gagal hantar webhook hasil permainan:', e.message);
    }
}

wss.on('connection', (ws) => {
    let myPlayerId = null;

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }

        if (msg.type === 'register') {
            myPlayerId = msg.playerId;
            players.set(myPlayerId, { ws, name: msg.name, rating: msg.rating, countryCode: msg.countryCode, status: 'online' });
            broadcastOnlineList();
            return;
        }

        if (!myPlayerId || !players.has(myPlayerId)) return;

        if (msg.type === 'challenge') {
            const target = players.get(msg.targetPlayerId);
            if (target && target.status === 'online') {
                sendTo(msg.targetPlayerId, { type: 'challenge_received', fromPlayerId: myPlayerId, fromName: players.get(myPlayerId).name });
            }
            return;
        }

        if (msg.type === 'challenge_response') {
            if (msg.accepted) {
                const gameId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
                const p1 = msg.fromPlayerId, p2 = myPlayerId;
                const p1IsWhite = Math.random() < 0.5;
                games.set(gameId, { player1Id: p1, player2Id: p2, player1Color: p1IsWhite ? 'w' : 'b' });
                players.get(p1).status = 'in_game';
                players.get(p2).status = 'in_game';
                sendTo(p1, { type: 'game_start', gameId, opponent: { name: players.get(p2).name, rating: players.get(p2).rating }, yourColor: p1IsWhite ? 'w' : 'b' });
                sendTo(p2, { type: 'game_start', gameId, opponent: { name: players.get(p1).name, rating: players.get(p1).rating }, yourColor: p1IsWhite ? 'b' : 'w' });
                broadcastOnlineList();
            } else {
                sendTo(msg.fromPlayerId, { type: 'challenge_declined', byName: players.get(myPlayerId).name });
            }
            return;
        }

        if (msg.type === 'create_invite') {
            const code = Math.random().toString(36).slice(2, 8).toUpperCase();
            invites.set(code, myPlayerId);
            sendTo(myPlayerId, { type: 'invite_code', code });
            return;
        }

        if (msg.type === 'join_invite') {
            const hostId = invites.get(msg.code);
            if (!hostId || !players.has(hostId) || hostId === myPlayerId) {
                sendTo(myPlayerId, { type: 'invite_error', message: 'Kod tidak sah atau anda cuba sertai permainan sendiri.' });
                return;
            }
            invites.delete(msg.code);
            const gameId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
            const p1 = hostId, p2 = myPlayerId;
            const p1IsWhite = Math.random() < 0.5;
            games.set(gameId, { player1Id: p1, player2Id: p2, player1Color: p1IsWhite ? 'w' : 'b' });
            players.get(p1).status = 'in_game';
            players.get(p2).status = 'in_game';
            sendTo(p1, { type: 'game_start', gameId, opponent: { name: players.get(p2).name, rating: players.get(p2).rating }, yourColor: p1IsWhite ? 'w' : 'b' });
            sendTo(p2, { type: 'game_start', gameId, opponent: { name: players.get(p1).name, rating: players.get(p1).rating }, yourColor: p1IsWhite ? 'b' : 'w' });
            broadcastOnlineList();
            return;
        }

        if (msg.type === 'move') {
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = game.player1Id === myPlayerId ? game.player2Id : game.player1Id;
            sendTo(opponentId, { type: 'opponent_move', from: msg.from, to: msg.to, promotion: msg.promotion });
            return;
        }

        if (msg.type === 'game_over') {
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = game.player1Id === myPlayerId ? game.player2Id : game.player1Id;
            sendTo(opponentId, { type: 'opponent_game_over', result: msg.result === 'win' ? 'loss' : (msg.result === 'loss' ? 'win' : 'draw') });

            if (msg.result !== 'draw') {
                const winnerId = msg.result === 'win' ? myPlayerId : opponentId;
                const loserId = msg.result === 'win' ? opponentId : myPlayerId;
                reportGameResult(msg.gameId, winnerId, loserId, false);
            } else {
                reportGameResult(msg.gameId, myPlayerId, opponentId, true);
            }

            if (players.has(myPlayerId)) players.get(myPlayerId).status = 'online';
            if (players.has(opponentId)) players.get(opponentId).status = 'online';
            games.delete(msg.gameId);
            broadcastOnlineList();
            return;
        }
    });

    ws.on('close', () => {
        if (myPlayerId) {
            players.delete(myPlayerId);
            broadcastOnlineList();
        }
    });
});

server.listen(PORT, () => console.log('SmartJER Chess Multiplayer Relay berjalan di port ' + PORT));
