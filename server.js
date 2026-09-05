/**
 * SmartJER Chess Multiplayer - Pelayan WebSocket Relay
 * ======================================================
 * TUJUAN: Urus presence pemain online, cabaran, kod jemputan, relay
 * gerakan, DAN (baharu) bilik perlawanan turnamen automatik dengan
 * sokongan sambung semula + timeout server-authoritative (elak
 * penipuan tuntutan menang).
 */
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const SMARTJER_WEBHOOK_URL = process.env.SMARTJER_WEBHOOK_URL || '';
const SMARTJER_WEBHOOK_SECRET = process.env.SMARTJER_WEBHOOK_SECRET || '';

const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('SmartJER Chess Multiplayer Relay - OK\n');
});
const wss = new WebSocket.Server({ server });

const players = new Map();          // playerId -> { ws, name, rating, countryCode, status }
const games = new Map();            // gameId -> { player1Id, player2Id, player1Color, timeControlSeconds, moves, tournamentId, matchId, activeColor, lastTickTimestamp, player1RemainingMs, player2RemainingMs }
const invites = new Map();          // code -> { hostId, timeControlSeconds }
const tournamentWaiting = new Map(); // "tournamentId_matchId" -> { playerId, name, rating }
const playerGameMap = new Map();    // playerId -> gameId (untuk cari game semasa reconnect)

function broadcastOnlineList() {
    const list = [...players.entries()]
        .filter(([id, p]) => p.status !== 'in_game')
        .map(([id, p]) => ({ playerId: id, name: p.name, rating: p.rating, countryCode: p.countryCode }));
    const msg = JSON.stringify({ type: 'online_list', players: list });
    players.forEach(p => { if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg); });
}

function sendTo(playerId, data) {
    const p = players.get(playerId);
    if (p && p.ws && p.ws.readyState === WebSocket.OPEN) p.ws.send(JSON.stringify(data));
}

function otherPlayer(game, playerId) {
    return game.player1Id === playerId ? game.player2Id : game.player1Id;
}

function colorOf(game, playerId) {
    return game.player1Id === playerId ? game.player1Color : (game.player1Color === 'w' ? 'b' : 'w');
}

async function reportGameResult(gameId, winnerId, loserId, isDraw, moveLog, tournamentId, matchId) {
    if (!SMARTJER_WEBHOOK_URL) return;
    try {
        await fetch(SMARTJER_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-webhook-secret': SMARTJER_WEBHOOK_SECRET },
            body: JSON.stringify({
                game_id: gameId, winner_player_id: winnerId, loser_player_id: loserId, is_draw: isDraw,
                move_log: moveLog || [], tournament_id: tournamentId || null, match_id: matchId || null,
            }),
        });
    } catch (e) {
        console.error('Gagal hantar webhook hasil permainan:', e.message);
    }
}

function endGame(gameId, winnerId, loserId, isDraw) {
    const game = games.get(gameId);
    if (!game) return;
    reportGameResult(gameId, winnerId, loserId, isDraw, game.moves.map(m => m.san).filter(Boolean), game.tournamentId, game.matchId);
    [game.player1Id, game.player2Id].forEach(pid => {
        if (players.has(pid)) players.get(pid).status = 'online';
        playerGameMap.delete(pid);
    });
    games.delete(gameId);
    broadcastOnlineList();
}

function startGame(p1, p2, timeControlSeconds, tournamentId, matchId) {
    const gameId = 'g_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    const p1IsWhite = Math.random() < 0.5;
    const nowMs = Date.now();
    games.set(gameId, {
        player1Id: p1, player2Id: p2, player1Color: p1IsWhite ? 'w' : 'b', timeControlSeconds,
        moves: [], tournamentId: tournamentId || null, matchId: matchId || null,
        activeColor: 'w', lastTickTimestamp: nowMs,
        player1RemainingMs: timeControlSeconds * 1000, player2RemainingMs: timeControlSeconds * 1000,
    });
    playerGameMap.set(p1, gameId);
    playerGameMap.set(p2, gameId);
    players.get(p1).status = 'in_game';
    players.get(p2).status = 'in_game';
    sendTo(p1, { type: 'game_start', gameId, opponent: { name: players.get(p2).name, rating: players.get(p2).rating }, yourColor: p1IsWhite ? 'w' : 'b', timeControlSeconds, tournamentId: tournamentId || null });
    sendTo(p2, { type: 'game_start', gameId, opponent: { name: players.get(p1).name, rating: players.get(p1).rating }, yourColor: p1IsWhite ? 'b' : 'w', timeControlSeconds, tournamentId: tournamentId || null });
    broadcastOnlineList();
}

wss.on('connection', (ws) => {
    let myPlayerId = null;

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }

        if (msg.type === 'register') {
            myPlayerId = msg.playerId;
            players.set(myPlayerId, { ws, name: msg.name, rating: msg.rating, countryCode: msg.countryCode, status: 'online' });

            // SAMBUNG SEMULA - kalau playerId ni ADA game aktif (disconnect
            // sebelum ni), hantar SEMULA state penuh (moves) supaya client
            // boleh bina semula papan (chess.js client-side replay).
            const existingGameId = playerGameMap.get(myPlayerId);
            if (existingGameId && games.has(existingGameId)) {
                const game = games.get(existingGameId);
                players.get(myPlayerId).status = 'in_game';
                const opponentId = otherPlayer(game, myPlayerId);
                sendTo(myPlayerId, {
                    type: 'game_resume', gameId: existingGameId, moves: game.moves,
                    yourColor: colorOf(game, myPlayerId), timeControlSeconds: game.timeControlSeconds,
                    opponent: { name: players.get(opponentId)?.name || 'Lawan' },
                    myRemainingMs: game.player1Id === myPlayerId ? game.player1RemainingMs : game.player2RemainingMs,
                    oppRemainingMs: game.player1Id === myPlayerId ? game.player2RemainingMs : game.player1RemainingMs,
                    activeColor: game.activeColor, tournamentId: game.tournamentId,
                });
                if (opponentId) sendTo(opponentId, { type: 'opponent_reconnected' });
            } else {
                broadcastOnlineList();
            }
            return;
        }

        if (!myPlayerId || !players.has(myPlayerId)) return;

        if (msg.type === 'challenge') {
            const target = players.get(msg.targetPlayerId);
            if (target && target.status === 'online') {
                sendTo(msg.targetPlayerId, { type: 'challenge_received', fromPlayerId: myPlayerId, fromName: players.get(myPlayerId).name, timeControlSeconds: msg.timeControlSeconds || 600 });
            }
            return;
        }

        if (msg.type === 'challenge_response') {
            if (msg.accepted) {
                startGame(msg.fromPlayerId, myPlayerId, msg.timeControlSeconds || 300, null, null);
            } else {
                sendTo(msg.fromPlayerId, { type: 'challenge_declined', byName: players.get(myPlayerId).name });
            }
            return;
        }

        if (msg.type === 'create_invite') {
            const code = Math.random().toString(36).slice(2, 8).toUpperCase();
            invites.set(code, { hostId: myPlayerId, timeControlSeconds: msg.timeControlSeconds || 600 });
            sendTo(myPlayerId, { type: 'invite_code', code });
            return;
        }

        if (msg.type === 'join_invite') {
            const invite = invites.get(msg.code);
            if (!invite || !players.has(invite.hostId) || invite.hostId === myPlayerId) {
                sendTo(myPlayerId, { type: 'invite_error', message: 'Kod tidak sah atau anda cuba sertai permainan sendiri.' });
                return;
            }
            invites.delete(msg.code);
            startGame(invite.hostId, myPlayerId, invite.timeControlSeconds, null, null);
            return;
        }

        if (msg.type === 'join_tournament_match') {
            // Bilik perlawanan turnamen AUTOMATIK - tiada kod manual. Kedua-dua
            // peserta hantar mesej SAMA (tournamentId+matchId), pelayan padankan.
            const roomKey = `${msg.tournamentId}_${msg.matchId}`;
            const existingGameId = playerGameMap.get(myPlayerId);
            if (existingGameId) { // sudah dalam game (mungkin refresh) - sambung semula sahaja
                return;
            }
            const waiting = tournamentWaiting.get(roomKey);
            if (waiting && waiting.playerId !== myPlayerId) {
                tournamentWaiting.delete(roomKey);
                startGame(waiting.playerId, myPlayerId, msg.timeControlSeconds || 600, msg.tournamentId, msg.matchId);
            } else {
                tournamentWaiting.set(roomKey, { playerId: myPlayerId });
                sendTo(myPlayerId, { type: 'waiting_for_opponent' });
            }
            return;
        }

        if (msg.type === 'move') {
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = otherPlayer(game, myPlayerId);
            game.moves.push({ from: msg.from, to: msg.to, promotion: msg.promotion, san: msg.san || null });
            // Kemas kini jam SERVER-SIDE (sumber kebenaran untuk sahkan tuntutan timeout)
            const now = Date.now();
            if (game.player1Id === myPlayerId) game.player1RemainingMs = msg.remainingMs;
            else game.player2RemainingMs = msg.remainingMs;
            game.activeColor = game.activeColor === 'w' ? 'b' : 'w';
            game.lastTickTimestamp = now;
            sendTo(opponentId, { type: 'opponent_move', from: msg.from, to: msg.to, promotion: msg.promotion, remainingMs: msg.remainingMs });
            return;
        }

        if (msg.type === 'claim_opponent_timeout') {
            // SERVER-AUTHORITATIVE - JANGAN percaya client secara membuta.
            // Kira sendiri sama ada masa SEBENAR berlalu cukup untuk jam
            // lawan capai 0, guna cap masa & baki jam TERKINI yang pelayan
            // simpan sendiri (bukan angka yang claimant hantar).
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = otherPlayer(game, myPlayerId);
            const opponentColor = colorOf(game, opponentId);
            if (game.activeColor !== opponentColor) {
                sendTo(myPlayerId, { type: 'claim_rejected', message: 'Bukan giliran lawan - tuntutan tidak sah.' });
                return;
            }
            const opponentRemainingMs = game.player1Id === opponentId ? game.player1RemainingMs : game.player2RemainingMs;
            const elapsedSinceLastTick = Date.now() - game.lastTickTimestamp;
            if (elapsedSinceLastTick >= opponentRemainingMs) {
                sendTo(myPlayerId, { type: 'opponent_game_over', result: 'win' });
                endGame(msg.gameId, myPlayerId, opponentId, false);
            } else {
                sendTo(myPlayerId, { type: 'claim_rejected', message: 'Lawan masih ada baki masa - tuntutan ditolak.' });
            }
            return;
        }

        if (msg.type === 'game_over') {
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = otherPlayer(game, myPlayerId);
            sendTo(opponentId, { type: 'opponent_game_over', result: msg.result === 'win' ? 'loss' : (msg.result === 'loss' ? 'win' : 'draw') });
            if (msg.result !== 'draw') {
                const winnerId = msg.result === 'win' ? myPlayerId : opponentId;
                const loserId = msg.result === 'win' ? opponentId : myPlayerId;
                endGame(msg.gameId, winnerId, loserId, false);
            } else {
                endGame(msg.gameId, myPlayerId, opponentId, true);
            }
            return;
        }

        if (msg.type === 'timeout') {
            const game = games.get(msg.gameId);
            if (!game) return;
            const opponentId = otherPlayer(game, myPlayerId);
            sendTo(opponentId, { type: 'opponent_game_over', result: 'win' });
            endGame(msg.gameId, opponentId, myPlayerId, false);
            return;
        }
    });

    ws.on('close', () => {
        if (!myPlayerId) return;
        const gameId = playerGameMap.get(myPlayerId);
        if (gameId && games.has(gameId)) {
            // Dalam permainan aktif - JANGAN padam state (sokongan sambung
            // semula). Jam TERUS dikira pelayan - lawan boleh tuntut timeout
            // bila cukup masa berlalu (lihat claim_opponent_timeout).
            const opponentId = otherPlayer(games.get(gameId), myPlayerId);
            sendTo(opponentId, { type: 'opponent_disconnected' });
            players.delete(myPlayerId);
        } else {
            players.delete(myPlayerId);
            broadcastOnlineList();
        }
    });
});

server.listen(PORT, () => console.log('SmartJER Chess Multiplayer Relay berjalan di port ' + PORT));
