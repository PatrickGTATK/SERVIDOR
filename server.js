/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║           TIKTOK LIVE MACRO — Servidor                               ║
 * ║  Conecta ao TikTok e dispara teclas + webhooks nos seus jogos        ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */

let WebcastPushConnection, SignConfig, signatureProvider;
try {
  const tiktokConnector = require('tiktok-live-connector');
  WebcastPushConnection = tiktokConnector.WebcastPushConnection;
  SignConfig            = tiktokConnector.SignConfig || null;
  signatureProvider     = tiktokConnector.signatureProvider || null;
} catch (e) {
  console.error('[INIT] Erro ao carregar tiktok-live-connector:', e.message);
}
const WebSocket                 = require('ws');
const http                      = require('http');
const https                     = require('https');
const fs                        = require('fs');
const path                      = require('path');
const os                        = require('os');
const { exec }                  = require('child_process');

// ── CONFIG (API KEY + PREFS) ──────────────────────────────────────
// USER_DATA: pasta gravável onde ficam config, mapeamentos e sons.
// - Se APP_USER_DATA estiver definido pelo main.js do Electron, usa ele.
// - Caso contrário usa __dirname (funciona com asar:false no electron-builder).
const USER_DATA = process.env.APP_USER_DATA || __dirname;
console.log('[INIT] USER_DATA =', USER_DATA);
const CONFIG_FILE = path.join(USER_DATA, 'config.json');

function loadConfig() {
  try {
    if (fs.existsSync(CONFIG_FILE)) {
      return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
    }
  } catch (e) {
    console.warn('[CONFIG] Erro ao carregar config.json:', e.message);
  }
  return {};
}

function saveConfig(data) {
  try {
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(data, null, 2));
    return true;
  } catch (e) {
    console.warn('[CONFIG] Erro ao salvar config.json:', e.message);
    return false;
  }
}

function setApiKey(key) {
  try {
    // SignConfig é o método correto na maioria das versões da biblioteca
    if (SignConfig) {
      SignConfig.apiKey = key;
    } else if (signatureProvider) {
      if (signatureProvider.config && typeof signatureProvider.config === 'object') {
        signatureProvider.config.apiKey = key;
      } else {
        signatureProvider.apiKey = key;
      }
    }
  } catch (e) {
    console.warn('[CONFIG] ⚠️  Não foi possível aplicar apiKey:', e.message);
  }
}

function applyConfig(cfg) {
  if (cfg.apiKey) {
    setApiKey(cfg.apiKey);
    console.log('[CONFIG] ✅ Eulerstream API Key carregada.');
  } else {
    console.warn('[CONFIG] ⚠️  API Key não configurada. Vá em Configurações e insira sua chave Eulerstream.');
  }
}

let appConfig = loadConfig();
applyConfig(appConfig);

// ── PORTAS ────────────────────────────────────────────────────────
const UI_PORT      = 3000;
const GAME_WS_PORT = 21213;

// ── MAPEAMENTOS — PERSISTÊNCIA ───────────────────────────────────
const MAPPINGS_FILE = path.join(USER_DATA, 'mappings.json');

function loadMappings() {
  try {
    if (fs.existsSync(MAPPINGS_FILE)) {
      const data = JSON.parse(fs.readFileSync(MAPPINGS_FILE, 'utf8'));
      console.log(`[MAPPINGS] ${data.length} mapeamentos carregados.`);
      return Array.isArray(data) ? data : [];
    }
  } catch (e) {
    console.warn('[MAPPINGS] Erro ao carregar mappings.json:', e.message);
  }
  return [];
}

function saveMappings() {
  try {
    fs.writeFileSync(MAPPINGS_FILE, JSON.stringify(mappings, null, 2));
  } catch (e) {
    console.warn('[MAPPINGS] Erro ao salvar mappings.json:', e.message);
  }
}

// ── ESTADO ────────────────────────────────────────────────────────
let tiktokConn   = null;
let mappings     = loadMappings();
let uiClients    = new Set();
let gameClients  = new Set();
let connected    = false;
let currentUser  = '';
let reconnectTimer   = null;
let reconnectAttempt = 0;
let manualDisconnect = false;
let isConnecting    = false; // impede conexões simultâneas
let currentVolume    = 80; // volume global sincronizado com o slider do frontend (0–100)

// ── BACKOFF EXPONENCIAL COM TETO ──────────────────────────────────
const RECONNECT_BASE_MS = 10_000;
const RECONNECT_MAX_MS  = 5 * 60 * 1000;
const RECONNECT_MAX_TRY = 10; // desiste apenas após 10 tentativas (~20min total)

function calcBackoff(attempt) {
  const exp    = Math.min(RECONNECT_BASE_MS * Math.pow(2, attempt - 1), RECONNECT_MAX_MS);
  const jitter = exp * 0.15 * (Math.random() * 2 - 1);
  return Math.round(exp + jitter);
}

// ── SISTEMA VIP ───────────────────────────────────────────────────
const VIP_FILE      = path.join(USER_DATA, 'vips.json');
const VIP_TTL_MS  = 3 * 24 * 60 * 60 * 1000;
let vipMap = new Map();

function loadVips() {
  try {
    if (fs.existsSync(VIP_FILE)) {
      const data = JSON.parse(fs.readFileSync(VIP_FILE, 'utf8'));
      const obj = data.vips || {};
      vipMap = new Map(Object.entries(obj));
      const now = Date.now();
      for (const [uid, ts] of vipMap)
        if (now - ts > VIP_TTL_MS) vipMap.delete(uid);
      console.log(`[VIP] ${vipMap.size} VIPs carregados (3 dias de tolerância).`);
    }
  } catch (e) {
    console.warn('[VIP] Erro ao carregar vips.json:', e.message);
    vipMap = new Map();
  }
}

function saveVips() {
  try {
    const obj = Object.fromEntries(vipMap);
    fs.writeFileSync(VIP_FILE, JSON.stringify({ vips: obj }, null, 2));
  } catch (e) {
    console.warn('[VIP] Erro ao salvar vips.json:', e.message);
  }
}

function isVip(uid) {
  if (!vipMap.has(uid)) return false;
  const ts = vipMap.get(uid);
  if (Date.now() - ts > VIP_TTL_MS) { vipMap.delete(uid); return false; }
  return true;
}

function renewVip(uid, reason) {
  vipMap.set(uid, Date.now());
  saveVips();
  console.log(`[VIP] ⭐ ${uid} VIP renovado por ${reason} (válido por 3 dias)`);
}

loadVips();

// ── MAPA DE NÍVEIS ────────────────────────────────────────────────
const playerLevelMap = new Map();

// ── ACUMULADOR DE LIKES (por mapeamento + usuário) ─────────────────
// Chave: "mappingId:uniqueId" → { count, lastFired }
const likeCumulMap = new Map();

// ── DEDUP DE GIFTS ────────────────────────────────────────────────
const giftMsgIdSeen = new Map();

function isDuplicateGift(data) {
  const now = Date.now();
  for (const [k, ts] of giftMsgIdSeen)
    if (now - ts > 8_000) giftMsgIdSeen.delete(k);

  if (!data.msgId) return false;

  if (giftMsgIdSeen.has(data.msgId)) {
    console.log(`[DEDUP] Bloqueado msgId duplicado: ${data.msgId}`);
    return true;
  }
  giftMsgIdSeen.set(data.msgId, now);
  return false;
}

// ── COOLDOWN DE FOLLOW / SHARE (10 min por usuário) ─────────────
const FOLLOW_SHARE_COOLDOWN_MS = 10 * 60 * 1000; // 10 minutos
const followCooldown = new Map(); // uniqueId → timestamp
const shareCooldown  = new Map();
const memberSeen     = new Set();  // quem já entrou nesta sessão (reseta ao desconectar)

function isOnCooldown(map, uid) {
  const last = map.get(uid);
  if (!last) return false;
  if (Date.now() - last < FOLLOW_SHARE_COOLDOWN_MS) return true;
  map.delete(uid);
  return false;
}

function setCooldown(map, uid) {
  map.set(uid, Date.now());
}

// ── STREAK SAFETY TIMER ───────────────────────────────────────────
const streakPending = new Map();

function processGift(data) {
  if (isDuplicateGift(data)) {
    console.log(`[GIFT] ⚠️  Duplicata descartada: "${data.giftName}" uid=${data.uniqueId}`);
    return;
  }
  console.log(`[GIFT] ✅ PROCESSANDO: nome="${data.giftName}" repeatCount=${data.repeatCount} uid=${data.uniqueId}"`);

  renewVip(data.uniqueId, 'presente');
  console.log(`[VIP] 💎 ${data.nickname} VIP renovado por presente`);

  const diamonds = (data.diamondCount || 0) * (data.repeatCount || 1);
  const payload = {
    uniqueId: data.uniqueId, nickname: data.nickname,
    profilePictureUrl: data.profilePictureUrl,
    giftName: data.giftName, giftId: data.giftId,
    diamondCount: data.diamondCount,
    repeatCount: data.repeatCount || 1,
    totalDiamonds: diamonds,
    msgId: data.msgId || null,
  };

  console.log(`[GIFT] ✅ Enviando: nome="${data.giftName}" uid=${data.uniqueId} gems=${diamonds}`);
  broadcastToGames('gift', payload);
  broadcastUI({ type: 'event', event: 'gift', data: payload });
  checkMappings('gift', payload);
}

// ── HTTP SERVER ───────────────────────────────────────────────────
const httpServer = http.createServer((req, res) => {


  // ── GET CONFIG (/config) ─────────────────────────────────────────
  if (req.method === 'GET' && req.url === '/config') {
    const safe = { ...appConfig };
    if (safe.apiKey && safe.apiKey.length > 12) {
      safe.apiKeyMasked = safe.apiKey.slice(0, 8) + '••••••••' + safe.apiKey.slice(-4);
      safe.hasApiKey = true;
    } else {
      safe.hasApiKey = false;
    }
    delete safe.apiKey;
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify(safe));
    return;
  }

  // ── POST CONFIG (/config) ─────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/config') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        if (data.apiKey !== undefined) {
          appConfig.apiKey = data.apiKey.trim();
          applyConfig(appConfig);
        }
        if (data.clearApiKey) {
          delete appConfig.apiKey;
          setApiKey('');
        }
        saveConfig(appConfig);
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true }));
        broadcastUI({ type: 'config_saved' });
      } catch (e) { res.writeHead(400); res.end('JSON invalido'); }
    });
    return;
  }

  // ── PROXY DE AVATAR ──────────────────────────────────────────────
  if (req.url && req.url.startsWith('/avatar?')) {
    const raw = req.url.slice('/avatar?url='.length);
    let avatarUrl;
    try { avatarUrl = decodeURIComponent(raw); } catch { res.writeHead(400); res.end(); return; }
    if (!avatarUrl.startsWith('http')) { res.writeHead(400); res.end(); return; }

    const client = avatarUrl.startsWith('https') ? https : http;
    const reqOpts = new URL(avatarUrl);
    const options = {
      hostname: reqOpts.hostname,
      path: reqOpts.pathname + reqOpts.search,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Referer': 'https://www.tiktok.com/',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
      },
    };

    const proxyReq = client.get(options, (proxyRes) => {
      if (proxyRes.statusCode >= 300 && proxyRes.statusCode < 400 && proxyRes.headers.location) {
        res.writeHead(302, { Location: '/avatar?url=' + encodeURIComponent(proxyRes.headers.location) });
        res.end();
        return;
      }
      res.writeHead(proxyRes.statusCode || 200, {
        'Content-Type': proxyRes.headers['content-type'] || 'image/jpeg',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=3600',
      });
      proxyRes.pipe(res);
    });
    proxyReq.on('error', () => { res.writeHead(502); res.end(); });
    return;
  }

  // ── SERVIR ARQUIVOS DE SOM (/sounds/<filename>) ──────────────────
  if (req.url && req.url.startsWith('/sounds/')) {
    const filename = path.basename(req.url.split('?')[0]);
    const soundPath = path.join(USER_DATA, 'sounds', filename);
    if (!fs.existsSync(soundPath)) { res.writeHead(404); res.end('Arquivo não encontrado'); return; }
    const ext = path.extname(filename).toLowerCase();
    const mimeTypes = { '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4', '.aac': 'audio/aac' };
    const ct = mimeTypes[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': ct, 'Access-Control-Allow-Origin': '*' });
    fs.createReadStream(soundPath).pipe(res);
    return;
  }

  // ── ABRIR PASTA DE SONS NO EXPLORER (/sounds-open-folder) ────────
  if (req.method === 'POST' && req.url === '/sounds-open-folder') {
    const soundsDir = path.join(USER_DATA, 'sounds');
    if (!fs.existsSync(soundsDir)) fs.mkdirSync(soundsDir, { recursive: true });
    exec(`explorer "${soundsDir}"`);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ ok: true, path: soundsDir }));
    return;
  }

  // ── RETORNAR CAMINHO DA PASTA DE SONS (/sounds-path) ─────────────
  if (req.method === 'GET' && req.url === '/sounds-path') {
    const soundsDir = path.join(USER_DATA, 'sounds');
    if (!fs.existsSync(soundsDir)) fs.mkdirSync(soundsDir, { recursive: true });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ path: soundsDir }));
    return;
  }

  // ── LISTAR SONS DISPONÍVEIS (/sounds-list) ──────────────────────
  if (req.url === '/sounds-list') {
    const soundsDir = path.join(USER_DATA, 'sounds');
    let files = [];
    try {
      if (!fs.existsSync(soundsDir)) fs.mkdirSync(soundsDir, { recursive: true });
      files = fs.readdirSync(soundsDir).filter(f => /\.(mp3|wav|ogg|m4a|aac)$/i.test(f));
    } catch {}
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify(files));
    return;
  }

  // ── UPLOAD DE SOM via base64/JSON (POST /sounds-upload) ─────────
  // Recebe: { files: [{ name, data }] }  onde data é base64 puro
  if (req.method === 'POST' && req.url === '/sounds-upload') {
    const soundsDir = path.join(USER_DATA, 'sounds');
    if (!fs.existsSync(soundsDir)) fs.mkdirSync(soundsDir, { recursive: true });

    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      try {
        const body = Buffer.concat(chunks).toString('utf8');
        const payload = JSON.parse(body);
        const files   = Array.isArray(payload.files) ? payload.files : [];
        let saved = 0;
        for (const f of files) {
          if (!f.name || !f.data) continue;
          const filename = path.basename(f.name);
          if (!/\.(mp3|wav|ogg|m4a|aac)$/i.test(filename)) continue;
          const buf = Buffer.from(f.data, 'base64');
          fs.writeFileSync(path.join(soundsDir, filename), buf);
          saved++;
          console.log(`[SOM] ✅ Salvo: ${filename} (${buf.length} bytes)`);
        }
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: true, saved }));
        if (saved > 0) broadcastUI({ type: 'sounds_updated' });
      } catch (e) {
        console.error('[SOM] Erro no upload:', e.message);
        res.writeHead(400, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    });
    return;
  }

  const file = path.join(__dirname, 'ui', 'index.html');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(500); res.end('Erro ao carregar UI'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// Helper: divide Buffer por separador
function splitBuffer(buf, sep) {
  const parts = [];
  let start = 0;
  let idx;
  while ((idx = indexOf(buf, sep, start)) !== -1) {
    parts.push(buf.slice(start, idx));
    start = idx + sep.length;
  }
  parts.push(buf.slice(start));
  return parts;
}

function indexOf(buf, search, fromIndex = 0) {
  for (let i = fromIndex; i <= buf.length - search.length; i++) {
    let match = true;
    for (let j = 0; j < search.length; j++) {
      if (buf[i + j] !== search[j]) { match = false; break; }
    }
    if (match) return i;
  }
  return -1;
}

httpServer.listen(UI_PORT, () => {
  console.log(`\n🎮 Interface aberta em → http://localhost:${UI_PORT}`);
});

// AUTO-RECONEXÃO DESATIVADA — o usuário deve clicar em Conectar manualmente.

// ── WEBSOCKET — UI ────────────────────────────────────────────────
const uiWss = new WebSocket.Server({ server: httpServer });

uiWss.on('connection', (ws) => {
  uiClients.add(ws);

  ws.send(JSON.stringify({
    type: 'state',
    connected,
    user: currentUser,
    mappings,
  }));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      handleUIMessage(msg, ws);
    } catch {}
  });

  ws.on('close', () => uiClients.delete(ws));
});

// ── WEBSOCKET — JOGOS HTML ────────────────────────────────────────
const EVENT_BUFFER_SIZE = 200;
const eventBuffer = [];

function bufferEvent(event, data) {
  eventBuffer.push({ event, data, ts: Date.now() });
  if (eventBuffer.length > EVENT_BUFFER_SIZE) eventBuffer.shift();
}

const gameWss = new WebSocket.Server({ port: GAME_WS_PORT });

gameWss.on('connection', (ws) => {
  gameClients.add(ws);
  console.log(`[JOGO] conectado — total: ${gameClients.size}`);

  const cutoff = Date.now() - 30_000;
  const missed = eventBuffer.filter(e => e.ts >= cutoff);
  if (missed.length > 0) {
    console.log(`[JOGO] reenviando ${missed.length} eventos perdidos...`);
    for (const e of missed) {
      if (ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify({ event: e.event, data: e.data, replayed: true }));
    }
  }

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.event && msg.event.startsWith('game_')) {
        const d = msg.data || {};
        if ((msg.event === 'game_join' || msg.event === 'game_revive') && d.nickname && d.level) {
          const entry = playerLevelMap.get(d.nickname) || {};
          playerLevelMap.set(d.nickname, { uid: d.uniqueId || entry.uid || '', level: d.level });
        }
        if (msg.event === 'game_level_up' && d.nickname && d.level) {
          const entry = playerLevelMap.get(d.nickname) || {};
          const uid = entry.uid || '';
          playerLevelMap.set(d.nickname, { uid, level: d.level });
          if (d.level >= 3 && uid) renewVip(uid, `nível ${d.level}`);
        }
        broadcastToGames(msg.event, msg.data || {}, ws);
        console.log(`[RELAY] ${msg.event} → ${gameClients.size - 1} cliente(s)`);
      }
    } catch {}
  });

  ws.on('close', () => {
    gameClients.delete(ws);
    console.log(`[JOGO] desconectado — total: ${gameClients.size}`);
  });
});

function broadcastToGames(event, data, skipSender = null) {
  if (['gift', 'member', 'follow', 'share'].includes(event)) bufferEvent(event, data);
  const payload = JSON.stringify({ event, data });
  for (const c of gameClients) {
    if (c === skipSender) continue;
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  }
}

// ── MENSAGENS DA UI ───────────────────────────────────────────────
function handleUIMessage(msg, ws) {
  switch (msg.type) {

    case 'connect':
      connectTikTok(msg.username);
      break;

    case 'disconnect':
      disconnectTikTok();
      break;

    case 'add_mapping':
      mappings.push({ ...msg.mapping });
      saveMappings();
      broadcastUI({ type: 'mappings', mappings });
      break;

    case 'remove_mapping':
      mappings = mappings.filter(m => m.id !== msg.id);
      saveMappings();
      broadcastUI({ type: 'mappings', mappings });
      break;

    case 'test_webhook':
      if (msg.url) testWebhook(msg.url, ws);
      break;

    case 'set_volume':
      if (typeof msg.volume === 'number') {
        currentVolume = Math.min(100, Math.max(0, msg.volume));
        console.log(`[VOLUME] 🔊 Volume global atualizado para ${currentVolume}%`);
      }
      break;
  }
}

// ── VALIDAR API KEY EULERSTREAM ──────────────────────────────────
function validateEulerstreamKey(apiKey) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ apiKey });
    const options = {
      hostname: 'tiktok.eulerstream.com',
      path: '/webcast/sign_url?client=ttlive-node&apiKey=' + encodeURIComponent(apiKey) + '&url=https%3A%2F%2Ftest',
      method: 'GET',
      headers: { 'User-Agent': 'TikTokLiveMacro/1.0' },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        // Se retornar 401 ou 403 a chave é inválida
        if (res.statusCode === 401 || res.statusCode === 403) {
          reject(new Error('Chave não autorizada (HTTP ' + res.statusCode + ')'));
        } else {
          resolve();
        }
      });
    });
    req.setTimeout(5000, () => { req.destroy(); reject(new Error('Timeout ao validar chave')); });
    req.on('error', (err) => {
      // Se não conseguir contatar o servidor, deixa passar (pode ser sem internet)
      console.warn('[EULER] Não foi possível validar a chave online:', err?.message || err);
      resolve();
    });
    req.end();
  });
}

// ── CONECTAR AO TIKTOK ────────────────────────────────────────────
async function connectTikTok(username) {
  // Impede chamadas simultâneas — mas com proteção via try/finally
  // para garantir que isConnecting SEMPRE seja resetado mesmo em erros inesperados
  if (isConnecting) {
    console.warn('[CONNECT] ⚠️  Já existe uma conexão em andamento — ignorando chamada duplicada.');
    return;
  }
  isConnecting = true;

  try {
    // Valida formato da chave
    const apiKey = (appConfig.apiKey || '').trim();
    if (!apiKey || !apiKey.startsWith('euler_') || apiKey.length < 20) {
      console.warn('⛔ Conexão bloqueada: API Key inválida ou não configurada.');
      broadcastUI({
        type: 'status',
        status: 'error',
        message: 'API Key inválida. A chave deve começar com "euler_". Acesse eulerstream.com para obter sua chave gratuita.',
      });
      return; // finally garante isConnecting = false
    }

    // Verifica a chave na API da Eulerstream
    broadcastUI({ type: 'status', status: 'connecting', user: username, message: 'Validando API Key...' });
    console.log('🔑 Validando API Key Eulerstream...');
    try {
      await validateEulerstreamKey(apiKey);
      console.log('✅ API Key válida.');
    } catch (err) {
      console.warn('⛔ API Key rejeitada:', err?.message || err);
      broadcastUI({
        type: 'status',
        status: 'error',
        message: `API Key inválida ou expirada. Acesse eulerstream.com para verificar sua chave.`,
      });
      return; // finally garante isConnecting = false
    }

    if (tiktokConn) tiktokConn.disconnect();

    manualDisconnect = false;
    currentUser = username.startsWith('@') ? username : '@' + username;

    // Salva o username para restaurar na próxima abertura
    appConfig.lastUsername = currentUser;
    saveConfig(appConfig);

    broadcastUI({ type: 'status', status: 'connecting', user: currentUser });
    console.log(`\n⏳ Conectando a ${currentUser}…`);

    tiktokConn = new WebcastPushConnection(currentUser, {
      processInitialData: false,
      enableExtendedGiftInfo: false,
      enableWebsocketUpgrade: true,
      apiKey: appConfig.apiKey || '',
    });

    // A partir daqui isConnecting pode ser liberado — connect() é async e os
    // callbacks .then/.catch cuidam do resto de forma independente
    isConnecting = false;

    tiktokConn.connect()
      .then(state => {
        connected = true;
        reconnectAttempt = 0;
        console.log(`✅ Conectado! Viewers: ${state.viewerCount}`);
        broadcastUI({ type: 'status', status: 'connected', user: currentUser, viewers: state.viewerCount });
        attachEvents();
      })
      .catch(err => {
        connected = false;
        const msg = (err && err.message) ? err.message : String(err || 'Erro desconhecido');
        console.error(`❌ Falha ao conectar: ${msg}`);
        const isRateLimit = msg.includes('rate_limit') || msg.includes('Rate Limited') || msg.includes('Too many connections');
        if (isRateLimit) {
          console.warn('⛔ Rate limit — reconexão automática desativada.');
          broadcastUI({ type: 'status', status: 'error', message: '⏳ Limite de conexões da Eulerstream atingido. Aguarde e tente novamente.' });
          manualDisconnect = true;
          return;
        }
        // Erro 500/408 da Eulerstream = servidor sobrecarregado, espera 30s extra antes de tentar
        const isServerError = msg.includes('status 500') || msg.includes('408') || msg.includes('Unexpected server response');
        if (isServerError) {
          console.warn('⏳ Erro no servidor Eulerstream (500/408) — aguardando 30s extra antes de reconectar...');
          broadcastUI({ type: 'status', status: 'error', message: '⏳ Servidor Eulerstream instável. Aguardando 30s para reconectar...' });
          setTimeout(() => scheduleReconnectDirect(), 30_000);
          return;
        }
        broadcastUI({ type: 'status', status: 'error', message: msg });
        scheduleReconnectDirect();
      });

  } finally {
    // Garante que a flag seja liberada mesmo em erros inesperados (exceções, etc.)
    // Nota: só chega aqui nos caminhos de retorno antecipado (apiKey inválida, etc.)
    // Nos outros caminhos isConnecting já foi liberado acima antes do connect()
    if (isConnecting) {
      isConnecting = false;
      console.warn('[CONNECT] isConnecting liberado via finally — verifique o fluxo acima.');
    }
  }
}

function scheduleReconnectDirect() {
  if (manualDisconnect || reconnectTimer) return;

  reconnectAttempt++;
  if (reconnectAttempt > RECONNECT_MAX_TRY) {
    console.warn(`⛔ ${RECONNECT_MAX_TRY} tentativas sem sucesso. Reconecte manualmente.`);
    broadcastUI({ type: 'status', status: 'gave_up', user: currentUser,
      message: `Sem conexão após ${RECONNECT_MAX_TRY} tentativas. Reconecte manualmente.` });
    reconnectAttempt = 0;
    return;
  }

  const delay = calcBackoff(reconnectAttempt);
  const delaySec = (delay / 1000).toFixed(0);
  console.log(`🔄 Tentativa ${reconnectAttempt}/${RECONNECT_MAX_TRY} em ${delaySec}s...`);
  broadcastUI({ type: 'status', status: 'reconnecting', user: currentUser,
    attempt: reconnectAttempt, maxAttempts: RECONNECT_MAX_TRY, delaySeconds: Number(delaySec) });

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    if (currentUser && !manualDisconnect) connectTikTok(currentUser);
  }, delay);
}

function disconnectTikTok() {
  manualDisconnect = true;
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  reconnectAttempt = 0;
  if (tiktokConn) { tiktokConn.disconnect(); tiktokConn = null; }
  connected = false;
  currentUser = '';
  memberSeen.clear();
  broadcastUI({ type: 'status', status: 'disconnected' });
  console.log('🔌 Desconectado manualmente — reconexão automática desativada.');
}

// ── EVENTOS DO TIKTOK ─────────────────────────────────────────────
function attachEvents() {
  tiktokConn.removeAllListeners();

  // 💬 Chat
  tiktokConn.on('chat', data => {
    const payload = {
      uniqueId: data.uniqueId, nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl || (data.profilePictureUrls && data.profilePictureUrls[0]) || '',
      comment: data.comment,
    };
    broadcastToGames('chat', payload);
    broadcastUI({ type: 'event', event: 'chat', data: payload });
    checkMappings('chat', payload);
  });

  // 👤 Entrou na live
  tiktokConn.on('member', data => {
    const uid = data.uniqueId;
    if (memberSeen.has(uid)) return; // já disparou nesta sessão
    memberSeen.add(uid);
    const vip = isVip(uid);
    const level = playerLevelMap.get(data.nickname)?.level || 1;
    const payload = {
      uniqueId: uid, nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl || (data.profilePictureUrls && data.profilePictureUrls[0]) || '',
      isVip: vip, level,
    };
    broadcastToGames('member', payload);
    broadcastUI({ type: 'event', event: 'member', data: payload });
    checkMappings('member', payload);
    if (vip) console.log(`[VIP] ⭐ ${data.nickname} entrou como VIP`);
    if (level >= 3) console.log(`[NÍVEL] 🌟 ${data.nickname} entrou como Nível ${level}`);
  });

  // 🎁 Gift
  tiktokConn.on('gift', data => {
    const streakKey = `${data.uniqueId}:${data.giftId}`;

    if (data.repeatEnd === false) {
      const existing = streakPending.get(streakKey);
      const bestCount = Math.max(data.repeatCount || 1, existing?.data?.repeatCount || 1);
      if (existing?.timer) clearTimeout(existing.timer);

      const timer = setTimeout(() => {
        const pending = streakPending.get(streakKey);
        if (pending) {
          console.log(`[STREAK-TIMEOUT] Processando por timeout: "${data.giftName}" uid=${data.uniqueId} count=${pending.data.repeatCount}`);
          streakPending.delete(streakKey);
          processGift(pending.data);
        }
      }, 6_000);

      streakPending.set(streakKey, { data: { ...data, repeatCount: bestCount }, timer });
      console.log(`[GIFT-RAW] streak em progresso: "${data.giftName}" uid=${data.uniqueId} count=${bestCount} (aguardando repeatEnd)`);
      return;
    }

    const existing = streakPending.get(streakKey);
    if (existing?.timer) { clearTimeout(existing.timer); streakPending.delete(streakKey); }

    console.log(`[GIFT-RAW] nome="${data.giftName}" uid="${data.uniqueId}" repeatCount=${data.repeatCount} repeatEnd=${data.repeatEnd} diamonds=${data.diamondCount} msgId=${data.msgId||'(none)'}`);
    processGift(data);
  });

  // ❤️ Like
  tiktokConn.on('like', data => {
    const payload = {
      uniqueId: data.uniqueId, nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl || (data.profilePictureUrls && data.profilePictureUrls[0]) || '',
      likeCount: data.likeCount, totalLikeCount: data.totalLikeCount,
    };
    broadcastToGames('like', payload);
    broadcastUI({ type: 'event', event: 'like', data: payload });
    checkMappings('like', payload);
  });

  // 👁 Viewers
  tiktokConn.on('roomUser', data => {
    broadcastToGames('viewer', { viewerCount: data.viewerCount });
    broadcastUI({ type: 'viewers', count: data.viewerCount });
  });

  // 🔔 Follow (cooldown 10 min por usuário)
  tiktokConn.on('follow', data => {
    if (isOnCooldown(followCooldown, data.uniqueId)) {
      console.log(`[FOLLOW] ⏱️  Cooldown ativo para ${data.uniqueId} — ignorado`);
      return;
    }
    setCooldown(followCooldown, data.uniqueId);
    const payload = {
      uniqueId: data.uniqueId, nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl || (data.profilePictureUrls && data.profilePictureUrls[0]) || '',
    };
    broadcastToGames('follow', payload);
    broadcastUI({ type: 'event', event: 'follow', data: payload });
    checkMappings('follow', payload);
  });

  // 🔗 Compartilhou (cooldown 10 min por usuário)
  tiktokConn.on('share', data => {
    if (isOnCooldown(shareCooldown, data.uniqueId)) {
      console.log(`[SHARE] ⏱️  Cooldown ativo para ${data.uniqueId} — ignorado`);
      return;
    }
    setCooldown(shareCooldown, data.uniqueId);
    const payload = {
      uniqueId: data.uniqueId, nickname: data.nickname,
      profilePictureUrl: data.profilePictureUrl || (data.profilePictureUrls && data.profilePictureUrls[0]) || '',
    };
    broadcastToGames('share', payload);
    broadcastUI({ type: 'event', event: 'share', data: payload });
    checkMappings('share', payload);
    console.log(`[SHARE] ${data.nickname} compartilhou a live!`);
  });

  // 🔄 Republicou
  tiktokConn.on('subscribe', data => {
    const payload = { uniqueId: data.uniqueId, nickname: data.nickname };
    broadcastToGames('repost', payload);
    broadcastUI({ type: 'event', event: 'repost', data: payload });
    console.log(`[REPOST] ${data.nickname} republicou a live!`);
  });

  // ── RECONEXÃO AUTOMÁTICA ────────────────────────────────────────
  // Flag para evitar que 'error' + 'disconnected' disparem dois timers simultâneos
  let reconnectScheduled = false;

  function scheduleReconnect() {
    if (manualDisconnect) return;
    if (reconnectTimer) return;
    if (reconnectScheduled) return;  // já agendado neste ciclo
    reconnectScheduled = true;

    reconnectAttempt++;

    if (reconnectAttempt > RECONNECT_MAX_TRY) {
      console.warn(`⛔ ${RECONNECT_MAX_TRY} tentativas sem sucesso.`);
      broadcastUI({ type: 'status', status: 'gave_up', user: currentUser,
        message: `Sem conexão após ${RECONNECT_MAX_TRY} tentativas. Reconecte manualmente.` });
      reconnectAttempt = 0;
      reconnectScheduled = false;
      return;
    }

    const delay = calcBackoff(reconnectAttempt);
    const delaySec = (delay / 1000).toFixed(0);
    console.log(`🔄 Tentativa ${reconnectAttempt}/${RECONNECT_MAX_TRY} em ${delaySec}s...`);
    broadcastUI({ type: 'status', status: 'reconnecting', user: currentUser,
      attempt: reconnectAttempt, maxAttempts: RECONNECT_MAX_TRY, delaySeconds: Number(delaySec) });

    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      reconnectScheduled = false;
      if (currentUser && !manualDisconnect) connectTikTok(currentUser);
    }, delay);
  }

  tiktokConn.on('error', (err) => {
    // Loga o erro mas NÃO agenda reconexão aqui —
    // o evento 'disconnected' sempre dispara logo depois e cuida disso.
    console.error('❌ Erro TikTok:', err?.message || err);
  });

  tiktokConn.on('streamEnd', (actionId) => {
    console.log('🔴 Live encerrada pelo streamer (streamEnd).');
    manualDisconnect = true; // impede qualquer reconexão
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    connected = false;
    broadcastUI({ type: 'status', status: 'live_ended', user: currentUser });
  });

  tiktokConn.on('disconnected', () => {
    connected = false;
    // Se a live já encerrou (manualDisconnect), não tenta reconectar
    if (manualDisconnect) {
      broadcastUI({ type: 'status', status: 'disconnected' });
      return;
    }
    broadcastUI({ type: 'status', status: 'disconnected' });
    console.log('⚠️  Desconectado do TikTok.');
    scheduleReconnect();
  });
}

// ── VERIFICAR MAPEAMENTOS ─────────────────────────────────────────
function checkMappings(eventType, data) {
  for (const m of mappings) {
    if (m.event !== eventType) continue;

    let match = false;

    if (eventType === 'gift') {
      if (m.condition === 'name') {
        match = (data.giftName || '').toLowerCase() === m.value.toLowerCase();
      } else if (m.condition === 'min_diamonds') {
        match = data.totalDiamonds >= parseInt(m.value || 0);
      }
    } else if (eventType === 'chat') {
      if (m.condition === 'keyword') {
        match = (data.comment || '').toLowerCase().includes(m.value.toLowerCase());
      } else {
        match = true;
      }
    } else if (eventType === 'like') {
      const threshold = parseInt(m.likeThreshold) || 0;
      if (threshold > 1) {
        // Contador por usuário — cada um tem sua própria contagem
        const userKey    = m.id + ':' + data.uniqueId;
        if (!likeCumulMap.has(userKey)) likeCumulMap.set(userKey, { count: 0 });
        const userEntry  = likeCumulMap.get(userKey);
        userEntry.count += (data.likeCount || 1);

        // Cooldown global por mapeamento — qualquer usuário que disparar bloqueia os demais
        const mappingKey = 'fired:' + m.id;
        if (!likeCumulMap.has(mappingKey)) likeCumulMap.set(mappingKey, { lastFired: 0 });
        const firedEntry = likeCumulMap.get(mappingKey);

        const cooldownMs = (parseFloat(m.likeCooldown) || 0) * 1000;
        const now = Date.now();
        if (userEntry.count >= threshold && (now - firedEntry.lastFired) >= cooldownMs) {
          userEntry.count      = 0;
          firedEntry.lastFired = now;
          match = true;
        }
      } else {
        match = true;
      }
    } else {
      match = true;
    }

    if (match) {
      // ── Pressionar tecla ────────────────────────────────────────
      if (m.key) pressKey(m.key);

      // ── Tocar som ────────────────────────────────────────────────
      // Manda o frontend tocar via WebSocket (Electron tem no-user-gesture-required,
      // então não é bloqueado por autoplay). PowerShell como fallback secundário.
      if (m.sound) {
        broadcastUI({ type: 'play_sound', sound: m.sound, volume: currentVolume });
        playSoundNative(m.sound, currentVolume);
      }

      // ── Disparar webhook ────────────────────────────────────────
      if (m.webhook) {
        const webhookUrl = m.webhook
          .replace(/\{username\}/gi,   encodeURIComponent((data.uniqueId || '').trim()))
          .replace(/\{uniqueid\}/gi,   encodeURIComponent((data.uniqueId || '').trim()))
          .replace(/\{gift\}/gi,       encodeURIComponent(data.giftName  || ''))
          .replace(/\{diamonds\}/gi,   encodeURIComponent(data.totalDiamonds || 0))
          .replace(/\{comment\}/gi,    encodeURIComponent(data.comment   || ''));
        fireWebhook(webhookUrl, {
          event: eventType,
          mapping: { id: m.id, event: m.event, key: m.key, webhook: m.webhook },
          user: data.uniqueId,
          uniqueId: data.uniqueId,
          data,
          timestamp: new Date().toISOString(),
        });
      }

      broadcastUI({
        type: 'triggered',
        mapping: { id: m.id, event: m.event, key: m.key || '', webhook: m.webhook || '', sound: m.sound || '' },
        user: data.uniqueId,
        detail: eventType === 'gift' ? `${data.giftName} (${data.totalDiamonds}💎)` : (data.comment || ''),
      });
      console.log(`[MACRO] ${m.key || '(sem tecla)'} ${m.sound ? '+ som' : ''} ${m.webhook ? '+ webhook' : ''} → ${eventType} de ${data.uniqueId}`);
    }
  }
}

// ── SIMULAR TECLA (Windows via PowerShell) ────────────────────────
function pressKey(key) {
  const safe = key.replace(/[^a-zA-Z0-9\+\%\^\{\}\(\)~]/g, '');
  if (!safe) return;

  const ps = `Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.SendKeys]::SendWait('${safe}')`;
  exec(`powershell -NoProfile -Command "${ps}"`, (err) => {
    if (err) console.error(`[TECLA] Erro ao pressionar ${safe}:`, err?.message || err);
  });
}

// ── TOCAR SOM NATIVO (PowerShell -STA + MediaPlayer) ─────────────
// Executa no processo Node.js — completamente independente do Chromium.
// Nunca é bloqueado por política de autoplay, funciona ao reabrir o app.
// Usa -STA (Single-Threaded Apartment), obrigatório para MediaPlayer do WPF.
// Escreve um .ps1 temporário para evitar qualquer problema de escape de shell.
function playSoundNative(filename, volumePct) {
  try {
    const soundPath = path.join(USER_DATA, 'sounds', path.basename(filename));
    if (!fs.existsSync(soundPath)) {
      console.warn(`[SOM] Arquivo não encontrado: ${soundPath}`);
      return;
    }

    const vol = Math.min(1, Math.max(0, (typeof volumePct === 'number' ? volumePct : 80) / 100));

    // URI com barras normais e codificada — MediaPlayer não aceita backslash nem espaços
    const uri = encodeURI('file:///' + soundPath.replace(/\\/g, '/'));

    // Script PS1 em arquivo temporário — sem nenhum problema de escape
    const tmpScript = path.join(os.tmpdir(), `ttlm_${Date.now()}.ps1`);
    const psContent = `
Add-Type -AssemblyName presentationCore
$m = New-Object System.Windows.Media.MediaPlayer
$m.Open([uri]'${uri.replace(/'/g, "''")}')
$m.Volume = ${vol.toFixed(2)}
$m.Play()
$i = 0
while (-not $m.NaturalDuration.HasTimeSpan -and $i -lt 40) {
    Start-Sleep -Milliseconds 100
    $i++
}
if ($m.NaturalDuration.HasTimeSpan) {
    $ms = [int]$m.NaturalDuration.TimeSpan.TotalMilliseconds + 400
} else {
    $ms = 6000
}
Start-Sleep -Milliseconds $ms
$m.Stop()
$m.Close()
`.trimStart();

    // BOM obrigatório para PowerShell 5.x interpretar UTF-8 corretamente
    fs.writeFileSync(tmpScript, '\uFEFF' + psContent, 'utf8');

    exec(
      `powershell -STA -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "${tmpScript}"`,
      (err) => {
        try { fs.unlinkSync(tmpScript); } catch (_) {}
        if (err) console.error(`[SOM] Erro ao tocar "${filename}": ${err.message}`);
        else     console.log(`[SOM] ✅ Tocou: ${filename} (vol=${Math.round(vol * 100)}%)`);
      }
    );
  } catch (e) {
    console.error('[SOM] Exceção em playSoundNative:', e.message);
  }
}


// Faz POST com JSON para a URL configurada. Timeout de 5s.
function fireWebhook(url, payload) {
  try {
    const body    = JSON.stringify(payload);
    const parsed  = new URL(url);
    const isHttps = parsed.protocol === 'https:';
    const client  = isHttps ? https : http;

    const options = {
      hostname: parsed.hostname,
      port:     parsed.port || (isHttps ? 443 : 80),
      path:     parsed.pathname + parsed.search,
      method:   'POST',
      headers:  {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(body),
        'User-Agent':     'TikTokLiveMacro/1.0',
        'X-Event-Source': 'tiktok-live-macro',
      },
    };

    const req = client.request(options, (res) => {
      let resBody = '';
      res.on('data', chunk => resBody += chunk);
      res.on('end', () => {
        console.log(`[WEBHOOK] ✅ ${url} → HTTP ${res.statusCode}`);
        broadcastUI({ type: 'webhook_result', url, status: res.statusCode, ok: res.statusCode < 400 });
      });
    });

    req.setTimeout(5000, () => {
      req.destroy();
      console.warn(`[WEBHOOK] ⏱️  Timeout: ${url}`);
      broadcastUI({ type: 'webhook_result', url, status: 0, ok: false, error: 'Timeout' });
    });

    req.on('error', (err) => {
      console.error(`[WEBHOOK] ❌ Erro: ${url} — ${err?.message || err}`);
      broadcastUI({ type: 'webhook_result', url, status: 0, ok: false, error: err?.message || String(err) });
    });

    req.write(body);
    req.end();
  } catch (err) {
    console.error(`[WEBHOOK] ❌ URL inválida: ${url} — ${err?.message || err}`);
    broadcastUI({ type: 'webhook_result', url, status: 0, ok: false, error: err?.message || String(err) });
  }
}

// ── TESTAR WEBHOOK MANUALMENTE ────────────────────────────────────
function testWebhook(url, ws) {
  const testPayload = {
    event: 'test',
    mapping: { event: 'test', key: 'TEST' },
    user: 'TestUser',
    uniqueId: 'test_user',
    data: { nickname: 'TestUser', comment: 'Teste de webhook ✅' },
    timestamp: new Date().toISOString(),
    isTest: true,
  };
  console.log(`[WEBHOOK] 🧪 Teste para: ${url}`);
  fireWebhook(url, testPayload);
}

// ── BROADCAST PARA UI ─────────────────────────────────────────────
function broadcastUI(msg) {
  const payload = JSON.stringify(msg);
  for (const c of uiClients) {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  }
}

console.log('\n══════════════════════════════════════');
console.log('   TIKTOK LIVE MACRO');
console.log('══════════════════════════════════════');
console.log(`   UI:     http://localhost:${UI_PORT}`);
console.log(`   Jogos:  ws://localhost:${GAME_WS_PORT}`);
console.log(`   Sons:   ./sounds/  (mp3, wav, ogg)`);
console.log('══════════════════════════════════════\n');