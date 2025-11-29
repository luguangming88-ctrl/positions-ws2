const crypto = require('crypto')
const WebSocket = require('ws')

const {
OKX_API_KEY,
OKX_API_SECRET,
OKX_PASSPHRASE,
API_ID,
SYMBOLS = '',
SUPABASE_FUNCTION_URL,
SUPABASE_SERVICE_ROLE_KEY,
} = process.env

let priv = null
let pub = null

function tsISO () { return new Date().toISOString() }
function sign (message, secret) { return crypto.createHmac('sha256', secret).update(message).digest('base64') }
function instIdToSymbol (instId) { return instId.replace('-SWAP', '').replace('-', '/') + ':USDT' }

async function postStrategyExec (payload) {
const r = await fetch(SUPABASE_FUNCTION_URL, {
method: 'POST',
headers: {
'Content-Type': 'application/json',
Authorization: 'Bearer ' + SUPABASE_SERVICE_ROLE_KEY,
},
body: JSON.stringify(payload),
})
if (!r.ok) throw new Error('strategy-exec HTTP ' + r.status)
}

function setupHeartbeat(ws) {
clearInterval(ws._hb)
ws. hb = setInterval(() => {
try { ws.send(JSON.stringify({ op: 'ping' })) } catch ( ) {}
}, 20000)
}

function startPrivateWS () {
if (priv && priv.readyState === 1) return
const ws = new WebSocket('wss://ws.okx.com/ws/v5/private')
priv = ws
ws.on('open', () => {
const ts = tsISO()
const sig = sign(ts + 'GET' + '/users/self/verify', OKX_API_SECRET)
ws.send(JSON.stringify({ op: 'login', args: [{ apiKey: OKX_API_KEY, passphrase: OKX_PASSPHRASE, timestamp: ts, sign: sig }] }))
ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'positions', instType: 'SWAP' }] }))
setupHeartbeat(ws)
console.log('private-ws-open')
})
ws.on('message', async (data) => {
try {
const msg = JSON.parse(data)
if (msg.event === 'error') return
if (msg.event === 'login') return
if (msg.op === 'pong' || msg.event === 'pong') return
if (msg.arg?.channel !== 'positions') return
const rows = msg.data || []
for (const p of rows) {
const pos = Math.abs(parseFloat(p.pos || '0'))
if (pos <= 0) continue
const symbol = instIdToSymbol(p.instId)
const uplRatio = parseFloat(p.uplRatio || '0')
await postStrategyExec({ apiId: API_ID, symbol, posSide: p.posSide, size: pos, uplRatio, ts: Date.now() })
}
} catch (_) {}
})
ws.on('close', () => { clearInterval(ws._hb); priv = null; setTimeout(startPrivateWS, 2000) })
ws.on('error', () => { clearInterval(ws. hb); try { ws.close() } catch ( ) {} priv = null; setTimeout(startPrivateWS, 2000) })
}

function startPublicWS () {
const list = SYMBOLS.split(',').map(s => s.trim()).filter(Boolean)
if (!list.length) return
if (pub && pub.readyState === 1) return
const ws = new WebSocket('wss://ws.okx.com/ws/v5/public')
pub = ws
ws.on('open', () => {
const args = list.map(instId => ({ channel: 'candle1H', instId }))
ws.send(JSON.stringify({ op: 'subscribe', args }))
setupHeartbeat(ws)
console.log('public-ws-open', args)
})
ws.on('message', async (data) => {
try {
const msg = JSON.parse(data)
if (msg.op === 'pong' || msg.event === 'pong') return
if (msg.arg?.channel && String(msg.arg.channel).startsWith('candle')) {
const i = msg.arg.instId
const sym = instIdToSymbol(i)
const d = msg.data && msg.data[0]
if (d && d.length >= 5) {
const o = parseFloat(d[1])
const c = parseFloat(d[4])
const candleDir = c >= o ? 'up' : 'down'
await postStrategyExec({ apiId: API_ID, symbol: sym, candleDir, ts: Date.now() })
}
}
} catch (_) {}
})
ws.on('close', () => { clearInterval(ws._hb); pub = null; setTimeout(startPublicWS, 2000) })
ws.on('error', () => { clearInterval(ws. hb); try { ws.close() } catch ( ) {} pub = null; setTimeout(startPublicWS, 2000) })
}

startPrivateWS()
startPublicWS()
console.log('WS forwarder started')
