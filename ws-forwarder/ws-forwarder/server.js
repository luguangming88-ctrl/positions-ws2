SOLO Coder

很好，就在这个页面创建文件。请按顺序创建三个文件：

- 在顶部“Name your file...”输入： ws-forwarder/package.json
  
  - 粘贴内容：
    {
    "name": "okx-ws-forwarder",
    "version": "1.0.0",
    "private": true,
    "type": "module",
    "scripts": { "start": "node server.js" },
    "dependencies": { "ws": "8.17.0" }
    }
  - 右上角 Commit changes... → 提交到 main
- 再点击 Add file → Create new file
  
  - 名称： ws-forwarder/server.js
  - 粘贴内容（完整）：
    import crypto from 'crypto'
    import WebSocket from 'ws'
    
    const {
    OKX_API_KEY,
    OKX_API_SECRET,
    OKX_PASSPHRASE,
    API_ID,
    SYMBOLS = '',
    SUPABASE_FUNCTION_URL,
    SUPABASE_SERVICE_ROLE_KEY,
    } = process.env
    
    function tsISO() { return new Date().toISOString() }
    function sign(message, secret) { return crypto.createHmac('sha256', secret).update(message).digest('base64') }
    function instIdToSymbol(instId) { return instId.replace('-SWAP','').replace('-', '/')+':USDT' }
    
    async function postStrategyExec(payload) {
    const r = await fetch(SUPABASE_FUNCTION_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: Bearer ${SUPABASE_SERVICE_ROLE_KEY} },
    body: JSON.stringify(payload)
    })
    if (!r.ok) throw new Error( strategy-exec HTTP ${r.status} )
    }
    
    function startPrivateWS() {
    const ws = new WebSocket('wss://ws.okx.com/ws/v5/private')
    ws.on('open', () => {
    const ts = tsISO()
    const sig = sign(ts + 'GET' + '/users/self/verify', OKX_API_SECRET)
    ws.send(JSON.stringify({ op: 'login', args: [{ apiKey: OKX_API_KEY, passphrase: OKX_PASSPHRASE, timestamp: ts, sign: sig }] }))
    ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'positions', instType: 'SWAP' }] }))
    console.log('private-ws-open')
    })
    ws.on('message', async (data) => {
    try {
    const msg = JSON.parse(data)
    if (msg.event === 'error') return
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
    ws.on('close', () => setTimeout(startPrivateWS, 2000))
    ws.on('error', () => setTimeout(startPrivateWS, 2000))
    }
    
    function startPublicWS() {
    const list = SYMBOLS.split(',').map(s => s.trim()).filter(Boolean)
    if (!list.length) return
    const ws = new WebSocket('wss://ws.okx.com/ws/v5/public')
    ws.on('open', () => {
    const args = list.map(instId => ({ channel: 'candle1H', instId }))
    ws.send(JSON.stringify({ op: 'subscribe', args }))
    console.log('public-ws-open', args)
    })
    ws.on('message', async (data) => {
    try {
    const msg = JSON.parse(data)
    if (msg.arg?.channel && String(msg.arg.channel).startsWith('candle')) {
    const sym = instIdToSymbol(msg.arg.instId)
    const d = msg.data && msg.data[0]
    if (d && d.length >= 5) {
    const o = parseFloat(d[1]); const c = parseFloat(d[4])
    const candleDir = c >= o ? 'up' : 'down'
    await postStrategyExec({ apiId: API_ID, symbol: sym, candleDir, ts: Date.now() })
    }
    }
    } catch (_) {}
    })
    ws.on('close', () => setTimeout(startPublicWS, 2000))
    ws.on('error', () => setTimeout(startPublicWS, 2000))
    }
    
    if (!OKX_API_KEY || !OKX_API_SECRET || !OKX_PASSPHRASE) throw new Error('Missing OKX credentials')
    if (!SUPABASE_FUNCTION_URL || !SUPABASE_SERVICE_ROLE_KEY) throw new Error('Missing Supabase function config')
    if (!API_ID) throw new Error('Missing API_ID')
    
    startPrivateWS()
    startPublicWS()
    console.log('WS forwarder started')
