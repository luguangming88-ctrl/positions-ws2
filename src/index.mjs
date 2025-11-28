function instIdToSymbol(instId) {
  const base = instId.replace('-SWAP', '').replace('-', '/')
  return `${base}:USDT`
}

function symbolToInstId(symbol) {
  return symbol.replace(':USDT','-SWAP').replace('/','-')
}

async function hmacSHA256Base64(message, secret) {
  const enc = new TextEncoder()
  const key = await crypto.subtle.importKey('raw', enc.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const sig = await crypto.subtle.sign('HMAC', key, enc.encode(message))
  return btoa(String.fromCharCode(...new Uint8Array(sig)))
}

async function getJson(url, headers) {
  const r = await fetch(url, { headers })
  return await r.json()
}

async function getRaw(url, headers) {
  const r = await fetch(url, { headers })
  const text = await r.text()
  return { status: r.status, text }
}

async function postJson(url, headers, body) {
  const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) })
  return await r.json()
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

async function postJsonRetry(url, headers, body, opts = {}) {
  const { retries = 3, baseDelay = 500, maxDelay = 60000 } = opts
  for (let attempt = 0; attempt <= retries; attempt++) {
    const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) })
    if (res.ok) return await res.json()
    if (res.status === 429 || res.status >= 500) {
      const jitter = Math.floor(Math.random() * 300)
      const delay = Math.min(baseDelay * Math.pow(2, attempt) + jitter, maxDelay)
      await sleep(delay)
      continue
    }
    throw new Error(`HTTP ${res.status}`)
  }
}

async function listActiveApiIds(env) {
  const headers = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` }
  const rows = await getJson(`${env.SUPABASE_URL}/rest/v1/strategies?status=eq.running&select=api_credential_id`, headers)
  const set = new Set()
  for (const r of rows || []) { if (r.api_credential_id) set.add(r.api_credential_id) }
  return Array.from(set)
}

export class PositionsDO {
  constructor(state, env) {
    this.state = state
    this.env = env
    this.ws = null
    this.pub = null
    this.apiId = ''
    this.creds = null
    this.strategies = new Map()
    this.lastAction = new Map()
    this.candleDir = new Map()
    this.timer = null
    this.diag = { last: null }
  }

  async fetch(request) {
    const url = new URL(request.url)
    this.apiId = url.searchParams.get('apiId') || this.apiId || ''
    if (request.method === 'OPTIONS') return cors('', { status: 204 })
    if (url.pathname === '/start') {
      this.apiId = url.searchParams.get('apiId') || ''
      ;(async () => {
        try {
          postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
            strategy_id: null,
            level: 'info',
            message: 'WS事件：收到启动订阅请求',
            data: { apiId: this.apiId }
          }).catch(()=>{})
          console.log('start-subscription', { apiId: this.apiId })
          await this.loadCredentials()
          await this.loadStrategies()
          this.connectWS()
          if (this.timer) clearInterval(this.timer)
          this.timer = setInterval(() => this.loadStrategies().catch(() => {}), 60000)
        } catch (_) {}
      })()
      return cors('started')
    }
    if (url.pathname === '/refresh') {
      await this.loadStrategies()
      return cors('refreshed')
    }
    if (url.pathname === '/status') {
      if (this.apiId && !this.creds && this.env.SUPABASE_URL && this.env.SUPABASE_SERVICE_ROLE_KEY) {
        try { await this.loadCredentials() } catch (_) {}
        if (this.creds && !this.ws) { try { this.connectWS() } catch (_) {} }
      }
      if (!this.strategies.size && this.apiId) {
        try { await this.loadStrategies() } catch (_) {}
      }
      const symbols = Array.from(this.strategies.keys())
      const body = {
        apiId: this.apiId,
        privateConnected: !!this.ws,
        publicConnected: !!this.pub,
        symbols,
        hasSupabaseEnv: !!(this.env.SUPABASE_URL && this.env.SUPABASE_SERVICE_ROLE_KEY),
        credsPresent: !!this.creds,
      }
      return cors(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
    }
    if (url.pathname === '/diag') {
      if (this.apiId && !this.creds && this.env.SUPABASE_URL && this.env.SUPABASE_SERVICE_ROLE_KEY) {
        try { await this.loadCredentials() } catch (_) {}
      }
      const body = {
        apiId: this.apiId,
        hasSupabaseEnv: !!(this.env.SUPABASE_URL && this.env.SUPABASE_SERVICE_ROLE_KEY),
        credsPresent: !!this.creds,
        symbols: Array.from(this.strategies.keys()),
      }
      return cors(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
    }
    if (url.pathname === '/supa-check') {
      const headers = { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
      const q1 = await getRaw(`${this.env.SUPABASE_URL}/rest/v1/okx_api_credentials?id=eq.${this.apiId}&select=*`, headers)
      const q2 = await getRaw(`${this.env.SUPABASE_URL}/rest/v1/strategies?api_credential_id=eq.${this.apiId}&select=id,symbol,status`, headers)
      return cors(JSON.stringify({ apiId: this.apiId, okx_api_credentials: q1, strategies: q2 }), { headers: { 'Content-Type': 'application/json' } })
    }
    if (url.pathname === '/start-symbol') {
      const symbol = url.searchParams.get('symbol') || ''
      if (!symbol) return cors('symbol required', { status: 400 })
      const instId = symbolToInstId(symbol)
      await this.subscribePublic(instId)
      return cors('symbol-started')
    }
    if (url.pathname === '/stop') {
      if (this.ws) this.ws.close()
      if (this.timer) clearInterval(this.timer)
      return cors('stopped')
    }
    return cors('ok')
  }

  async loadCredentials() {
    const headers = { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    const data = await getJson(`${this.env.SUPABASE_URL}/rest/v1/okx_api_credentials?id=eq.${this.apiId}&select=*`, headers)
    this.creds = data?.[0]
    if (!this.creds) {
      postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
        strategy_id: null,
        level: 'error',
        message: 'WS事件：未找到账户凭证，订阅失败',
        data: { apiId: this.apiId }
      }).catch(()=>{})
    }
  }

  async loadStrategies() {
    const headers = { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    const data = await getJson(`${this.env.SUPABASE_URL}/rest/v1/strategies?api_credential_id=eq.${this.apiId}&status=in.(running,paused)&select=id,symbol,status,profit_ratio,loss_stop_ratio,margin_mode,auto_restart_on_price_return`, headers)
    const map = new Map()
    for (const s of data || []) {
      const arr = map.get(s.symbol) || []
      arr.push(s)
      map.set(s.symbol, arr)
    }
    this.strategies = map
    this.connectPublicWS()
  }

  connectWS() {
    if (!this.creds) return
    const { api_key, api_secret, passphrase } = this.creds
    const ws = new WebSocket('wss://ws.okx.com/ws/v5/private')
    this.ws = ws
    ws.onopen = async () => {
      const ts = new Date().toISOString()
      const sign = await hmacSHA256Base64(ts + 'GET' + '/users/self/verify', api_secret)
      ws.send(JSON.stringify({ op: 'login', args: [{ apiKey: api_key, passphrase, timestamp: ts, sign }] }))
      ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'positions', instType: 'SWAP' }] }))
      console.log('private-ws-open', { apiId: this.apiId })
      postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
        strategy_id: null,
        level: 'info',
        message: 'WS事件：私有WS已连接',
        data: { apiId: this.apiId }
      }).catch(()=>{})
    }
    ws.onmessage = (ev) => this.onMessage(ev).catch(() => {})
    ws.onclose = () => setTimeout(() => this.connectWS(), 2000)
    ws.onerror = () => setTimeout(() => this.connectWS(), 2000)
  }

  connectPublicWS() {
    if (this.pub) return
    const instIds = Array.from(this.strategies.keys()).map(x => x.replace(':USDT','-SWAP').replace('/','-'))
    if (!instIds.length) return
    const ws = new WebSocket('wss://ws.okx.com/ws/v5/public')
    this.pub = ws
    ws.onopen = () => {
      const args = instIds.map(instId => ({ channel: 'candle1H', instId }))
      ws.send(JSON.stringify({ op: 'subscribe', args }))
    }
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data)
        if (msg.arg?.channel && String(msg.arg.channel).startsWith('candle')) {
          const instId = msg.arg.instId
          const sym = instId.replace('-SWAP','').replace('-', '/')+':USDT'
          const d = msg.data && msg.data[0]
          if (d && d.length >= 5) {
            const o = parseFloat(d[1])
            const c = parseFloat(d[4])
            this.candleDir.set(sym, c >= o ? 'up' : 'down')
          }
        }
      } catch (_) {}
    }
    ws.onclose = () => { this.pub = null; setTimeout(()=>this.connectPublicWS(), 2000) }
    ws.onerror = () => { this.pub = null; setTimeout(()=>this.connectPublicWS(), 2000) }
  }

  async subscribePublic(instId) {
    if (this.pub && this.pub.readyState === 1) {
      this.pub.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'candle1H', instId }] }))
      return
    }
    const ws = new WebSocket('wss://ws.okx.com/ws/v5/public')
    this.pub = ws
    ws.onopen = () => {
      ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel: 'candle1H', instId }] }))
    }
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data)
        if (msg.arg?.channel && String(msg.arg.channel).startsWith('candle')) {
          const i = msg.arg.instId
          const sym = instIdToSymbol(i)
          const d = msg.data && msg.data[0]
          if (d && d.length >= 5) {
            const o = parseFloat(d[1])
            const c = parseFloat(d[4])
            this.candleDir.set(sym, c >= o ? 'up' : 'down')
          }
        }
      } catch (_) {}
    }
    ws.onclose = () => { this.pub = null; setTimeout(()=>this.subscribePublic(instId), 2000) }
    ws.onerror = () => { this.pub = null; setTimeout(()=>this.subscribePublic(instId), 2000) }
  }

  async onMessage(ev) {
    const msg = JSON.parse(ev.data)
    if (msg.event === 'error') return
    if (msg.arg?.channel !== 'positions') return
    const rows = msg.data || []
    for (const p of rows) {
      const pos = Math.abs(parseFloat(p.pos || '0'))
      if (pos <= 0) continue
      const instId = p.instId
      const symbol = instIdToSymbol(instId)
      const list = this.strategies.get(symbol) || []
      if (!list.length) continue
      const uplRatio = parseFloat(p.uplRatio || '0')
      const posSide = p.posSide
      for (const s of list) {
        if (s.status !== 'running') continue
        const now = Date.now()
        const last = this.lastAction.get(instId) || 0
        if (now - last < 2000) continue
        const lossThresh = (()=>{ const v = Number(s.loss_stop_ratio||0); return v/100 })()
        if (lossThresh && uplRatio <= -lossThresh) {
          const dir = this.candleDir.get(symbol)
          const needOpp = (posSide === 'long' && dir === 'down') || (posSide === 'short' && dir === 'up')
          if (needOpp) {
            const size = Math.abs(parseFloat(p.pos || '0'))
            postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
              strategy_id: s.id,
              level: 'warning',
              message: 'WS事件：对冲触发条件满足，5秒后下单',
              data: { symbol, uplRatio, lossThresh, dir, posSide, size }
            }).catch(()=>{})
            setTimeout(()=>{ this.openHedge(s, symbol, posSide, size).catch(()=>{}) }, 5000)
            this.lastAction.set(instId, Date.now())
            console.log('hedge-trigger', { symbol, uplRatio, lossThresh, dir, posSide, size })
            continue
          }
        }
        const profitThresh = (()=>{ const v = Number(s.profit_ratio||0); return v/100 })()
        if (uplRatio >= profitThresh) {
          const jitter = Math.floor(Math.random() * 200)
          const size = Math.abs(parseFloat(p.pos || '0'))
          postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
            strategy_id: s.id,
            level: 'info',
            message: 'WS事件：止盈触发条件满足，0.5秒后平仓',
            data: { symbol, uplRatio, profitThresh, posSide }
          }).catch(()=>{})
          setTimeout(()=>{ this.closePosition(s, symbol, posSide).catch(()=>{}) }, 500 + jitter)
          setTimeout(()=>{
            const dir = this.candleDir.get(symbol)
            const side = dir === 'up' ? 'buy' : 'sell'
            this.openReentry(s, symbol, side, size).catch(()=>{})
          }, 7000)
          this.lastAction.set(instId, Date.now())
          console.log('take-profit-trigger', { symbol, uplRatio, profitThresh, posSide })
        }
      }
    }
  }

  async closePosition(s, symbol, posSide) {
    const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    await postJsonRetry(`${this.env.SUPABASE_URL}/functions/v1/okx-trading`, headers, {
      action: 'closePosition',
      data: { symbol, posSide, marginMode: s.margin_mode || 'isolated', credentialId: this.apiId }
    })
    await postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
      strategy_id: s.id,
      level: 'info',
      message: 'WS事件：达到止盈阈值，执行市价全平',
      data: { symbol, posSide }
    })
  }

  async openHedge(s, symbol, posSide, size) {
    const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    const side = posSide === 'long' ? 'sell' : 'buy'
    await postJsonRetry(`${this.env.SUPABASE_URL}/functions/v1/okx-trading`, headers, {
      action: 'placeOrder',
      data: { strategyId: s.id, symbol, side, orderType: 'market', size, marginMode: s.margin_mode || 'isolated', credentialId: this.apiId }
    })
    await postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
      strategy_id: s.id,
      level: 'warning',
      message: 'WS事件：达到对冲亏损阈值，开对冲仓',
      data: { symbol, hedgeSide: side, size }
    })
  }

  async openReentry(s, symbol, side, size) {
    const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    await postJsonRetry(`${this.env.SUPABASE_URL}/functions/v1/okx-trading`, headers, {
      action: 'placeOrder',
      data: { strategyId: s.id, symbol, side, orderType: 'market', size, marginMode: s.margin_mode || 'isolated', credentialId: this.apiId }
    })
    await postJsonRetry(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
      strategy_id: s.id,
      level: 'info',
      message: 'WS事件：止盈后按信号重新开仓',
      data: { symbol, side, size }
    })
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url)
    const id = env.POSITIONS_DO.idFromName(url.searchParams.get('apiId') || 'default')
    const obj = env.POSITIONS_DO.get(id)
    return obj.fetch(req)
  },
  async scheduled(event, env, ctx) {
    try {
      const ids = await listActiveApiIds(env)
      for (const apiId of ids) {
        const id = env.POSITIONS_DO.idFromName(apiId)
        const obj = env.POSITIONS_DO.get(id)
        ctx.waitUntil(obj.fetch(new Request(`https://dummy/start?apiId=${apiId}`)))
      }
    } catch (_) {}
  }
}

function cors(body, init = {}) {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    ...init.headers
  }
  return new Response(body, { ...init, headers })
}
