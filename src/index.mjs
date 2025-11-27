function instIdToSymbol(instId) {
  const base = instId.replace('-SWAP', '').replace('-', '/')
  return `${base}:USDT`
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

async function postJson(url, headers, body) {
  const r = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) })
  return await r.json()
}

export class PositionsDO {
  constructor(state, env) {
    this.state = state
    this.env = env
    this.ws = null
    this.apiId = ''
    this.creds = null
    this.strategies = new Map()
    this.lastAction = new Map()
    this.timer = null
  }

  async fetch(request) {
    const url = new URL(request.url)
    if (url.pathname === '/start') {
      this.apiId = url.searchParams.get('apiId') || ''
      ;(async () => {
        try {
          await this.loadCredentials()
          await this.loadStrategies()
          this.connectWS()
          if (this.timer) clearInterval(this.timer)
          this.timer = setInterval(() => this.loadStrategies().catch(() => {}), 60000)
        } catch (_) {}
      })()
      return new Response('started')
    }
    if (url.pathname === '/stop') {
      if (this.ws) this.ws.close()
      if (this.timer) clearInterval(this.timer)
      return new Response('stopped')
    }
    return new Response('ok')
  }

  async loadCredentials() {
    const headers = { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    const data = await getJson(`${this.env.SUPABASE_URL}/rest/v1/okx_api_credentials?id=eq.${this.apiId}&select=*`, headers)
    this.creds = data?.[0]
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
    }
    ws.onmessage = (ev) => this.onMessage(ev).catch(() => {})
    ws.onclose = () => setTimeout(() => this.connectWS(), 2000)
    ws.onerror = () => setTimeout(() => this.connectWS(), 2000)
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
        if (s.loss_stop_ratio && uplRatio <= -s.loss_stop_ratio) {
          await this.openHedge(s, symbol, posSide, Math.abs(parseFloat(p.pos || '0')))
          this.lastAction.set(instId, Date.now())
          continue
        }
        if (uplRatio >= s.profit_ratio) {
          await this.closePosition(s, symbol, posSide)
          this.lastAction.set(instId, Date.now())
        }
      }
    }
  }

  async closePosition(s, symbol, posSide) {
    const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    await postJson(`${this.env.SUPABASE_URL}/functions/v1/okx-trading`, headers, {
      action: 'closePosition',
      data: { symbol, posSide, marginMode: s.margin_mode || 'isolated', credentialId: this.apiId }
    })
    await postJson(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
      strategy_id: s.id,
      level: 'info',
      message: 'WS事件：达到止盈阈值，执行市价全平',
      data: { symbol, posSide }
    })
  }

  async openHedge(s, symbol, posSide, size) {
    const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}` }
    const side = posSide === 'long' ? 'sell' : 'buy'
    await postJson(`${this.env.SUPABASE_URL}/functions/v1/okx-trading`, headers, {
      action: 'placeOrder',
      data: { strategyId: s.id, symbol, side, orderType: 'market', size, marginMode: s.margin_mode || 'isolated', credentialId: this.apiId }
    })
    await postJson(`${this.env.SUPABASE_URL}/rest/v1/strategy_logs`, { apikey: this.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${this.env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' }, {
      strategy_id: s.id,
      level: 'warning',
      message: 'WS事件：达到对冲亏损阈值，开对冲仓',
      data: { symbol, hedgeSide: side, size }
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
  async scheduled(event, env, ctx) {}
}

