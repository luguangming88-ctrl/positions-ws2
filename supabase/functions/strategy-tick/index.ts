
import { createClient } from " https://esm.sh/@supabase/supabase-js@2 "

export const corsHeaders = {
"Access-Control-Allow-Origin": "*",
"Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
}

function sleep(ms: number) {
return new Promise(res => setTimeout(res, ms))
}

type StrategyRow = {
id: string
api_credential_id: string
symbol: string
status: "running" | "paused"
profit_ratio?: number
loss_stop_ratio?: number
margin_mode?: "isolated" | "cross"
signal_type?: string
range_low?: number
range_high?: number
auto_restart_on_price_return?: boolean
entry_size?: number
}

export default async (req: Request) => {
if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })

const url = Deno.env.get("SUPABASE_URL")!
const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
const supabase = createClient(url, key)

const body = await req.json().catch(() => ({}))
const apiId: string | undefined = body.apiId
const onlySymbols: string[] | undefined = body.onlySymbols

const logs = supabase.from("strategy_logs")

let q = supabase.from("strategies")
.select("id,api_credential_id,symbol,status,profit_ratio,loss_stop_ratio,margin_mode,signal_type,range_low,range_high,auto_restart_on_price_return,entry_size")
.eq("status", "running")
if (apiId) q = q.eq("api_credential_id", apiId)
if (onlySymbols?.length) q = q.in("symbol", onlySymbols)

const { data: strategies, error: stErr } = await q
if (stErr) return new Response(JSON.stringify({ error: stErr.message }), { status: 500, headers: corsHeaders })
const rows = (strategies || []) as StrategyRow[]
if (!rows.length) return new Response(JSON.stringify({ ok: true, msg: "no running strategies" }), { headers: corsHeaders })

function profitThresh(s: StrategyRow) { return Number(s.profit_ratio || 0) / 100 }
function lossThresh(s: StrategyRow) { return Number(s.loss_stop_ratio || 0) / 100 }

async function getPositions(apiCredentialId: string, instId: string) {
const { data: creds } = await supabase.from("okx_api_credentials").select("*").eq("id", apiCredentialId).limit(1)
const c = creds?.[0]
if (!c) return { data: [] as any[] }
const now = new Date().toISOString()
const path = "/api/v5/account/positions?instType=SWAP&instId=" + instId
const base = now + "GET" + path
const enc = new TextEncoder()
const secretKey = await crypto.subtle.importKey("raw", enc.encode(c.api_secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"])
const sigBuf = await crypto.subtle.sign("HMAC", secretKey, enc.encode(base))
const sign = btoa(String.fromCharCode(...new Uint8Array(sigBuf)))
const r = await fetch(" https://www.okx.com " + path, {
headers: {
"OK-ACCESS-KEY": c.api_key,
"OK-ACCESS-PASSPHRASE": c.passphrase,
"OK-ACCESS-TIMESTAMP": now,
"OK-ACCESS-SIGN": sign,
}
})
const j = await r.json().catch(() => ({ data: [] }))
return j
}

async function getCandleDir(instId: string) {
const r = await fetch( https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=1H&limit=1 )
const j = await r.json().catch(() => ({ data: [] }))
const d = j?.data?.[0]
if (!d || d.length < 5) return null
const o = parseFloat(d[1]); const c = parseFloat(d[4])
return c >= o ? "up" : "down"
}

async function placeOrder(strategy: StrategyRow, symbol: string, side: "buy" | "sell", size: number) {
await logs.insert({ strategy_id: strategy.id, level: "info", message: "策略动作：市价开仓", data: { symbol, side, size } })
const r = await fetch(url + "/functions/v1/okx-trading", {
method: "POST",
headers: { "Content-Type": "application/json", Authorization: Bearer ${key} },
body: JSON.stringify({
action: "placeOrder",
data: { strategyId: strategy.id, symbol, side, orderType: "market", size, marginMode: strategy.margin_mode || "isolated", credentialId: strategy.api_credential_id }
})
})
return await r.json().catch(() => ({}))
}

async function closePosition(strategy: StrategyRow, symbol: string, posSide: "long" | "short") {
await logs.insert({ strategy_id: strategy.id, level: "info", message: "策略动作：市价平仓", data: { symbol, posSide } })
const r = await fetch(url + "/functions/v1/okx-trading", {
method: "POST",
headers: { "Content-Type": "application/json", Authorization: Bearer ${key} },
body: JSON.stringify({
action: "closePosition",
data: { symbol, posSide, marginMode: strategy.margin_mode || "isolated", credentialId: strategy.api_credential_id }
})
})
return await r.json().catch(() => ({}))
}

for (const s of rows) {
const symbol = s.symbol
const instId = symbol.replace(":USDT", "-SWAP").replace("/", "-")
  }

return new Response(JSON.stringify({ ok: true, processed: rows.length }), { headers: corsHeaders })
}
