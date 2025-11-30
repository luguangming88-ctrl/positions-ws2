const dir = await getCandleDir(instId)
const { data: posResp } = await getPositions(s.api_credential_id, instId)
const posRows = Array.isArray(posResp) ? posResp : (posResp?.data || [])
const p = posRows.find((x: any) => x.instId === instId)
const pos = p ? Math.abs(parseFloat(p.pos || "0")) : 0
const uplRatio = p ? parseFloat(p.uplRatio || "0") : 0
const posSide = p?.posSide as ("long" | "short" | undefined)

const priceOk = async () => {
  if (s.range_low == null || s.range_high == null) return true
  const r = await fetch(`https://www.okx.com/api/v5/market/ticker?instId=${instId}`)
  const j = await r.json().catch(() => ({ data: [] }))
  const last = j?.data?.[0]?.last ? parseFloat(j.data[0].last) : undefined
  if (last == null) return true
  return last >= Number(s.range_low) && last <= Number(s.range_high)
}
const inRange = await priceOk()
if (!inRange) {
  await logs.insert({ strategy_id: s.id, level: "warning", message: "策略暂停：价格超区间", data: { symbol, range_low: s.range_low, range_high: s.range_high } })
  continue
}

const lt = lossThresh(s)
if (pos > 0 && lt && uplRatio <= -lt && dir && posSide) {
  const needOpp = (posSide === "long" && dir === "down") || (posSide === "short" && dir === "up")
  if (needOpp) {
    await logs.insert({ strategy_id: s.id, level: "warning", message: "对冲触发：满足亏损与信号反转", data: { symbol, uplRatio, lossThresh: lt, dir, posSide, size: pos } })
    await sleep(5000)
    const hedgeSide = posSide === "long" ? "sell" : "buy"
    await placeOrder(s, symbol, hedgeSide, pos)
    await logs.insert({ strategy_id: s.id, level: "warning", message: "已开对冲仓位", data: { symbol, hedgeSide, size: pos } })
    continue
  }
}

const pt = profitThresh(s)
if (pos > 0 && pt && uplRatio >= pt && posSide) {
  await logs.insert({ strategy_id: s.id, level: "info", message: "止盈触发：达到盈利阈值", data: { symbol, uplRatio, profitThresh: pt, posSide } })
  await sleep(2500)
  await closePosition(s, symbol, posSide)
  await sleep(7000)
  const side = (dir === "up") ? "buy" : "sell"
  const reSize = pos
  await placeOrder(s, symbol, side, reSize || Number(s.entry_size || 1))
  await logs.insert({ strategy_id: s.id, level: "info", message: "止盈后按信号重新开仓", data: { symbol, side, size: reSize || s.entry_size || 1 } })
  continue
}

if (pos <= 0) {
  await logs.insert({ strategy_id: s.id, level: "info", message: "策略A：首次或仓位为0，准备按信号开仓", data: { symbol, dir } })
  await sleep(7000)
  const side = (dir === "up") ? "buy" : "sell"
  const size = Number(s.entry_size || 1)
  await placeOrder(s, symbol, side, size)
  await logs.insert({ strategy_id: s.id, level: "info", message: "策略A：已按信号开仓", data: { symbol, side, size } })
  continue
}

await logs.insert({ strategy_id: s.id, level: "info", message: "观察：未触发A/B", data: { symbol, uplRatio, profitThresh: pt, lossThresh: lt, dir, posSide, pos } })
