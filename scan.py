#!/usr/bin/env python3
"""scan.py v1 (master prompt v7.4): EVM data layer for Robinhood Chain + Base. Prints evidence with fixed definitions and bands; Claude judges.
scan.py <addr> [quick|standard|recheck|activity] [--chain robinhood|base] [--platform 0xA,0xB] [--wallets 0xA,0xB] [--liq-floor N]  |  scan.py --bands"""
import sys, re, time, statistics as S, collections as C, concurrent.futures as cf, requests
from datetime import datetime, timezone
T0, N = time.time(), [0]
TR = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ML = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"  # V4 ModifyLiquidity
Z, DEAD = "0x" + "0" * 40, "0x" + "0" * 36 + "dead"
UA = {"User-Agent": "Mozilla/5.0 (scan)", "Accept": "application/json"}
CH = {"robinhood": dict(rpc="https://rpc.mainnet.chain.robinhood.com", cid=4663, llama="Robinhood Chain", fomo=1, span=9_000_000, bs=None,
                        stable="0x5fc5360d0400a0fd4f2af552add042d716f1d168", ref="0xa92768863a55d8A0591709f7f5E594A249d36Ea3",
                        infra={"0x267444d099b10fb5ed7c3cc7b7c767adca574952": "Pons locker", "0x8366a39cc670b4001a1121b8f6a443a643e40951": "V4 PoolManager"}),
      "base": dict(rpc="https://mainnet.base.org", cid=8453, llama="Base", fomo=0, span=2000, bs="https://base.blockscout.com",
                   stable="0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", ref=None, infra={"0x498581ff718922c3f8e6a244956af099b2652b2b": "V4 PoolManager"})}
EXCL = re.compile(r"pool|lock|router|exchange|bridge|binance|coinbase|bybit|okx|mexc|kucoin|gate\.io", re.I)
# THE KPI definitions (prompt §7 points here): value < cut[i] -> label[i], else the last label.
BANDS = {"mc_liq": ([10, 25], ["comfortable", "normal", "fragile"]), "vol_liq": ([0.2, 0.5, 2, 5], ["dead", "low", "healthy", "elevated", "churn/wash"]),
         "buy_sell": ([0.8, 1.25], ["sell skew", "balanced", "buy skew"]), "adj_top10": ([20, 35], ["good", "watch", "concentrated"]),
         "whale_exit": ([25], ["ok", "whale-exit risk"]), "trend": ([-15, 15], ["decaying", "flat", "rising"]), "chain": ([-25, 25], ["bust", "flat", "boom"])}
RULES = ["trend = per-day average last 3d vs prior 4d (rolling 24h days); chain = chain DEX volume last 7d vs prior 7d (DefiLlama)",
         "exit cap = largest sell at <=5% impact incl. fees (interpolated quotes); stressed = impact above the $1K quote doubled (-50% liquidity)",
         "adj top-10 = 10 largest holders by live balance excl. pools, lockers, burn, PoolManager, CEX/protocol tags (team/treasury/token contracts count); missing ranks filled as upper bound",
         "early stop: liq <$25K (quick <$10K) | >=90% below ATH with vol/liq <0.2 | 0 trades 24h | honeypot or sell restriction",
         "holders: 8 largest external profiled; age bins <3d 3-7d 7-14d 14-21d >21d; entry = daily close on first-buy day (ESTIMATED)"]


def band(k, v):
    if v is None: return "n/a"
    c, l = BANDS[k]; return next((l[i] for i, x in enumerate(c) if v < x), l[-1])


def get(u, t=20, h=None):
    N[0] += 1
    try: return requests.get(u, headers={**UA, **(h or {})}, timeout=t).json()
    except Exception: return None


def usd(x):
    if x is None: return "n/a"
    x = float(x); return f"${x/1e6:.2f}M" if abs(x) >= 1e6 else f"${x/1e3:.1f}K" if abs(x) >= 1e3 else f"${x:.2f}" if abs(x) >= 1 else f"${x:.6f}"


pad = lambda a: "0x" + "0" * 24 + a[2:].lower()
sh = lambda a: a[:8] + "…" + a[-4:]
pct = lambda a, b: (a / b - 1) * 100 if a is not None and b else None


class Ch:
    def __init__(s, n): s.n = n; s.__dict__.update(CH[n])

    def call(s, m, p):
        e = None
        for _ in range(3):
            N[0] += 1
            try: r = requests.post(s.rpc, json={"jsonrpc": "2.0", "id": 1, "method": m, "params": p}, headers=UA, timeout=25).json()
            except Exception as x: e = str(x)[:80]; time.sleep(1); continue
            if "error" not in r: return r["result"]
            e = str(r["error"].get("message"))[:120]
            if r["error"].get("code") != 429 and "Too Many" not in e: raise RuntimeError(e)
            time.sleep(2)
        raise RuntimeError("rpc: " + str(e))

    def batch(s, cs):
        try:
            N[0] += 1; o = [None] * len(cs)
            for x in requests.post(s.rpc, json=[{"jsonrpc": "2.0", "id": i, "method": m, "params": p} for i, (m, p) in enumerate(cs)], headers=UA, timeout=30).json(): o[x["id"]] = x.get("result")
            return o
        except Exception:
            o = []
            for m, p in cs:
                try: o.append(s.call(m, p))
                except Exception: o.append(None)
            return o

    def logs(s, a, f, t, tp, d=0):
        if t - f >= s.span:  # RPC block-range limit: chunk
            if (t - f) / s.span > 60: raise RuntimeError(f"range too large for keyless RPC ({s.span:,}-block limit)")
            o, x = [], f
            while x <= t: y = min(x + s.span - 1, t); o += s.logs(a, x, y, tp, d); x = y + 1
            return o
        try: return s.call("eth_getLogs", [{"address": a, "fromBlock": hex(f), "toBlock": hex(t), "topics": tp}])
        except RuntimeError as e:  # >10K logs: split
            if re.search("limit|too many|too large", str(e), re.I) and t - f > 50 and d < 8:
                m = (f + t) // 2; return s.logs(a, f, m, tp, d + 1) + s.logs(a, m + 1, t, tp, d + 1)
            raise

    def setup(s):
        s.L = int(s.call("eth_blockNumber", []), 16) - 20; k = min(9_000_000, s.L - 1)
        a, b = (s.call("eth_getBlockByNumber", [hex(s.L - j), False]) for j in (0, k))
        s.tl = int(a["timestamp"], 16); s.bt = (s.tl - int(b["timestamp"], 16)) / k

    def ago(s, b): return (s.L - b) * s.bt / 86400
    def ts(s, b): return s.tl - (s.L - b) * s.bt
    def back(s, days): return s.L - int(days * 86400 / s.bt)
    def bal(s, tok, ws): return [int(x, 16) if x and x != "0x" else None for x in s.batch([("eth_call", [{"to": tok, "data": "0x70a08231" + pad(w)[2:]}, "latest"]) for w in ws])]
    def codes(s, ws): return [len(x) // 2 - 1 if x else -1 for x in s.batch([("eth_getCode", [w, "latest"]) for w in ws])]
    def eth(s, ws): return [int(x, 16) / 1e18 if x else 0 for x in s.batch([("eth_getBalance", [w, "latest"]) for w in ws])]


def xfer(ch, A, w, since=0):
    """This token in/out of wallet w: ([(block, counterparty, raw)], [...], complete)."""
    w = w.lower()
    if not ch.bs:
        f = lambda tp, k: [(int(l["blockNumber"], 16), "0x" + l["topics"][k][-40:], int(l["data"], 16)) for l in ch.logs(A, max(since, ch.b0), ch.L, tp)]
        return f([TR, None, pad(w)], 1), f([TR, pad(w)], 2), True
    i, o, q = [], [], ""
    for _ in range(4):  # Base: Blockscout (keyless RPC caps logs at 2K blocks), newest first, 50 per page
        r = get(f"{ch.bs}/api/v2/addresses/{w}/token-transfers?type=ERC-20&token={A}{q}")
        if r is None or "items" not in r: raise RuntimeError("blockscout transfers")
        for x in r["items"]:
            b, v, fr, to = x["block_number"], int(x["total"]["value"]), x["from"]["hash"].lower(), x["to"]["hash"].lower()
            if b < since: return i, o, True
            if to == w: i.append((b, fr, v))
            if fr == w: o.append((b, to, v))
        if not r.get("next_page_params"): return i, o, True
        q = "&" + "&".join(f"{k}={v}" for k, v in r["next_page_params"].items())
    return i, o, False


def safe(n, f, *a):
    if time.time() - T0 > 80: return [f"UNAVAILABLE {n}: time budget"], {}
    try: return f(*a)
    except Exception as e: return [f"UNAVAILABLE {n}: {str(e)[:120]}"], {}


def tape(ch, A):
    g = f"https://api.geckoterminal.com/api/v2/networks/{ch.n}/tokens/{A}"
    with cf.ThreadPoolExecutor(3) as ex: ds, gt, gi = ex.map(get, [f"https://api.dexscreener.com/latest/dex/tokens/{A}", g + "?include=top_pools", g + "/info"])
    P = [p for p in (ds or {}).get("pairs") or [] if p.get("chainId") == ch.n and A.lower() in (p["baseToken"]["address"].lower(), p["quoteToken"]["address"].lower())]
    if not P: raise RuntimeError("not on DexScreener for this chain (too new / wrong chain)")
    f = lambda p, a, b: (p.get(a) or {}).get(b) or 0
    tx = lambda p, k: (f(p, "txns", "h24") or {}).get(k, 0)
    b = max(P, key=lambda p: f(p, "liquidity", "usd"))
    d = dict(name=b["baseToken"]["name"], sym=b["baseToken"]["symbol"], price=float(b.get("priceUsd") or 0) or None, mc=b.get("marketCap"), fdv=b.get("fdv"),
             liq=sum(f(p, "liquidity", "usd") for p in P), vol=sum(f(p, "volume", "h24") for p in P), buys=sum(tx(p, "buys") for p in P), sells=sum(tx(p, "sells") for p in P),
             chg=b.get("priceChange") or {}, boosts=sum(f(p, "boosts", "active") for p in P), created=min(p.get("pairCreatedAt") or 9e15 for p in P) / 1000,
             pools=[dict(id=p["pairAddress"], liq=f(p, "liquidity", "usd"), vol=f(p, "volume", "h24"), dex=p.get("dexId"), lab=p.get("labels") or [],
                         q=p["quoteToken"]["symbol"], c=(p.get("pairCreatedAt") or 9e15) / 1000) for p in P])
    try: a = gt["data"]["attributes"]; d["liq_gt"], d["px_gt"] = float(a.get("total_reserve_in_usd") or 0), float(a.get("price_usd") or 0)
    except Exception: d["liq_gt"] = d["px_gt"] = None
    t = [(p["attributes"].get("transactions") or {}).get("h24") or {} for p in (gt or {}).get("included", []) if p.get("type") == "pool"]
    d["ub"], d["us"] = sum(x.get("buyers", 0) for x in t), sum(x.get("sellers", 0) for x in t)
    try: h = gi["data"]["attributes"]["holders"]; d["holders"], d["dist"] = h.get("count"), h.get("distribution_percentage") or {}
    except Exception: d["holders"], d["dist"] = None, {}
    d["age"] = (time.time() - d["created"]) / 86400; c = d["chg"]
    L = [f"{d['name']} ({d['sym']}) | {ch.n} | age {d['age']:.1f}d | {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
         f"price {usd(d['price'])} | MC {usd(d['mc'])} | FDV {usd(d['fdv'])} | liq {usd(d['liq'])} (DexScreener, {len(P)} pools) | vol24 {usd(d['vol'])} | holders {d['holders']}",
         f"buys/sells 24h {d['buys']}/{d['sells']} | unique buyers/sellers (GT) {d['ub']}/{d['us']} | chg 1h/6h/24h {c.get('h1')}/{c.get('h6')}/{c.get('h24')}% | DS boosts {d['boosts']}"]
    if d["liq_gt"] and abs(d["liq"] / d["liq_gt"] - 1) > 0.25: L.append(f"  WARNING liquidity: DexScreener {usd(d['liq'])} (used) vs GeckoTerminal {usd(d['liq_gt'])}; exit quotes are the reality check")
    if d["px_gt"] and d["price"] and abs(d["px_gt"] / d["price"] - 1) > 0.15: L.append(f"  WARNING price: DexScreener {d['price']} vs GeckoTerminal {d['px_gt']}")
    L += [f"  pool {p['q']} {sh(p['id'])} [{p['dex']} {','.join(p['lab'])}] liq {usd(p['liq'])} vol {usd(p['vol'])}" for p in sorted(d["pools"], key=lambda p: -p["liq"])[:4]]
    return L, d


def history(ch, d):
    ps = [p for p in d["pools"] if p["liq"] >= 0.03 * (d["liq"] or 1)] or d["pools"]
    ps = list({p["id"]: p for p in [min(ps, key=lambda p: p["c"])] + sorted(d["pools"], key=lambda p: -p["vol"])[:2]}.values())
    with cf.ThreadPoolExecutor(3) as ex: cs = list(ex.map(lambda p: get(f"https://api.geckoterminal.com/api/v2/networks/{ch.n}/pools/{p['id']}/ohlcv/day?limit=365"), ps))
    cs = [sorted(c["data"]["attributes"]["ohlcv_list"]) for c in cs if c and c.get("data")]
    if not cs: raise RuntimeError("no OHLCV")
    vol, cl = C.Counter(), {}
    for c in cs:
        for k in c: vol[int(k[0] // 86400)] += k[5]; cl.setdefault(int(k[0] // 86400), k[4])
    ath = max((k for c in cs for k in c), key=lambda k: k[2]); c7 = cl.get(int(time.time() // 86400) - 7); d.update(ath=ath[2], close=cl, dvol=vol)
    return [f"ATH (daily high, top pools) {usd(ath[2])} on {datetime.fromtimestamp(ath[0], timezone.utc):%Y-%m-%d} -> now {pct(d['price'], ath[2]):+.0f}%" + (f" | 7d: {usd(c7)} -> now {pct(d['price'], c7):+.0f}%" if c7 else "")], {}


def mind(ch, A, d):
    with cf.ThreadPoolExecutor(3) as ex: ll, tr, fr = ex.map(get, [f"https://api.llama.fi/overview/dexs/{ch.llama}?excludeTotalDataChartBreakdown=true",
                                                                  f"https://api.geckoterminal.com/api/v2/networks/{ch.n}/trending_pools", "https://fomoradar.app/api/fresh" if ch.fomo else ""])
    L, cv, tv, td = [], {int(t // 86400): v for t, v in (ll or {}).get("totalDataChart") or []}, d.get("dvol") or {}, int(time.time() // 86400)
    if cv:
        sv = lambda k: tv.get(td - k, 0) / cv[td - k] * 100 if cv.get(td - k) else None
        now, pr = sv(1), [x for x in (sv(k) for k in range(3, 8)) if x is not None]; prv = sum(pr) / len(pr) if pr else None
        c7, p7 = sum(cv.get(td - k, 0) for k in range(1, 8)), sum(cv.get(td - k, 0) for k in range(8, 15))
        L += [f"share of chain DEX volume (top pools, full UTC days): yesterday {now or 0:.3f}% vs 3-7d ago avg {prv or 0:.3f}% ({pct(now, prv) or 0:+.0f}%)",
              f"CHAIN REGIME: {ch.llama} DEX volume 7d {usd(c7)} vs prior 7d {usd(p7)} ({pct(c7, p7) or 0:+.0f}%) -> {band('chain', pct(c7, p7))}"]
    else: L.append("UNAVAILABLE chain volume (DefiLlama)")
    rk = next((i + 1 for i, p in enumerate((tr or {}).get("data") or []) if p["relationships"]["base_token"]["data"]["id"].lower().endswith(A.lower())), None)
    fx = next((t for t in (fr or {}).get("tokens") or [] if (t.get("mint") or "").lower() == A.lower()), None)
    L.append(f"trending rank (GT, top 20): {rk or 'not listed'} | FOMO fresh 24h: " + (f"listed, {fx.get('buyers')} scored buyers, avg score {fx.get('avg_score') or 0:.0f}" if fx else "not listed" if ch.fomo else "n/a")
             + f" | unique buyers 24h {d['ub']} (7d average UNAVAILABLE) | paid boosts {d['boosts']}")
    return L, {}


def security(ch, A, d):
    c = ch.call("eth_getCode", [A, "latest"]); own, ts_, dec = ch.batch([("eth_call", [{"to": A, "data": x}, "latest"]) for x in ("0x8da5cb5b", "0x18160ddd", "0x313ce567")])
    dec = int(dec, 16) if dec and dec != "0x" else 18; sup = int(ts_, 16) / 10 ** dec if ts_ and ts_ != "0x" else None
    own = "none/renounced" if not (own and own != "0x" and int(own, 16)) else "0x" + own[-40:]
    hint = [k for k, v in {"mint": "40c10f19", "pause": "8456cb59", "upgradeTo": "3659cfe6", "blacklist": "f9f92be4"}.items() if v in c]
    L = [f"bytecode {len(c)//2-1} B | owner() {own} | supply {sup or 0:,.0f} | power selectors (hint): {', '.join(hint) or 'none'}"]
    if len(c) < 400: L.append("  TINY BYTECODE = proxy/clone: powers live in the implementation; read it manually")
    if ch.ref:
        n = lambda x: re.sub(r"7f[0-9a-f]{64}|73[0-9a-f]{40}", "", x)
        L.append("  Pons template vs ASKR: " + ("IDENTICAL (embedded addresses stripped)" if n(c) == n(ch.call("eth_getCode", [ch.ref, "latest"])) else "DIFFERENT -> read functions manually"))
    g = ((get(f"https://api.gopluslabs.io/api/v1/token_security/{ch.cid}?contract_addresses={A}") or {}).get("result") or {}).get(A.lower()) or {}
    k = ["is_honeypot", "cannot_sell_all", "buy_tax", "sell_tax", "is_mintable", "is_proxy", "hidden_owner", "can_take_back_ownership", "owner_change_balance", "transfer_pausable", "is_blacklisted", "slippage_modifiable", "is_open_source"]
    L.append("  GoPlus: " + (" ".join(f"{x}={g.get(x)}" for x in k if g.get(x) not in (None, "")) if g else "UNAVAILABLE"))
    if ch.n == "base":
        h = get(f"https://api.honeypot.is/v2/IsHoneypot?address={A}&chainID=8453") or {}
        L.append("  honeypot.is: " + (f"honeypot={h['honeypotResult'].get('isHoneypot')} buyTax={h['simulationResult'].get('buyTax')} sellTax={h['simulationResult'].get('sellTax')}" if h.get("simulationResult") else "UNAVAILABLE"))
    d.update(dec=dec, sup=sup or 1e9, gp=g)
    return L, {}


def launch(ch, A, d):
    dec, sup, zt = d["dec"], d["sup"], "0x" + "0" * 64; mint = []
    if ch.span > 1e6:
        try: mint = ch.logs(A, 0, ch.L, [TR, zt])
        except Exception: pass
    if not mint:
        e = ch.L - int((time.time() - d["created"]) / ch.bt); mint = ch.logs(A, max(e - int(7200 / ch.bt), 0), min(e + int(3600 / ch.bt), ch.L), [TR, zt])
    if not mint: raise RuntimeError("mint not found near pool creation")
    k = lambda l: (int(l["blockNumber"], 16), int(l["logIndex"], 16)); mint.sort(key=k); mb = int(mint[0]["blockNumber"], 16)
    cr = ch.call("eth_getTransactionByHash", [mint[0]["transactionHash"]])["from"].lower(); lg, a, w = [], mb, min(20000, ch.span)
    for _ in range(5):
        if a > ch.L or len(lg) >= 500: break
        lg += ch.logs(A, a, min(a + w - 1, ch.L), [TR]); a += w
    P = [(int(l["blockNumber"], 16), "0x" + l["topics"][1][-40:], "0x" + l["topics"][2][-40:], int(l["data"], 16) / 10 ** dec, l["transactionHash"]) for l in sorted([l for l in lg if len(l["topics"]) == 3], key=k)[:600]]
    ti, to = C.defaultdict(set), C.defaultdict(set)
    for b, f, t, v, h in P: ti[h].add(t); to[h].add(f)
    Sx = {x for h in ti for x in ti[h] & to[h]} | {"0x" + mint[0]["topics"][2][-40:], Z, DEAD} | {p["id"].lower() for p in d["pools"]} | set(ch.infra)
    fi = C.OrderedDict()
    for b, f, t, v, h in P:
        if f in Sx and t not in Sx and t not in fi: fi[t] = [b, 0]
        if t in fi and f in Sx: fi[t][1] += v
    fi = list(fi.items())[:20]; ws = [w for w, _ in fi] + [cr]; now = {w: (x or 0) / 10 ** dec for w, x in zip(ws, ch.bal(A, ws))}
    R = [dict(w=w, s=(b - mb) * ch.bt, l=v / sup * 100, n=now[w] / sup * 100) for w, (b, v) in fi]
    dev = next((r for r in R if r["w"] == cr), None); fast = [r for r in R if r["s"] <= 3 and r["w"] != cr]
    L = [f"mint {ch.ago(mb):.1f}d ago (block {mb}) | deployer (tx.from) {cr}",
         f"  dev buy {dev['l']:.2f}% -> now {dev['n']:.2f}%" if dev else f"  no dev buy among first buyers; deployer holds {now[cr]/sup*100:.2f}%",
         f"  first {len(R)} buyers: launch {sum(r['l'] for r in R):.1f}% -> now {sum(r['n'] for r in R):.1f}% | fully out {sum(r['n'] < 0.05 * r['l'] for r in R)}/{len(R)} | within 3s of mint: {len(fast)} ({sum(r['l'] for r in fast):.1f}%)",
         "  funding/cluster tracing NOT done -> visibility B at best"]
    bg = C.defaultdict(list)
    for b, f, t, v, h in P:
        if f in Sx and t not in Sx and t != cr: bg[b].append((t, v))
    for b, lst in sorted(bg.items(), key=lambda x: -sum(v for _, v in x[1]))[:6]:
        m = S.median(v for _, v in lst); sim = {t for t, v in lst if 0.75 * m <= v <= 1.25 * m}
        if len(sim) >= 3: L.append(f"  SAME-BLOCK SIMILAR-SIZE GROUP +{(b-mb)*ch.bt:.1f}s: {len(sim)} wallets ~{m/sup*100:.2f}% each -> {sum(v for t, v in lst if t in sim)/sup*100:.1f}% at launch, {sum(x or 0 for x in ch.bal(A, list(sim)))/10**dec/sup*100:.1f}% now (LIKELY COORDINATED)")
    i, o, ok = xfer(ch, A, cr, mb); lab = {DEAD: "BURN", Z: "BURN", **ch.infra}; ct = C.Counter()
    for _, x, v in o: ct[lab.get(x, sh(x))] += v / 10 ** dec
    L.append(f"CREATOR {sh(cr)}: {'contract' if ch.codes([cr])[0] > 23 else 'wallet'} | ETH {ch.eth([cr])[0]:.2f} | holds {now[cr]/sup*100:.2f}% | in {len(i)} / out {len(o)} txs{'' if ok else ' (latest 200 only)'} | outflows: " + ("; ".join(f"{a} {v/sup*100:.2f}%" for a, v in ct.most_common(3)) or "none"))
    bu = []
    for t in (DEAD, Z):
        try: bu += xfer(ch, A, t, mb)[0]
        except Exception: pass
    if not bu: L.append("BURNS: none found"); return L, {}
    dd, src = C.Counter(), C.Counter()
    for b, f, v in bu: v /= 10 ** dec; dd[int(ch.ago(b))] += v; src[f] += v
    l3, p4 = sum(dd[j] for j in range(3)) / 3, sum(dd[j] for j in range(3, 7)) / 4; tp = [x for x in src.most_common(3) if x[1] / sup >= 1e-4]; ee = ch.eth([w for w, _ in tp])
    L.append(f"BURNS: total {sum(dd.values())/sup*100:.2f}% ({len(bu)} txs) | per day last 3d {l3/1e6:.2f}M vs prior 4d {p4/1e6:.2f}M ({pct(l3, p4) or 0:+.0f}% -> {band('trend', pct(l3, p4))})"
             + (f" | ~{usd(l3*d['price'])}/day = {l3*d['price']*365/d['mc']*100:.0f}% of MC/yr" if d.get("price") and d.get("mc") else ""))
    L.append("  burn sources: " + "; ".join(f"{sh(w)}{' (creator)' if w == cr else ''} {v/sup*100:.2f}% (ETH {e:.2f})" for (w, v), e in zip(tp, ee)) + " | who funds them: UNVERIFIED by script")
    return L, {}


def cands(ch, A, d, fomo):
    ex = set(ch.infra) | {Z, DEAD} | {p["id"].lower() for p in d["pools"]}
    tag = {h["address"].lower(): (h.get("tag") or "") + (" locked" if h.get("is_locked") else "") for h in d["gp"].get("holders") or []}
    if ch.bs:
        for x in ((get(f"{ch.bs}/api/v2/tokens/{A}/holders") or {}).get("items") or [])[:25]: tag.setdefault(x["address"]["hash"].lower(), x["address"].get("name") or "")
    fh = {h["address"].lower(): h for h in (fomo or {}).get("holders", []) if h.get("address")}
    ws = [w for w in dict.fromkeys(list(tag) + list(fh)) if w not in ex and not EXCL.search(tag.get(w, ""))]; sup = d["sup"]
    R = sorted([dict(w=w, h=fh.get(w, {}).get("handle") or ("token contract" if w == A.lower() else tag.get(w) or sh(w)), sc=fh.get(w, {}).get("score"), p=(b or 0) / 10 ** d["dec"] / sup * 100)
                for w, b in zip(ws, ch.bal(A, ws))], key=lambda r: -r["p"])
    inf = dict(zip(list(ch.infra) + [DEAD], [(x or 0) / 10 ** d["dec"] / sup * 100 for x in ch.bal(A, list(ch.infra) + [DEAD])]))
    g10 = float(d["dist"].get("top_10", 0) or 0); big = [v for v in inf.values() if v >= 0.5]
    gest = g10 - sum(big) + len(big) * float(d["dist"].get("11_30", 0) or 0) / 20 if g10 else None
    gx = sorted((r["p"] for r in R if r["w"] in tag), reverse=True)[:10]  # GoPlus lists only the top 10 incl. infra: fill missing ranks with its smallest external (upper bound)
    adj = sum(gx) + (10 - len(gx)) * gx[-1] if gx and not ch.bs and len(gx) < 10 else sum(r["p"] for r in R[:10]) if tag else gest
    d.update(R=R, exited=[f"{h['handle']} ({h['score']})" for w, h in fh.items() if not any(r["w"] == w and r["p"] > 0.05 for r in R)], adj=adj)
    return ["INFRA: " + " | ".join(f"{ch.infra.get(w, 'burn')} {v:.2f}%" for w, v in inf.items()),
            f"ADJ TOP-10 {d['adj'] or 0:.1f}% -> {band('adj_top10', d['adj'])} ({'live balances' + (', ranks beyond the GoPlus top-10 filled as upper bound' if not ch.bs and len(gx) < 10 else '') if tag else 'GeckoTerminal distribution estimate'}; ESTIMATED) | largest: "
            + ", ".join(f"{r['h'][:16]} {r['p']:.1f}%" for r in R[:5])], {}


def holders(ch, A, d, fomo):
    R, sup, dec, w7 = [r for r in d.get("R", []) if r["p"] > 0.05][:8], d["sup"], d["dec"], ch.back(7)
    def one(r):
        i, o, ok = xfer(ch, A, r["w"]); v = lambda x: x[2] / 10 ** dec
        r.update(rec=sum(map(v, i)), sent=sum(map(v, o)), f=min((x[0] for x in i), default=None), ls=max((x[0] for x in o), default=None), ok=ok,
                 i7=sum(v(x) for x in i if x[0] >= w7) / sup * 100, o7=sum(v(x) for x in o if x[0] >= w7) / sup * 100)
        r["prof"] = get(f"https://fomoradar.app/api/trader/{r['w']}", 25) if ch.fomo else None; return r
    with cf.ThreadPoolExecutor(3) as ex: R = list(ex.map(one, R))
    L = ["handle (score) | % supply | held | sold % (last sell) | 7d in/out % supply | est. entry -> multiple | style, win rate, best wins"]
    cov = sum(r["p"] for r in R) or 1; mix, rot, prof, mult = C.Counter(), C.Counter(), 0, []
    for r in R:
        h = ch.ago(r["f"]) if r["f"] else None; sold = r["sent"] / r["rec"] * 100 if r["rec"] else 0
        tg = " ADDING" if r["i7"] >= 0.02 and r["i7"] > 2 * r["o7"] else " TRIMMING" if r["o7"] >= 0.02 and r["o7"] > 2 * r["i7"] else ""
        if h is not None: mix[next(b for b, x in (("<3d", 3), ("3-7d", 7), ("7-14d", 14), ("14-21d", 21), (">21d", 1e9)) if h < x)] += r["p"]
        ent = d.get("close", {}).get(int(ch.ts(r["f"]) // 86400)) if r["f"] else None
        if ent: m = d["price"] / ent; mult.append(m); prof += r["p"] if m > 1 else 0
        p, st = (r["prof"] if isinstance(r["prof"], dict) and r["prof"].get("handle") else None), "profile n/a"
        if p:
            pos = p.get("positions") or []; wn = sorted([x for x in pos if (x.get("pnl") or 0) > 0], key=lambda x: -x["pnl"])[:2]; wr = (p.get("stats") or {}).get("win_rate")
            st = "/".join(p.get("style") or []) + (f", win {wr*100:.0f}%" if wr is not None else "") + ", wins: " + ", ".join(f"{x['sym']} +{usd(x['pnl'])}" for x in wn)
            for x in pos:
                if x.get("state") in ("held", "trimmed", "open") and (x.get("token") or "").lower() != A.lower(): rot[x.get("sym")] += 1
        L.append(f"  {r['h'][:16]} ({r['sc'] or '-'}) | {r['p']:.2f}% | {'' if r['ok'] else '>='}{f'{h:.1f}d' if h is not None else 'n/a'} | {sold:.0f}%" + (f" ({ch.ago(r['ls']):.1f}d ago)" if r["ls"] else " (never)")
                 + f" | +{r['i7']:.2f}/-{r['o7']:.2f}{tg} | " + (f"~{usd(ent)} -> {d['price']/ent:.1f}x" if ent else "entry n/a") + f" | {st}")
    L.append(f"  covered {cov:.1f}% | never-sold {sum(r['p'] for r in R if not r['sent'])/cov*100:.0f}% | age mix " + ", ".join(f"{b} {mix[b]/cov*100:.0f}%" for b in ("<3d", "3-7d", "7-14d", "14-21d", ">21d") if mix[b])
             + (f" | in profit {prof/cov*100:.0f}% (median {S.median(mult):.1f}x)" if mult else " | profit overhang n/a (no entry prices)"))
    if d.get("exited"): L.append("  EXITED/NOT HOLDING scored wallets on FOMO list: " + ", ".join(d["exited"][:6]))
    if rot: L.append("  ROTATION (other open positions of profiled wallets): " + ", ".join(f"{k} x{v}" for k, v in rot.most_common(5)))
    if fomo:
        fl, th = fomo.get("flow") or [], (fomo.get("theses") or [{}])[0]; fs = lambda s: usd(sum(x["usd"] for x in fl if x.get("side") == s))
        L.append(f"  scored cohort: {fomo.get('trusted_holders')} holders, avg score {fomo.get('avg_score') or 0:.0f}, value {usd(fomo.get('cohort_value'))} | flow last {fomo.get('hours')}h: buys {fs('buy')} vs sells {fs('sell')} | seeded {(fomo.get('seeded') or {}).get('wallets')}")
        if th.get("text"): L.append(f"  holder thesis (HOLDER-REPORTED, {th.get('handle')}): \"{th['text'][:100]}\"")
    d["prof"] = R
    return L, {}


def exitq(ch, A, d):
    w = d["R"][0]["p"] / 100 * d["sup"] * d["price"] if d.get("R") else None
    def q(u):
        try: r = get(f"https://aggregator-api.kyberswap.com/{ch.n}/api/v1/routes?tokenIn={A}&tokenOut={ch.stable}&amountIn={int(u/d['price']*10**d['dec'])}", h={"x-client-id": "scan"})["data"]["routeSummary"]; return u, (1 - float(r["amountOutUsd"]) / float(r["amountInUsd"])) * 100
        except Exception: return u, None
    with cf.ThreadPoolExecutor(3) as ex: R = sorted(ex.map(q, sorted({1000, 2500, 5000, 10000, 15000, 25000, 50000} | ({round(w)} if w else set()))))
    ok = [(u, i) for u, i in R if i is not None]
    if not ok: raise RuntimeError("no aggregator quotes")
    b0 = ok[0][1]
    def cap(lim):
        best = 0
        for (u1, i1), (u2, i2) in zip(ok, ok[1:] + [(None, None)]):
            if i1 > lim: break
            best = u1 if u2 is None or i2 <= lim else u1 + (u2 - u1) * (lim - i1) / (i2 - i1)
            if u2 and i2 > lim: break
        return best
    wi = next((i for u, i in R if w and u == round(w)), None); d["cap"] = cap(5)
    return ["ladder (sell to stable, incl. fees): " + " | ".join(f"{usd(u)} {'n/a' if i is None else f'{i:.1f}%'}" for u, i in R),
            f"  EXIT CAP <=5%: {usd(d['cap'])} ({d['cap']/d['mc']*100 if d.get('mc') else 0:.2f}% of MC) | stressed (-50% liq): {usd(cap((5 + b0) / 2))}"
            + (f" | largest holder bag {usd(w)}: {wi:.1f}% -> {band('whale_exit', wi)}" if wi is not None else "")], {}


def lp(ch, d):
    pm, tot = next((k for k, v in ch.infra.items() if "PoolManager" in v), None), d["liq"] or 1
    L = ["LP per pool >10% of liquidity (V4 churn 7d = managed/removable, not a lock proof; ESTIMATED):"]
    L += [f"  GoPlus LP holder {sh(h.get('address', ''))} {float(h.get('percent') or 0)*100:.0f}% tag={h.get('tag')} locked={h.get('is_locked')}" for h in (d["gp"].get("lp_holders") or [])[:3]]
    for p in [p for p in sorted(d["pools"], key=lambda p: -p["liq"]) if p["liq"] >= 0.1 * tot][:4]:
        t = f"  {p['q']} {sh(p['id'])} {usd(p['liq'])} ({p['liq']/tot*100:.0f}%)"
        if "pons" in str(p["dex"]): L.append(t + ": Pons pool, LP in Pons locker by template"); continue
        if "v4" not in p["lab"] or len(p["id"]) != 66 or ch.span < 1e6: L.append(t + ": owner/churn UNVERIFIED -> treat as removable in stressed exit"); continue
        a = r = na = nr = 0; s = set()
        for l in ch.logs(pm, ch.back(7), ch.L, [ML, p["id"]]):
            x = l["data"][2:]; v = int(x[128:192], 16); v = v - (1 << 256) if v >= 1 << 255 else v; s.add(x[192:256])
            if v > 0: a += v; na += 1
            elif v < 0: r -= v; nr += 1
        L.append(t + f": {na} adds/{nr} removes, removed {r/a*100 if a else 0:.0f}% of added, {len(s)} positions -> " + ("ACTIVELY MANAGED/REMOVABLE" if na and nr >= 0.3 * na and r >= 0.5 * a else "low churn, owner UNVERIFIED"))
    return L, {}


def platform(ch, addrs, d):
    L = []
    for a in addrs:
        if ch.bs:  # Base: newest 250 logs via Blockscout
            lg, q = [], ""
            for _ in range(5):
                r = get(f"{ch.bs}/api/v2/addresses/{a}/logs{q}") or {}; lg += [(x["topics"][0], (time.time() - datetime.fromisoformat(x["block_timestamp"].replace("Z", "+00:00")).timestamp()) / 86400) for x in r.get("items", []) if x.get("topics")]
                if not r.get("next_page_params"): break
                q = "?" + "&".join(f"{k}={v}" for k, v in r["next_page_params"].items())
        else: lg = [(l["topics"][0], ch.ago(int(l["blockNumber"], 16))) for l in ch.logs(a, ch.back(8), ch.L, []) if l["topics"]]
        top = C.Counter(t for t, _ in lg).most_common(1)[0][0] if lg else None; dd = C.Counter(int(x) for t, x in lg if t == top)
        l3, p4 = sum(dd[j] for j in range(3)) / 3, sum(dd[j] for j in range(3, 7)) / 4
        L.append(f"CORE ACTIVITY {sh(a)} main event {top[:10] if top else '-'}: per day last 3d {l3:.1f} vs prior 4d {p4:.1f} ({pct(l3, p4) or 0:+.0f}% -> {band('trend', pct(l3, p4))}) | days 0-6 (0 = last 24h): " + ",".join(str(dd[j]) for j in range(7))
                 + (f" | covers {max(x for _, x in lg):.1f}d only" if lg and max(x for _, x in lg) < 7 else ""))
    return L, {}


def recheck(ch, A, d, ws, floor):
    s, sc = ch.back(1), d["dec"]; L = [f"liquidity {usd(d['liq'])} vs floor {usd(floor)}: {'BREACH' if d['liq'] < floor else 'ok'}"] if floor else []
    for w in ws:
        n = ch.bal(A, [w])[0] or 0; i, o, _ = xfer(ch, A, w, s); i, o = sum(x[2] for x in i), sum(x[2] for x in o); b = n - i + o
        L.append(f"wallet {sh(w)}: {n/10**sc/d['sup']*100:.2f}% | 24h sold {o/b*100 if b > 0 else 0:.0f}% of bag" + (" ALERT >50%" if b > 0 and o / b > 0.5 else ""))
    bl = xfer(ch, A, DEAD, s)[0]; L.append(f"burns last 24h: {len(bl)} txs, {sum(x[2] for x in bl)/10**sc/d['sup']*100:.3f}% of supply")
    return L, {}


def kpis(d):
    r = lambda a, b: a / b if a is not None and b else None; ml, vl, bs = r(d.get("mc"), d["liq"]), r(d["vol"], d["liq"]), r(d["buys"], d["sells"])
    st = [x for x, y in (("liq <$25K", d["liq"] < 25000), ("0 trades 24h", d["buys"] + d["sells"] == 0), ("honeypot/sell restriction", "1" in (str(d["gp"].get("is_honeypot")), str(d["gp"].get("cannot_sell_all")))),
                         (">=90% below ATH + vol/liq <0.2", d.get("ath") and d["price"] < 0.1 * d["ath"] and (vl or 0) < 0.2)) if y]
    kw = [f"{x['h']}:{x['w']}" for x in sorted([x for x in d.get("prof", []) if x.get("sc")], key=lambda x: -x["p"])[:3]]
    return [f"MC/Liq {ml or 0:.1f}x {band('mc_liq', ml)} | Vol/Liq {vl or 0:.2f}x {band('vol_liq', vl)} | Vol/MC {(r(d['vol'], d.get('mc')) or 0)*100:.0f}%/day | buys:sells {bs or 0:.2f} {band('buy_sell', bs)}",
            "holder growth 7d/28d: UNAVAILABLE (no keyless history)", "EARLY STOP: " + (", ".join(st) if st else "none (quick-scan floor: liq <$10K)"),
            f"CSV: ath_usd {d.get('ath')} | key_wallets {';'.join(kw) or 'n/a'} | liq floor (-50%) {round(d['liq']/2)}"]


def main():
    a = sys.argv[1:]
    if "--bands" in a: [print(f"{k}: cuts {c} -> {l}") for k, (c, l) in BANDS.items()]; [print(x) for x in RULES]; return
    if not a or not a[0].startswith("0x"): print("Not an EVM address (Solana: manual)."); return
    A = a[0]; mode = next((x for x in a[1:] if x in ("quick", "standard", "recheck", "activity")), "standard")
    o = lambda k: next((a[i + 1] for i, x in enumerate(a) if x == k and i + 1 < len(a)), None); lst = lambda k: [x for x in (o(k) or "").split(",") if x]
    chain = o("--chain") or next((c for c in CH if get(f"https://api.dexscreener.com/tokens/v1/{c}/{A}")), None)
    if chain not in CH: print("Not found on Robinhood Chain/Base via DexScreener; pass --chain or scan manually."); return
    ch, out = Ch(chain), lambda h, L: (print(f"== {h} =="), [print(x) for x in L])
    if mode == "activity": ch.setup(); out("CORE ACTIVITY", safe("activity", platform, ch, lst("--platform"), {})[0]); return
    with cf.ThreadPoolExecutor(3) as ex:
        ft, fs = ex.submit(safe, "tape", tape, ch, A), ex.submit(ch.setup)
        ff = ex.submit(get, f"https://fomoradar.app/api/token/{A}", 25) if ch.fomo and mode != "recheck" else None
        tl, d = ft.result()
        try: fs.result()
        except Exception as e: print("UNAVAILABLE rpc:", e); return
    print(f"SCAN {mode.upper()} | {A} | {chain} | tier A"); out("TAPE", tl)
    if not d: return
    ch.b0 = max(ch.L - int((time.time() - d["created"] + 86400) / ch.bt), 0)
    out("CONTRACT", safe("contract", security, ch, A, d)[0])
    if "dec" not in d: d.update(dec=18, sup=(d["mc"] or 1e9 * d["price"]) / d["price"], gp={})
    if mode == "recheck": out("RECHECK", safe("recheck", recheck, ch, A, d, [w.lower() for w in lst("--wallets")], float(o("--liq-floor") or 0))[0]); print(f"[{time.time()-T0:.0f}s, {N[0]} requests]"); return
    fomo = ff.result() if ff else None; fomo = fomo if isinstance(fomo, dict) and fomo.get("holders") is not None else None
    with cf.ThreadPoolExecutor(4) as ex: res = [f.result()[0] for f in [ex.submit(safe, "history", history, ch, d), ex.submit(safe, "holders", cands, ch, A, d, fomo), ex.submit(safe, "lp", lp, ch, d), ex.submit(safe, "launch", launch, ch, A, d)]]
    jobs = [("EXIT", exitq, (ch, A, d))] + ([("MINDSHARE", mind, (ch, A, d)), ("WHO HOLDS IT", holders, (ch, A, d, fomo))] if mode == "standard" else []) + ([("CORE ACTIVITY", platform, (ch, lst("--platform"), d))] if lst("--platform") else [])
    with cf.ThreadPoolExecutor(4) as ex: R2 = [(h, ex.submit(safe, h.lower(), f, *x)) for h, f, x in jobs]
    for h, x in zip(("PRICE HISTORY", "OWNERSHIP", "LP", "LAUNCH / CREATOR / BURNS"), res): out(h, x)
    for h, f in R2: out(h, f.result()[0])
    out("KPIS (bands: scan.py --bands)", kpis(d)); print(f"[{time.time()-T0:.0f}s, {N[0]} requests; Solana, funding/cluster tracing and web claims stay manual]")


if __name__ == "__main__": main()
