"""
RAPTOR SIGNAL v2.2 — Data Fetcher
Fix v2.2:
  - VSTOXX: cascata 3 ticker ^V2TX → V2TX.DE → VIXEF.PA
  - P/C Ratios: CSV CBOE + parser robusto
  - HYG/IEI: sanity check dinamico Δ>15% vs ieri
  - Yield curve: ^IRX già in % — NO /100
  - Cu/Au: HG=F / GC=F futures diretti
"""
import json, os, urllib.request, csv, io, re
from datetime import datetime, timezone

DATA_FILE   = "data/signal.json"
MAX_HISTORY = 504

def fetch_yahoo(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        return round(closes[-1], 4) if closes else None
    except Exception as e:
        print(f"  Yahoo [{ticker}]: {e}")
        return None

def fetch_yahoo_cascade(*tickers):
    for t in tickers:
        v = fetch_yahoo(t)
        if v is not None:
            print(f"  ✓ {t} = {v}")
            return v
        print(f"  ✗ {t} N/D")
    return None

def fetch_cboe_putcall():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urls = [
        f"https://www.cboe.com/us/options/market_statistics/daily/?mkt=cone&dt={today}",
        "https://www.cboe.com/us/options/market_statistics/daily/",
        "https://cdn.cboe.com/api/global/us_options_volume/daily-options.csv",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                content = r.read().decode("utf-8", errors="ignore")
            result = _parse_cboe(content)
            if any(v is not None for v in result.values()):
                print(f"  ✓ CBOE: {result}")
                return result
        except Exception as e:
            print(f"  CBOE [{url[:45]}]: {e}")
    print("  ⚠ CBOE: tutti i metodi falliti")
    return {"pc_equity": None, "pc_total": None, "pc_index": None}

def _parse_cboe(content):
    result = {"pc_equity": None, "pc_total": None, "pc_index": None}
    patterns = {
        "pc_equity": [r'equity[^0-9\n]{0,30}([0-9]+\.[0-9]+)', r'EQUITY[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
        "pc_total":  [r'total[^0-9\n]{0,30}([0-9]+\.[0-9]+)',  r'TOTAL[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
        "pc_index":  [r'index[^0-9\n]{0,30}([0-9]+\.[0-9]+)',  r'INDEX[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
    }
    for key, pats in patterns.items():
        for pat in pats:
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                if 0.3 <= val <= 2.5:
                    result[key] = val
                    break
    return result

def percentile_rank(series, value, window=252):
    data = [x for x in series[-window:] if x is not None]
    if len(data) < 20 or value is None: return None
    return round(sum(1 for v in data if v <= value) / len(data) * 100, 1)

def ma(series, n):
    vals = [x for x in series[-n:] if x is not None]
    return round(sum(vals) / len(vals), 4) if vals else None

def signal_from_percentile(pct, inverted=False):
    if pct is None: return "N/D"
    if inverted:
        if pct >= 80: return "FEAR"
        if pct <= 20: return "GREED"
        return "NEUTRAL"
    else:
        if pct >= 80: return "RISK-ON"
        if pct <= 20: return "RISK-OFF"
        return "NEUTRAL"

def compute_projection(signals, ratios):
    def score(sig, w):
        m = {"RISK-ON":1,"GREED":1,"NEUTRAL":0,"FEAR":-1,"RISK-OFF":-1,"TAIL RISK":-2,"COMPLACENCY":0.5,"N/D":0}
        return m.get(sig, 0) * w
    s  = (score(signals.get("vix",{}).get("signal","N/D"),2.5)
        + score(signals.get("pc_equity",{}).get("signal","N/D"),2.0)
        + score(signals.get("pc_total",{}).get("signal","N/D"),1.5)
        + score(signals.get("skew",{}).get("signal","N/D"),1.0)
        + score(signals.get("vstoxx",{}).get("signal","N/D"),1.0))
    m2 = (score(ratios.get("hyg_iei",{}).get("signal","N/D"),2.0)
        + score(ratios.get("eem_spy",{}).get("signal","N/D"),1.5)
        + score(ratios.get("copper_gold",{}).get("signal","N/D"),2.0)
        + score(ratios.get("tip_ief",{}).get("signal","N/D"),1.5)
        + score(ratios.get("eurusd",{}).get("signal","N/D"),1.0))
    l  = (score(ratios.get("yield_curve",{}).get("signal","N/D"),3.0)
        + score(ratios.get("stoxx_spy",{}).get("signal","N/D"),1.5)
        + score(ratios.get("btp_bund",{}).get("signal","N/D"),2.0))
    def norm(v, mx): return round((v/mx)*100,1) if mx else 0
    def regime(n):
        if n>=30: return "RISK-ON"
        if n>=5:  return "POSITIVO"
        if n>=-5: return "NEUTRO"
        if n>=-30:return "CAUTO"
        return "RISK-OFF"
    def probs(n):
        bp=min(max((n+100)/200,0.05),0.95); bn=min(max((-n+100)/200,0.05),0.95)
        ro=round(bp*0.6*100,0); roff=round(bn*0.4*100,0)
        return {"risk_on":int(ro),"neutro":int(max(0,100-ro-roff)),"risk_off":int(roff)}
    sn=norm(s,8.0); mn=norm(m2,8.0); ln=norm(l,6.5)
    return {
        "breve":{"orizzonte":"2-4 settimane","score":sn,"regime":regime(sn),"probabilita":probs(sn),"driver":"VIX · P/C Ratio · SKEW · VSTOXX"},
        "medio":{"orizzonte":"1-3 mesi","score":mn,"regime":regime(mn),"probabilita":probs(mn),"driver":"HYG/IEI · Copper/Gold · EEM/SPY · EUR/USD"},
        "lungo":{"orizzonte":"6-18 mesi","score":ln,"regime":regime(ln),"probabilita":probs(ln),"driver":"Yield Curve · BTP/Bund · STOXX/SPY"},
    }

def main():
    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%Y-%m-%d")
    ts    = now_utc.strftime("%Y-%m-%dT%H:%M")
    existing = {}
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f: existing = json.load(f)
    history = existing.get("history", [])

    print(f"\n{'='*55}\nRAPTOR SIGNAL v2.2 — {ts} UTC\n{'='*55}")

    print("\n[1/4] Sentiment...")
    cboe   = fetch_cboe_putcall()
    vix    = fetch_yahoo("^VIX")
    skew   = fetch_yahoo("^SKEW")
    print("  VSTOXX cascata...")
    vstoxx = fetch_yahoo_cascade("^V2TX", "V2TX.DE", "VIXEF.PA")
    stoxx50= fetch_yahoo("^STOXX50E")

    print("\n[2/4] Intermarket...")
    hyg    = fetch_yahoo("HYG")
    iei    = fetch_yahoo("IEI")
    eem    = fetch_yahoo("EEM")
    spy    = fetch_yahoo("SPY")
    tip    = fetch_yahoo("TIP")
    ief    = fetch_yahoo("IEF")
    dxy    = fetch_yahoo("DX-Y.NYB")
    eurusd = fetch_yahoo("EURUSD=X")
    tnx    = fetch_yahoo("^TNX")
    ust2   = fetch_yahoo("^IRX")
    yc_spread = round(tnx - ust2, 3) if tnx and ust2 else None
    print(f"  ✓ YC: {tnx}% - {ust2}% = {yc_spread}%")
    copper_fut = fetch_yahoo("HG=F")
    gold_fut   = fetch_yahoo("GC=F")
    r_copper_gold = round(copper_fut/gold_fut, 6) if copper_fut and gold_fut else None
    print(f"  ✓ Cu/Au: {copper_fut}/{gold_fut} = {r_copper_gold}")

    def ratio(a, b):
        if a and b and b != 0: return round(a/b, 6)
        return None

    r_hyg_iei   = ratio(hyg, iei)
    r_eem_spy   = ratio(eem, spy)
    r_tip_ief   = ratio(tip, ief)
    r_stoxx_spy = ratio(stoxx50, spy)

    # Sanity check HYG/IEI dinamico
    if r_hyg_iei is not None:
        prev = next((r.get("hyg_iei") for r in reversed(history)
                     if r.get("hyg_iei") is not None and r.get("date") != today), None)
        if prev and abs(r_hyg_iei - prev) / prev > 0.15:
            print(f"  ⚠ HYG/IEI={r_hyg_iei} Δ{((r_hyg_iei-prev)/prev*100):.1f}% vs {prev} — scartato")
            r_hyg_iei = None
        else:
            print(f"  ✓ HYG/IEI={r_hyg_iei} OK")

    record = {
        "date":today,"ts":ts,
        "pc_equity":cboe["pc_equity"],"pc_total":cboe["pc_total"],"pc_index":cboe["pc_index"],
        "vix":vix,"skew":skew,"vstoxx":vstoxx,"stoxx50":stoxx50,
        "hyg_iei":r_hyg_iei,"eem_spy":r_eem_spy,"copper_gold":r_copper_gold,
        "tip_ief":r_tip_ief,"dxy":dxy,"eurusd":eurusd,
        "yield_curve":yc_spread,"stoxx_spy":r_stoxx_spy,"btp_bund":None,
    }

    history = [r for r in history if r.get("date") != today]
    history.append(record)
    history = history[-MAX_HISTORY:]

    print("\n[3/4] Signals...")
    def mk_signal(field, inverted=True):
        vals=[r.get(field) for r in history]; current=vals[-1]
        pct=percentile_rank(vals,current); sig=signal_from_percentile(pct,inverted)
        ma50=ma(vals,50); trend=None
        if current and ma50: trend="SOPRA MA50" if current>ma50 else "SOTTO MA50"
        return {"value":current,"percentile":pct,"signal":sig,"ma50":ma50,"trend":trend}

    signals = {
        "vix":mk_signal("vix",True),"skew":mk_signal("skew",True),
        "vstoxx":mk_signal("vstoxx",True),"pc_equity":mk_signal("pc_equity",True),
        "pc_total":mk_signal("pc_total",True),"pc_index":mk_signal("pc_index",True),
    }
    ratios_signals = {
        "hyg_iei":mk_signal("hyg_iei",False),"eem_spy":mk_signal("eem_spy",False),
        "copper_gold":mk_signal("copper_gold",False),"tip_ief":mk_signal("tip_ief",False),
        "dxy":mk_signal("dxy",True),"eurusd":mk_signal("eurusd",False),
        "yield_curve":mk_signal("yield_curve",False),"stoxx_spy":mk_signal("stoxx_spy",False),
        "btp_bund":{"value":None,"percentile":None,"signal":"N/D","ma50":None,"trend":None},
    }

    all_sigs=list(signals.values())+list(ratios_signals.values())
    fear_n=sum(1 for s in all_sigs if s["signal"] in ("FEAR","RISK-OFF","TAIL RISK"))
    greed_n=sum(1 for s in all_sigs if s["signal"] in ("GREED","RISK-ON","COMPLACENCY"))
    total_n=len([s for s in all_sigs if s["signal"]!="N/D"])
    if total_n==0:             composite="N/D"
    elif fear_n/total_n>=0.55: composite="RISK-OFF"
    elif fear_n/total_n>=0.35: composite="CAUTO"
    elif greed_n/total_n>=0.55:composite="RISK-ON"
    elif greed_n/total_n>=0.35:composite="POSITIVO"
    else:                      composite="NEUTRO"

    print("\n[4/4] Projections...")
    projection = compute_projection(signals, ratios_signals)

    output={"updated":ts,"date":today,"composite":composite,"latest":record,
            "signals":signals,"ratios":ratios_signals,"projection":projection,"history":history}
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE,"w") as f: json.dump(output,f,indent=2)

    ok = len([k for k,v in record.items() if k not in ('date','ts','btp_bund') and v is not None])
    tot = len([k for k in record if k not in ('date','ts')])
    print(f"\n{'='*55}")
    print(f"COMPOSITO: {composite} | DATI: {ok}/{tot} OK")
    print(f"VIX={vix} | VSTOXX={vstoxx} | P/C Eq={cboe['pc_equity']}")
    print(f"HYG/IEI={r_hyg_iei} | Cu/Au={r_copper_gold} | YC={yc_spread}%")
    print(f"PROIEZIONE: {projection['breve']['regime']} / {projection['medio']['regime']} / {projection['lungo']['regime']}")
    print(f"Storico: {len(history)} | {DATA_FILE}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
