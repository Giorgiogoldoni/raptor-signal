"""
RAPTOR SIGNAL v2.1 — Data Fetcher
Fix v2.1:
  - Yield curve: rimosso /100 su ^IRX (già in percentuale)
  - CPER/GLD: sostituito con HG=F/GC=F (futures diretti)
  - HYG/IEI: aggiunta sanity check (range 0.5-1.2)
"""
import json, os, urllib.request, urllib.error
from datetime import datetime, date, timezone
import re

DATA_FILE = "data/signal.json"
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
        print(f"  Yahoo error [{ticker}]: {e}")
        return None

def fetch_cboe_putcall():
    url = "https://www.cboe.com/us/options/market_statistics/daily/"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8")
        def extract(pattern):
            m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            return float(m.group(1)) if m else None
        return {
            "pc_equity": extract(r'equity[^<]*<[^>]+>\s*(\d+\.\d+)'),
            "pc_total":  extract(r'total[^<]*<[^>]+>\s*(\d+\.\d+)'),
            "pc_index":  extract(r'index[^<]*<[^>]+>\s*(\d+\.\d+)'),
        }
    except Exception as e:
        print(f"  CBOE error: {e}")
        return {"pc_equity": None, "pc_total": None, "pc_index": None}

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
    def score_signal(sig, weight):
        mapping = {"RISK-ON":1,"GREED":1,"NEUTRAL":0,"FEAR":-1,"RISK-OFF":-1,"TAIL RISK":-2,"COMPLACENCY":0.5,"N/D":0}
        return mapping.get(sig, 0) * weight
    short_score = (score_signal(signals.get("vix",{}).get("signal","N/D"),2.5)
                 + score_signal(signals.get("pc_equity",{}).get("signal","N/D"),2.0)
                 + score_signal(signals.get("pc_total",{}).get("signal","N/D"),1.5)
                 + score_signal(signals.get("skew",{}).get("signal","N/D"),1.0)
                 + score_signal(signals.get("vstoxx",{}).get("signal","N/D"),1.0))
    mid_score = (score_signal(ratios.get("hyg_iei",{}).get("signal","N/D"),2.0)
               + score_signal(ratios.get("eem_spy",{}).get("signal","N/D"),1.5)
               + score_signal(ratios.get("copper_gold",{}).get("signal","N/D"),2.0)
               + score_signal(ratios.get("tip_ief",{}).get("signal","N/D"),1.5)
               + score_signal(ratios.get("eurusd",{}).get("signal","N/D"),1.0))
    long_score = (score_signal(ratios.get("yield_curve",{}).get("signal","N/D"),3.0)
                + score_signal(ratios.get("stoxx_spy",{}).get("signal","N/D"),1.5)
                + score_signal(ratios.get("btp_bund",{}).get("signal","N/D"),2.0))
    def norm(s,mx): return round((s/mx)*100,1) if mx else 0
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
    sn=norm(short_score,8.0); mn=norm(mid_score,8.0); ln=norm(long_score,6.5)
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

    print(f"\n{'='*50}\nRAPTOR SIGNAL v2.1 — {ts} UTC\n{'='*50}")

    print("\n[1/4] Sentiment indicators...")
    cboe    = fetch_cboe_putcall()
    vix     = fetch_yahoo("^VIX")
    skew    = fetch_yahoo("^SKEW")
    vstoxx  = fetch_yahoo("^V2TX")
    stoxx50 = fetch_yahoo("^STOXX50E")

    print("\n[2/4] Intermarket ratios...")
    hyg    = fetch_yahoo("HYG")
    iei    = fetch_yahoo("IEI")
    eem    = fetch_yahoo("EEM")
    spy    = fetch_yahoo("SPY")
    tip    = fetch_yahoo("TIP")
    ief    = fetch_yahoo("IEF")
    dxy    = fetch_yahoo("DX-Y.NYB")
    eurusd = fetch_yahoo("EURUSD=X")
    tnx    = fetch_yahoo("^TNX")

    # ── FIX 1: Yield curve — ^IRX già in % — NON dividere per 100 ──
    ust2 = fetch_yahoo("^IRX")
    yc_spread = round(tnx - ust2, 3) if tnx and ust2 else None
    print(f"  ✓ YC: {tnx}% - {ust2}% = {yc_spread}%  [FIX: rimosso /100]")

    # ── FIX 2: Copper/Gold — futures diretti HG=F / GC=F ──
    copper_fut = fetch_yahoo("HG=F")
    gold_fut   = fetch_yahoo("GC=F")
    r_copper_gold = round(copper_fut / gold_fut, 6) if copper_fut and gold_fut else None
    print(f"  ✓ Cu/Au: HG=F={copper_fut} / GC=F={gold_fut} = {r_copper_gold}  [FIX: futures diretti]")

    def ratio(a, b):
        if a and b and b != 0: return round(a / b, 6)
        return None

    r_hyg_iei   = ratio(hyg, iei)
    r_eem_spy   = ratio(eem, spy)
    r_tip_ief   = ratio(tip, ief)
    r_stoxx_spy = ratio(stoxx50, spy)

    # ── FIX 3: Sanity check HYG/IEI ──
    if r_hyg_iei and (r_hyg_iei < 0.5 or r_hyg_iei > 1.2):
        print(f"  ⚠ HYG/IEI={r_hyg_iei} fuori range [0.5-1.2] — scartato")
        r_hyg_iei = None

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

    print("\n[3/4] Computing signals...")
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

    print("\n[4/4] Computing projections...")
    projection = compute_projection(signals, ratios_signals)

    output={"updated":ts,"date":today,"composite":composite,"latest":record,
            "signals":signals,"ratios":ratios_signals,"projection":projection,"history":history}
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE,"w") as f: json.dump(output,f,indent=2)

    print(f"\n{'='*50}")
    print(f"COMPOSITO: {composite}")
    print(f"PROIEZIONE: {projection['breve']['regime']} / {projection['medio']['regime']} / {projection['lungo']['regime']}")
    print(f"Storico: {len(history)} record | Salvato: {DATA_FILE}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
