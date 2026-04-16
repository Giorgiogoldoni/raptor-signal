"""
RAPTOR SIGNAL v2 — Data Fetcher
Scarica dati da CBOE e Yahoo Finance 4 volte al giorno.
Indicatori: P/C Ratio, VIX, SKEW, VSTOXX, ratio intermarket USA + Europa.
"""

import json, os, urllib.request, urllib.error
from datetime import datetime, date, timezone
import re

DATA_FILE = "data/signal.json"
MAX_HISTORY = 504  # ~2 anni di dati giornalieri

# ─── FETCH HELPERS ────────────────────────────────────────────

def fetch_yahoo(ticker):
    """Scarica ultimo prezzo da Yahoo Finance."""
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
    """Scarica P/C ratio da pagina CBOE daily statistics."""
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

# ─── CALCOLI STATISTICI ───────────────────────────────────────

def percentile_rank(series, value, window=252):
    """Calcola il rango percentile del valore corrente nella finestra."""
    data = [x for x in series[-window:] if x is not None]
    if len(data) < 20 or value is None:
        return None
    rank = sum(1 for v in data if v <= value) / len(data) * 100
    return round(rank, 1)

def ma(series, n):
    """Media mobile semplice degli ultimi n valori non-None."""
    vals = [x for x in series[-n:] if x is not None]
    return round(sum(vals) / len(vals), 4) if vals else None

def signal_from_percentile(pct, inverted=False):
    """
    Converte percentile in segnale direzionale.
    inverted=True per indicatori dove alto = bearish (P/C ratio, VIX).
    """
    if pct is None:
        return "N/D"
    if inverted:
        if pct >= 80: return "FEAR"
        if pct <= 20: return "GREED"
        return "NEUTRAL"
    else:
        if pct >= 80: return "RISK-ON"
        if pct <= 20: return "RISK-OFF"
        return "NEUTRAL"

# ─── PROIEZIONE ───────────────────────────────────────────────

def compute_projection(signals, ratios):
    """
    Calcola proiezione a 3 orizzonti con score pesato.
    Restituisce score (-100 a +100) e probabilità di regime.
    """
    def score_signal(sig, weight):
        mapping = {"RISK-ON": 1, "GREED": 1, "NEUTRAL": 0,
                   "FEAR": -1, "RISK-OFF": -1, "TAIL RISK": -2,
                   "COMPLACENCY": 0.5, "N/D": 0}
        return mapping.get(sig, 0) * weight

    # ── Breve termine (2-4 settimane): sentiment opzioni
    short_score = 0
    short_score += score_signal(signals.get("vix", {}).get("signal", "N/D"), 2.5)
    short_score += score_signal(signals.get("pc_equity", {}).get("signal", "N/D"), 2.0)
    short_score += score_signal(signals.get("pc_total", {}).get("signal", "N/D"), 1.5)
    short_score += score_signal(signals.get("skew", {}).get("signal", "N/D"), 1.0)
    short_score += score_signal(signals.get("vstoxx", {}).get("signal", "N/D"), 1.0)
    short_max = 8.0

    # ── Medio termine (1-3 mesi): intermarket
    mid_score = 0
    mid_score += score_signal(ratios.get("hyg_iei", {}).get("signal", "N/D"), 2.0)
    mid_score += score_signal(ratios.get("eem_spy", {}).get("signal", "N/D"), 1.5)
    mid_score += score_signal(ratios.get("copper_gold", {}).get("signal", "N/D"), 2.0)
    mid_score += score_signal(ratios.get("tip_ief", {}).get("signal", "N/D"), 1.5)
    mid_score += score_signal(ratios.get("eurusd", {}).get("signal", "N/D"), 1.0)
    mid_max = 8.0

    # ── Lungo termine (6-18 mesi): macro strutturale
    long_score = 0
    long_score += score_signal(ratios.get("yield_curve", {}).get("signal", "N/D"), 3.0)
    long_score += score_signal(ratios.get("stoxx_spy", {}).get("signal", "N/D"), 1.5)
    long_score += score_signal(ratios.get("btp_bund", {}).get("signal", "N/D"), 2.0)
    long_max = 6.5

    def normalize(score, max_score):
        if max_score == 0: return 0
        return round((score / max_score) * 100, 1)

    def to_regime(norm_score):
        if norm_score >= 30:   return "RISK-ON"
        if norm_score >= 5:    return "POSITIVO"
        if norm_score >= -5:   return "NEUTRO"
        if norm_score >= -30:  return "CAUTO"
        return "RISK-OFF"

    def to_probs(norm_score):
        # Distribuzione probabilistica semplificata
        base_pos = min(max((norm_score + 100) / 200, 0.05), 0.95)
        base_neg = min(max((-norm_score + 100) / 200, 0.05), 0.95)
        neutral  = max(0.05, 1 - base_pos * 0.6 - base_neg * 0.4)
        risk_on  = round(base_pos * 0.6 * 100, 0)
        risk_off = round(base_neg * 0.4 * 100, 0)
        neut     = round(max(0, 100 - risk_on - risk_off), 0)
        return {"risk_on": int(risk_on), "neutro": int(neut), "risk_off": int(risk_off)}

    s_norm = normalize(short_score, short_max)
    m_norm = normalize(mid_score,   mid_max)
    l_norm = normalize(long_score,  long_max)

    return {
        "breve": {
            "orizzonte": "2-4 settimane",
            "score": s_norm,
            "regime": to_regime(s_norm),
            "probabilita": to_probs(s_norm),
            "driver": "VIX · P/C Ratio · SKEW · VSTOXX"
        },
        "medio": {
            "orizzonte": "1-3 mesi",
            "score": m_norm,
            "regime": to_regime(m_norm),
            "probabilita": to_probs(m_norm),
            "driver": "HYG/IEI · Copper/Gold · EEM/SPY · EUR/USD"
        },
        "lungo": {
            "orizzonte": "6-18 mesi",
            "score": l_norm,
            "regime": to_regime(l_norm),
            "probabilita": to_probs(l_norm),
            "driver": "Yield Curve · BTP/Bund · STOXX/SPY"
        }
    }

# ─── MAIN ─────────────────────────────────────────────────────

def main():
    now_utc = datetime.now(timezone.utc)
    today   = now_utc.strftime("%Y-%m-%d")
    ts      = now_utc.strftime("%Y-%m-%dT%H:%M")

    # Carica storico
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            existing = json.load(f)
    else:
        existing = {"history": []}
    history = existing.get("history", [])

    print(f"\n{'='*50}")
    print(f"RAPTOR SIGNAL v2 — {ts} UTC")
    print(f"{'='*50}")

    # ── 1. Fetch tutti i prezzi ──────────────────────────────
    print("\n[1/4] Fetching sentiment indicators...")
    cboe = fetch_cboe_putcall()
    vix   = fetch_yahoo("^VIX")
    skew  = fetch_yahoo("^SKEW")
    vstoxx = fetch_yahoo("^V2TX")       # VSTOXX — VIX europeo
    stoxx50 = fetch_yahoo("^STOXX50E")  # Euro Stoxx 50

    print(f"  VIX={vix} | SKEW={skew} | VSTOXX={vstoxx}")
    print(f"  P/C Equity={cboe['pc_equity']} | Total={cboe['pc_total']}")

    print("\n[2/4] Fetching intermarket ratios...")
    # USA
    hyg   = fetch_yahoo("HYG")
    iei   = fetch_yahoo("IEI")
    eem   = fetch_yahoo("EEM")
    spy   = fetch_yahoo("SPY")
    copper = fetch_yahoo("CPER")
    gold  = fetch_yahoo("GLD")
    tip   = fetch_yahoo("TIP")
    ief   = fetch_yahoo("IEF")
    dxy   = fetch_yahoo("DX-Y.NYB")
    # Europa
    eurusd = fetch_yahoo("EURUSD=X")
    tnx    = fetch_yahoo("^TNX")   # Rendimento 10Y USA
    tyx    = fetch_yahoo("^TYX")   # Rendimento 30Y USA (proxy spread)
    btp10  = fetch_yahoo("ITGVT10.MI")  # BTP 10Y (se disponibile)
    bund10 = fetch_yahoo("^EXHE")       # Proxy Bund

    # Calcola ratio
    def ratio(a, b):
        if a and b and b != 0: return round(a / b, 6)
        return None

    r_hyg_iei     = ratio(hyg, iei)
    r_eem_spy     = ratio(eem, spy)
    r_copper_gold = ratio(copper, gold)
    r_tip_ief     = ratio(tip, ief)
    r_stoxx_spy   = ratio(stoxx50, spy)
    # Yield curve USA: spread 10Y - 2Y (approssimato con TNX)
    ust2  = fetch_yahoo("^IRX")   # 13-week T-bill proxy breve
    yc_spread = round(tnx - ust2 / 100, 3) if tnx and ust2 else None

    print(f"  HYG/IEI={r_hyg_iei} | EEM/SPY={r_eem_spy} | Cu/Gold={r_copper_gold}")
    print(f"  TIP/IEF={r_tip_ief} | EUR/USD={eurusd} | YC Spread={yc_spread}")

    # ── 2. Aggiorna storico ─────────────────────────────────
    record = {
        "date": today,
        "ts":   ts,
        # Sentiment
        "pc_equity":  cboe["pc_equity"],
        "pc_total":   cboe["pc_total"],
        "pc_index":   cboe["pc_index"],
        "vix":        vix,
        "skew":       skew,
        "vstoxx":     vstoxx,
        "stoxx50":    stoxx50,
        # Ratio USA
        "hyg_iei":     r_hyg_iei,
        "eem_spy":     r_eem_spy,
        "copper_gold": r_copper_gold,
        "tip_ief":     r_tip_ief,
        "dxy":         dxy,
        # Ratio Europa
        "eurusd":      eurusd,
        "yield_curve": yc_spread,
        "stoxx_spy":   r_stoxx_spy,
        "btp_bund":    None,   # placeholder — dato non affidabile su Yahoo
    }

    # Rimuovi eventuale record dello stesso giorno (sovrascrittura intraday)
    history = [r for r in history if r.get("date") != today]
    history.append(record)
    history = history[-MAX_HISTORY:]

    # ── 3. Calcola segnali su storico ───────────────────────
    print("\n[3/4] Computing signals...")

    def mk_signal(field, inverted=True):
        vals = [r.get(field) for r in history]
        current = vals[-1]
        pct = percentile_rank(vals, current)
        sig = signal_from_percentile(pct, inverted)
        ma50 = ma(vals, 50)
        trend = None
        if current and ma50:
            trend = "SOPRA MA50" if current > ma50 else "SOTTO MA50"
        return {"value": current, "percentile": pct, "signal": sig,
                "ma50": ma50, "trend": trend}

    signals = {
        "vix":        mk_signal("vix",       inverted=True),
        "skew":       mk_signal("skew",      inverted=True),
        "vstoxx":     mk_signal("vstoxx",    inverted=True),
        "pc_equity":  mk_signal("pc_equity", inverted=True),
        "pc_total":   mk_signal("pc_total",  inverted=True),
        "pc_index":   mk_signal("pc_index",  inverted=True),
    }

    ratios_signals = {
        "hyg_iei":     mk_signal("hyg_iei",     inverted=False),
        "eem_spy":     mk_signal("eem_spy",      inverted=False),
        "copper_gold": mk_signal("copper_gold",  inverted=False),
        "tip_ief":     mk_signal("tip_ief",      inverted=False),
        "dxy":         mk_signal("dxy",          inverted=True),
        "eurusd":      mk_signal("eurusd",       inverted=False),
        "yield_curve": mk_signal("yield_curve",  inverted=False),
        "stoxx_spy":   mk_signal("stoxx_spy",    inverted=False),
        "btp_bund":    {"value": None, "percentile": None, "signal": "N/D", "ma50": None, "trend": None},
    }

    # ── 4. Segnale composito globale ────────────────────────
    all_sigs = list(signals.values()) + list(ratios_signals.values())
    fear_n   = sum(1 for s in all_sigs if s["signal"] in ("FEAR","RISK-OFF","TAIL RISK"))
    greed_n  = sum(1 for s in all_sigs if s["signal"] in ("GREED","RISK-ON","COMPLACENCY"))
    total_n  = len([s for s in all_sigs if s["signal"] != "N/D"])

    if total_n == 0:
        composite = "N/D"
    elif fear_n / total_n >= 0.55:  composite = "RISK-OFF"
    elif fear_n / total_n >= 0.35:  composite = "CAUTO"
    elif greed_n / total_n >= 0.55: composite = "RISK-ON"
    elif greed_n / total_n >= 0.35: composite = "POSITIVO"
    else:                           composite = "NEUTRO"

    # ── 5. Proiezione ───────────────────────────────────────
    print("\n[4/4] Computing projections...")
    projection = compute_projection(signals, ratios_signals)

    # ── 6. Salva ────────────────────────────────────────────
    output = {
        "updated":    ts,
        "date":       today,
        "composite":  composite,
        "latest":     record,
        "signals":    signals,
        "ratios":     ratios_signals,
        "projection": projection,
        "history":    history
    }

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n{'='*50}")
    print(f"COMPOSITO: {composite}")
    print(f"PROIEZIONE BREVE:  {projection['breve']['regime']}  (score {projection['breve']['score']})")
    print(f"PROIEZIONE MEDIO:  {projection['medio']['regime']}  (score {projection['medio']['score']})")
    print(f"PROIEZIONE LUNGO:  {projection['lungo']['regime']}  (score {projection['lungo']['score']})")
    print(f"Storico: {len(history)} record | Salvato: {DATA_FILE}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
