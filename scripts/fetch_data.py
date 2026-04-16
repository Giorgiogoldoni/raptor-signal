"""
RAPTOR SIGNAL — Daily Data Fetcher
Scarica Put/Call Ratio, VIX e SKEW da CBOE e Yahoo Finance
"""

import json
import os
import urllib.request
import urllib.error
from datetime import datetime, date
import re

DATA_FILE = "data/signal.json"

def load_existing():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {"updated": "", "history": []}

def fetch_cboe_daily():
    """Scarica la pagina daily stats CBOE e estrae i ratio"""
    url = "https://www.cboe.com/us/options/market_statistics/daily/"
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        # Cerca pattern numerici per equity P/C e total P/C
        equity = re.search(r'Equity.*?(\d+\.\d+)', html, re.DOTALL)
        total  = re.search(r'Total.*?(\d+\.\d+)', html, re.DOTALL)
        index  = re.search(r'Index.*?(\d+\.\d+)', html, re.DOTALL)
        return {
            "pc_equity": float(equity.group(1)) if equity else None,
            "pc_total":  float(total.group(1))  if total  else None,
            "pc_index":  float(index.group(1))  if index  else None,
        }
    except Exception as e:
        print(f"CBOE fetch error: {e}")
        return {"pc_equity": None, "pc_total": None, "pc_index": None}

def fetch_yahoo(ticker):
    """Scarica prezzo corrente da Yahoo Finance"""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=2d"
    headers = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        return round(closes[-1], 2) if closes else None
    except Exception as e:
        print(f"Yahoo fetch error {ticker}: {e}")
        return None

def compute_signal(history, field, window=252):
    """Calcola percentile e segnale FEAR/NEUTRAL/GREED"""
    values = [r[field] for r in history[-window:] if r.get(field) is not None]
    if len(values) < 20:
        return "N/A", None
    current = values[-1]
    rank = sum(1 for v in values if v <= current) / len(values) * 100
    if field in ("pc_equity", "pc_total", "pc_index"):
        # P/C alto = paura = contrarian bullish
        if rank >= 80: signal = "FEAR"
        elif rank <= 20: signal = "GREED"
        else: signal = "NEUTRAL"
    elif field == "vix":
        if current >= 30: signal = "FEAR"
        elif current <= 16: signal = "GREED"
        else: signal = "NEUTRAL"
    elif field == "skew":
        if current >= 145: signal = "TAIL RISK"
        elif current <= 110: signal = "COMPLACENCY"
        else: signal = "NEUTRAL"
    else:
        signal = "NEUTRAL"
    return signal, round(rank, 1)

def main():
    today = date.today().isoformat()
    data = load_existing()
    history = data.get("history", [])

    # Evita duplicati
    if history and history[-1]["date"] == today:
        print(f"Dati già presenti per {today}, skip.")
        return

    print(f"Fetching data for {today}...")
    cboe = fetch_cboe_daily()
    vix   = fetch_yahoo("^VIX")
    skew  = fetch_yahoo("^SKEW")

    record = {
        "date":       today,
        "pc_equity":  cboe["pc_equity"],
        "pc_total":   cboe["pc_total"],
        "pc_index":   cboe["pc_index"],
        "vix":        vix,
        "skew":       skew,
    }
    history.append(record)

    # Calcola segnali
    signals = {}
    for field in ("pc_equity", "pc_total", "vix", "skew"):
        sig, pct = compute_signal(history, field)
        signals[field] = {"signal": sig, "percentile": pct}

    # Segnale composito
    fear_count = sum(1 for s in signals.values() if s["signal"] in ("FEAR", "TAIL RISK"))
    greed_count = sum(1 for s in signals.values() if s["signal"] in ("GREED", "COMPLACENCY"))
    if fear_count >= 3:   composite = "FEAR"
    elif greed_count >= 3: composite = "GREED"
    elif fear_count >= 2: composite = "FEAR/NEUTRAL"
    elif greed_count >= 2: composite = "GREED/NEUTRAL"
    else:                  composite = "NEUTRAL"

    output = {
        "updated":   today,
        "latest":    record,
        "signals":   signals,
        "composite": composite,
        "history":   history[-504:]  # ~2 anni
    }

    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Done. Composite: {composite}")
    print(f"  VIX: {vix} | SKEW: {skew}")
    print(f"  P/C Equity: {cboe['pc_equity']} | P/C Total: {cboe['pc_total']}")

if __name__ == "__main__":
    main()
