"""
RAPTOR SIGNAL v3.0 — Data Fetcher
Novità v3.0 (rispetto a v2.3):
  - Data del record = data dell'ultima barra di mercato (^VIX), NON l'orologio UTC
    -> niente più righe "fantasma" nel weekend
  - Tutte le serie Yahoo lette "as-of" della stessa data (nessun dato misto)
  - P/C CBOE: CSV ufficiali + parser a regex solo come ultima risorsa;
    range per serie e scarto dei valori "fermi" (identici agli ultimi 3 giorni)
  - HYG/IEI: fonte (MI/US) registrata nel record; percentile, MA50 e sanity check
    usano solo osservazioni della stessa fonte
  - VSTOXX: solo ^V2TX (nessun fallback con vol. realizzata: se manca, il dato manca)
  - Curva dei rendimenti: 10Y-2Y da FRED (T10Y2Y), applicata anche allo storico
  - Proiezione -> "indice di regime": dati mancanti fuori dal denominatore,
    copertura minima 50%, quote simmetriche
  - signal.json contiene i blocchi "rules" (pesi/soglie reali) e "data_quality"
  - Opzione --rebuild: ricostruisce lo storico (2 anni) da Yahoo / FRED / CBOE
"""
import json, os, sys, urllib.request, re, math, csv, io
from datetime import datetime, timezone

VERSION      = "RAPTOR SIGNAL v3.1"
DATA_FILE    = "data/signal.json"
MAX_HISTORY  = 504
MIN_COVERAGE = 0.5          # copertura minima (peso dati disponibili / peso totale)

# ─── DEFINIZIONI (fonte unica per calcolo, regole JSON e PDF) ─────────────────
# (chiave, etichetta, invertito, fonte)
SENTIMENT = [
    ("vix",       "VIX",        True,  "Yahoo Finance ^VIX"),
    ("skew",      "SKEW",       True,  "Yahoo Finance ^SKEW"),
    ("vstoxx",    "VSTOXX",     True,  "Yahoo Finance ^V2TX (solo indice: se non disponibile il dato manca)"),
    ("pc_equity", "P/C Equity", True,  "CBOE (CSV ufficiale; in subordine pagina web)"),
    ("pc_total",  "P/C Total",  True,  "CBOE (CSV ufficiale; in subordine pagina web)"),
    ("pc_index",  "P/C Index",  True,  "CBOE (CSV ufficiale; in subordine pagina web)"),
]
RATIOS = [
    ("hyg_iei",     "HYG/IEI",      False, "IHYG.MI / IEGE.MI (EUR, Milano); se assenti HYG / IEI (USD). Percentile e MA50 solo sulla stessa fonte"),
    ("eem_spy",     "EEM/SPY",      False, "Yahoo Finance EEM / SPY"),
    ("copper_gold", "Rame/Oro",     False, "Yahoo Finance HG=F / GC=F (futures)"),
    ("tip_ief",     "TIP/IEF",      False, "Yahoo Finance TIP / IEF"),
    ("dxy",         "DXY",          True,  "Yahoo Finance DX-Y.NYB"),
    ("eurusd",      "EUR/USD",      False, "Yahoo Finance EURUSD=X"),
    ("yield_curve", "Curva 10Y-2Y", False, "FRED T10Y2Y (punti percentuali)"),
    ("stoxx_spy",   "STOXX50/SPY",  False, "Yahoo Finance ^STOXX50E / SPY (indice in punti / ETF in USD: misura di tendenza, non un rapporto puro)"),
    ("stoxx_eem",   "STOXX50/EEM",  False, "Yahoo Finance ^STOXX50E / EEM (indice in punti / ETF in USD: misura di tendenza)"),
    ("btp_bund",    "BTP/Bund",     False, "Non implementato: sempre N/D"),
]
LABELS     = {k: l for k, l, _, _ in SENTIMENT + RATIOS}
SRC_FIELDS = {"hyg_iei": "hyg_iei_src", "yield_curve": "yield_curve_src"}

SCORE_MAP = {"RISK-ON": 1, "GREED": 1, "NEUTRAL": 0, "FEAR": -1, "RISK-OFF": -1,
             "TAIL RISK": -2, "COMPLACENCY": 0.5, "N/D": 0}

# (chiave, gruppo, peso)
HORIZONS = {
    "breve": {"orizzonte": "2-4 settimane", "componenti": [
        ("vix", "signals", 2.5), ("pc_equity", "signals", 2.0), ("pc_total", "signals", 1.5),
        ("skew", "signals", 1.0), ("vstoxx", "signals", 1.0)]},
    "medio": {"orizzonte": "1-3 mesi", "componenti": [
        ("hyg_iei", "ratios", 2.0), ("eem_spy", "ratios", 1.5), ("copper_gold", "ratios", 2.0),
        ("tip_ief", "ratios", 1.5), ("eurusd", "ratios", 1.0)]},
    "lungo": {"orizzonte": "6-18 mesi", "componenti": [
        ("yield_curve", "ratios", 3.0), ("stoxx_spy", "ratios", 1.5), ("btp_bund", "ratios", 2.0)]},
}
REGIME_STEPS = [(30, "RISK-ON"), (5, "POSITIVO"), (-5, "NEUTRO"), (-30, "CAUTO")]   # sotto -30: RISK-OFF

PC_RANGE = {"pc_equity": (0.25, 1.5), "pc_total": (0.4, 2.0), "pc_index": (0.3, 4.0)}
# Range più stretti per il parser a regex sulle pagine web (fragile: in passato ha restituito la costante 2.5)
PC_RANGE_HTML = {"pc_equity": (0.25, 1.5), "pc_total": (0.4, 1.8), "pc_index": (0.3, 2.2)}
CBOE_CSV = {
    "pc_equity": "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv",
    "pc_total":  "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/totalpc.csv",
    "pc_index":  "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/indexpc.csv",
}

REF_DATE    = None      # data di riferimento = data dell'ultima barra di mercato USA
_BARS_CACHE = {}

# ─── HTTP / DATE ──────────────────────────────────────────────

def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="ignore")

def _days_between(a, b):
    return abs((datetime.strptime(b, "%Y-%m-%d") - datetime.strptime(a, "%Y-%m-%d")).days)

def _parse_date(s):
    s = s.strip().strip('"')
    for f in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d-%b-%y"):
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def _r4(v):
    return round(v, 4) if v is not None else None

def asof(series, ref, max_age=5):
    """Ultimo valore di {data: valore} con data <= ref, se non più vecchio di max_age giorni."""
    ds = [d for d in series if d <= ref]
    if not ds:
        return None
    d = max(ds)
    return series[d] if _days_between(d, ref) <= max_age else None

# ─── YAHOO ────────────────────────────────────────────────────

def fetch_bars(ticker, range_="5d"):
    """Barre giornaliere [(data_locale, close)] con data presa dalla barra (non dall'orologio)."""
    key = (ticker, range_)
    if key in _BARS_CACHE:
        return _BARS_CACHE[key]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={range_}"
    bars = []
    try:
        res = json.loads(http_get(url, 15))["chart"]["result"][0]
        ts = res.get("timestamp") or []
        closes = res["indicators"]["quote"][0]["close"]
        off = (res.get("meta", {}) or {}).get("gmtoffset", 0) or 0
        for t, c in zip(ts, closes):
            if c is None:
                continue
            d = datetime.fromtimestamp(t + off, tz=timezone.utc).strftime("%Y-%m-%d")
            bars.append((d, c))
    except Exception as e:
        print(f"  Yahoo [{ticker}]: {e}")
    _BARS_CACHE[key] = bars
    return bars

def fetch_last(ticker, range_="5d", max_age=5):
    """Ultimo close con data <= REF_DATE (as-of). None se assente o troppo vecchio."""
    bars = fetch_bars(ticker, range_)
    if REF_DATE:
        bars = [b for b in bars if b[0] <= REF_DATE]
        if bars and _days_between(bars[-1][0], REF_DATE) > max_age:
            print(f"  ⚠ {ticker}: ultima barra {bars[-1][0]} troppo vecchia vs {REF_DATE}")
            return None
    return round(bars[-1][1], 4) if bars else None

# ─── FRED (curva 10Y-2Y) ──────────────────────────────────────

def fetch_fred(series_id="T10Y2Y", start="2023-12-01"):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}"
    out = {}
    try:
        for row in csv.reader(io.StringIO(http_get(url, 25))):
            if len(row) < 2:
                continue
            d = _parse_date(row[0])
            if not d:
                continue
            try:
                out[d] = float(row[1])
            except ValueError:
                continue          # "." = dato non disponibile
        if out:
            print(f"  ✓ FRED {series_id}: {len(out)} righe, ultima {max(out)} = {out[max(out)]}")
        else:
            print(f"  ✗ FRED {series_id}: nessun dato")
    except Exception as e:
        print(f"  FRED [{series_id}]: {e}")
    return out

def fred_valid(v):
    return v if (v is not None and -6 <= v <= 6) else None

def normalize_yc(history, fred):
    """Allinea la curva 10Y-2Y dello storico a FRED (unica fonte). Ultime 5 righe sempre riallineate."""
    if not fred:
        return 0
    n = 0
    for i, rec in enumerate(history):
        recent = i >= len(history) - 5
        if rec.get("yield_curve_src") == "FRED" and not recent:
            continue
        v = fred_valid(asof(fred, rec["date"], 7))
        if v is None and rec.get("yield_curve_src") == "FRED":
            continue
        rec["yield_curve"] = v
        rec["yield_curve_src"] = "FRED" if v is not None else None
        n += 1
    return n

# ─── CBOE PUT/CALL ────────────────────────────────────────────

def parse_cboe_csv(text):
    """CSV CBOE: prima colonna data, ultima colonna rapporto P/C. Ritorna {data: valore}."""
    out = {}
    for row in csv.reader(io.StringIO(text)):
        if len(row) < 2:
            continue
        d = _parse_date(row[0])
        if not d:
            continue
        try:
            out[d] = float(row[-1])
        except ValueError:
            continue
    return out

def fetch_cboe_series():
    """Serie storiche dai CSV ufficiali CBOE: {chiave: {data: valore}} (vuoto se non disponibili)."""
    out = {}
    for key, url in CBOE_CSV.items():
        try:
            series = parse_cboe_csv(http_get(url))
            lo, hi = PC_RANGE[key]
            series = {d: v for d, v in series.items() if lo <= v <= hi}
            if series:
                out[key] = series
                last = max(series)
                print(f"  ✓ CBOE CSV {key}: {len(series)} righe, ultima {last} = {series[last]}")
            else:
                print(f"  ✗ CBOE CSV {key}: nessuna riga valida")
        except Exception as e:
            print(f"  CBOE CSV [{key}]: {e}")
    return out

def _parse_cboe(content):
    result = {"pc_equity": None, "pc_total": None, "pc_index": None}
    patterns = {
        "pc_equity": [r'equity[^0-9\n]{0,30}([0-9]+\.[0-9]+)', r'EQUITY[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
        "pc_total":  [r'total[^0-9\n]{0,30}([0-9]+\.[0-9]+)',  r'TOTAL[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
        "pc_index":  [r'index[^0-9\n]{0,30}([0-9]+\.[0-9]+)',  r'INDEX[^,\n]*,[^,\n]*,[^,\n]*,\s*([0-9]+\.[0-9]+)'],
    }
    for key, pats in patterns.items():
        lo, hi = PC_RANGE_HTML[key]
        for pat in pats:
            m = re.search(pat, content, re.IGNORECASE)
            if m:
                val = float(m.group(1))
                if lo <= val <= hi:
                    result[key] = val
                    break
    return result

def _cboe_html():
    """Ultima risorsa: pagine CBOE con parser a regex (fragile)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    urls = [
        f"https://www.cboe.com/us/options/market_statistics/daily/?mkt=cone&dt={today}",
        "https://www.cboe.com/us/options/market_statistics/daily/",
        "https://cdn.cboe.com/api/global/us_options_volume/daily-options.csv",
    ]
    for url in urls:
        try:
            result = _parse_cboe(http_get(url, 15))
            if any(v is not None for v in result.values()):
                print(f"  ✓ CBOE (regex) {url[:45]}: {result}")
                return result
        except Exception as e:
            print(f"  CBOE [{url[:45]}]: {e}")
    return {"pc_equity": None, "pc_total": None, "pc_index": None}

def fetch_cboe_putcall(history, ref_date):
    result = {"pc_equity": None, "pc_total": None, "pc_index": None}
    for k, s in fetch_cboe_series().items():
        result[k] = asof(s, ref_date, 7)
    if any(v is None for v in result.values()):
        for k, v in _cboe_html().items():
            if result[k] is None:
                result[k] = v
    for k in list(result):
        v = result[k]
        if v is None:
            continue
        lo, hi = PC_RANGE[k]
        if not (lo <= v <= hi):
            print(f"  ⚠ {k}={v} fuori range [{lo}, {hi}] — scartato")
            result[k] = None
            continue
        prev = [r.get(k) for r in history if r.get("date") != ref_date and r.get(k) is not None][-3:]
        if len(prev) == 3 and all(p == v for p in prev):
            print(f"  ⚠ {k}={v} identico agli ultimi 3 giorni — scartato (dato fermo)")
            result[k] = None
    print(f"  P/C finale: {result}")
    return result

# ─── COSTRUZIONE RECORD ───────────────────────────────────────

def ratio(a, b):
    if a and b and b != 0:
        return round(a / b, 8)
    return None

def vstoxx_ok(v):
    if v is None:
        return None
    if 8 <= v <= 150:
        return v
    print(f"  ⚠ VSTOXX={v} non plausibile — scartato")
    return None

def hyg_from(g):
    """Ratio High Yield / Gov Bond breve. Ritorna (valore, fonte): Milano EUR, altrimenti USD."""
    ihyg, iege = g("IHYG.MI"), g("IEGE.MI")
    if ihyg and iege:
        return ratio(ihyg, iege), "MI"
    hyg, iei = g("HYG"), g("IEI")
    if hyg and iei:
        return ratio(hyg, iei), "US"
    return None, None

def sanity_hyg(r, src, history, ref_date):
    """Scarta se Δ>15% rispetto all'ultimo valore valido della STESSA fonte."""
    if r is None:
        return None, None
    prev = next((rec.get("hyg_iei") for rec in reversed(history)
                 if rec.get("hyg_iei") is not None and rec.get("hyg_iei_src") == src
                 and rec.get("date") != ref_date), None)
    if prev and abs(r - prev) / prev > 0.15:
        print(f"  ⚠ HYG/IEI ({src}) Δ{(r - prev) / prev * 100:.1f}% vs ultimo valore ({prev}) — scartato")
        return None, None
    return r, src

def build_record(date, ts, g, pc, yc, hyg):
    """g(ticker) -> valore as-of; pc dict P/C; yc curva 10Y-2Y; hyg (ratio, fonte)."""
    eem, spy = g("EEM"), g("SPY")
    tip, ief = g("TIP"), g("IEF")
    stoxx50  = g("^STOXX50E")
    return {
        "date": date, "ts": ts,
        "pc_equity": pc.get("pc_equity"), "pc_total": pc.get("pc_total"), "pc_index": pc.get("pc_index"),
        "vix": g("^VIX"), "skew": g("^SKEW"), "vstoxx": vstoxx_ok(g("^V2TX")), "stoxx50": stoxx50,
        "hyg_iei": hyg[0], "hyg_iei_src": hyg[1],
        "eem_spy": ratio(eem, spy), "copper_gold": ratio(g("HG=F"), g("GC=F")),
        "tip_ief": ratio(tip, ief), "dxy": g("DX-Y.NYB"), "eurusd": g("EURUSD=X"),
        "yield_curve": yc, "yield_curve_src": "FRED" if yc is not None else None,
        "stoxx_spy": ratio(stoxx50, spy), "stoxx_eem": ratio(stoxx50, eem), "btp_bund": None,
    }

# ─── REBUILD STORICO ──────────────────────────────────────────

def rebuild_history(fred):
    """Ricostruisce 2 anni di storico da Yahoo / FRED / CBOE. None se i dati non bastano."""
    print("\n[REBUILD] Ricostruzione storico (2 anni)...")
    tickers = ["^VIX", "^SKEW", "^V2TX", "^STOXX50E", "IHYG.MI", "IEGE.MI", "HYG", "IEI",
               "EEM", "SPY", "TIP", "IEF", "DX-Y.NYB", "EURUSD=X", "HG=F", "GC=F"]
    series = {t: dict(fetch_bars(t, "2y")) for t in tickers}
    cal = sorted(series["^VIX"])
    if len(cal) < 200:
        print(f"  ⚠ ^VIX: solo {len(cal)} barre — rebuild annullato, storico invariato")
        return None
    cboe = fetch_cboe_series()
    rows = []
    for d in cal[-MAX_HISTORY:]:
        g  = lambda t, d=d: _r4(asof(series[t], d, 5))
        pc = {k: (asof(cboe[k], d, 7) if k in cboe else None) for k in PC_RANGE}
        yc = fred_valid(asof(fred, d, 7)) if fred else None
        hyg = sanity_hyg(*hyg_from(g), rows, d)
        rows.append(build_record(d, f"{d}T23:59", g, pc, yc, hyg))
    print(f"  ✓ Storico ricostruito: {len(rows)} sedute, da {rows[0]['date']} a {rows[-1]['date']}")
    return rows

# ─── CALCOLI STATISTICI ───────────────────────────────────────

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

# ─── INDICE DI REGIME (ex proiezione) ─────────────────────────

def _regime(n):
    for thr, name in REGIME_STEPS:
        if n >= thr:
            return name
    return "RISK-OFF"

def _quote(n):
    """Quote simmetriche: |score| va alla direzione del segno, il resto è neutro."""
    a = int(round(min(abs(n), 100)))
    on  = a if n > 0 else 0
    off = a if n < 0 else 0
    return {"risk_on": on, "neutro": 100 - on - off, "risk_off": off}

def compute_projection(signals, ratios):
    out = {}
    for horizon, spec in HORIZONS.items():
        tot_w = sum(w for _, _, w in spec["componenti"])
        used, s, missing = 0.0, 0.0, []
        for key, grp, w in spec["componenti"]:
            sig = (signals if grp == "signals" else ratios).get(key, {}).get("signal", "N/D")
            if sig == "N/D":
                missing.append(key)
                continue
            used += w
            s += SCORE_MAP.get(sig, 0) * w
        cov = used / tot_w if tot_w else 0
        base = {
            "orizzonte": spec["orizzonte"],
            "copertura": int(round(cov * 100)),
            "dati_mancanti": missing,
            "driver": " · ".join(LABELS.get(k, k) for k, _, _ in spec["componenti"]),
        }
        if cov < MIN_COVERAGE or used == 0:
            out[horizon] = dict(base, score=None, regime="N/D", probabilita=None)
        else:
            n = round(s / used * 100, 1)
            out[horizon] = dict(base, score=n, regime=_regime(n), probabilita=_quote(n))
    return out

# ─── REGOLE (scritte in signal.json: pagina e PDF restano allineati al codice) ──

def build_rules():
    return {
        "versione": VERSION,
        "frequenza": ("Lun-Ven alle 08:00, 14:00, 21:30 e 23:00 UTC (GitHub Actions). Una riga per seduta: "
                      "l'ultimo aggiornamento del giorno sostituisce i precedenti. La data della riga è quella "
                      "dell'ultima barra di mercato (^VIX), non l'orario di esecuzione."),
        "fonti": [{"serie": l, "fonte": f} for _, l, _, f in SENTIMENT + RATIOS]
                 + [{"serie": "Storico", "fonte": "Ricostruibile su 2 anni da Yahoo Finance, FRED e CBOE con l'opzione --rebuild"}],
        "calcolo_segnali": [
            "Percentile del valore odierno sulle ultime 252 osservazioni (minimo 20, altrimenti N/D).",
            "Serie invertite (" + ", ".join(l for _, l, inv, _ in SENTIMENT + RATIOS if inv)
            + "): percentile >= 80 = FEAR; percentile <= 20 = GREED; altrimenti NEUTRAL.",
            "Serie dirette (" + ", ".join(l for _, l, inv, _ in SENTIMENT + RATIOS if not inv)
            + "): percentile >= 80 = RISK-ON; percentile <= 20 = RISK-OFF; altrimenti NEUTRAL.",
            "TREND: valore sopra/sotto la media mobile a 50 osservazioni (MA50). Trend e segnale sono misure diverse e possono divergere.",
            "HYG/IEI e curva 10Y-2Y: percentile e MA50 usano solo osservazioni della stessa fonte.",
        ],
        "composito": ("Quota di segnali FEAR / RISK-OFF / TAIL RISK sul totale dei segnali disponibili (N/D esclusi): "
                      ">= 55% = RISK-OFF; >= 35% = CAUTO. Altrimenti quota GREED / RISK-ON / COMPLACENCY: "
                      ">= 55% = RISK-ON; >= 35% = POSITIVO. In tutti gli altri casi NEUTRO."),
        "indice_regime": [
            "Ogni segnale vale +1 (RISK-ON/GREED), 0 (NEUTRAL) o -1 (FEAR/RISK-OFF), moltiplicato per il suo peso.",
            "Indice = somma pesata / somma dei pesi dei soli segnali disponibili, x100 (intervallo -100 / +100).",
            "I dati mancanti (N/D) sono esclusi dal calcolo. Se i segnali disponibili pesano meno del "
            + str(int(MIN_COVERAGE * 100)) + "% del totale, l'orizzonte è N/D (dati insufficienti).",
            "Quote Risk-On / Neutro / Risk-Off: |indice| viene attribuito alla direzione del segno, il resto è Neutro. "
            "Non sono probabilità statistiche.",
        ],
        "pesi": {h: {"orizzonte": s["orizzonte"],
                     "componenti": [{"nome": LABELS.get(k, k), "peso": w} for k, _, w in s["componenti"]]}
                 for h, s in HORIZONS.items()},
        "regimi": [f"indice >= {thr:+d}: {name}" for thr, name in REGIME_STEPS]
                  + [f"indice < {REGIME_STEPS[-1][0]:+d}: RISK-OFF"],
        "qualita_dati": [
            "P/C CBOE: valori fuori range o identici agli ultimi 3 giorni vengono scartati (dato mancante, non sostituito).",
            "VSTOXX: solo indice ^V2TX; nessuna sostituzione con volatilità realizzata.",
            "HYG/IEI: variazioni > 15% rispetto all'ultimo valore della stessa fonte vengono scartate.",
            "La pagina mostra le serie mancanti nella riga 'Qualità dati'.",
        ],
        "limiti": [
            "Non è una previsione di prezzo né una probabilità statistica: è un indice sintetico di regime.",
            "STOXX50/SPY e STOXX50/EEM dividono un indice in punti per un ETF in USD: misurano la tendenza, non un rapporto puro.",
            "BTP/Bund non è implementato (sempre N/D).",
            "I segnali non costituiscono consulenza finanziaria né raccomandazione di investimento.",
        ],
        "rotazione": ROTAZIONE,
    }

# ─── ROTAZIONE GEOGRAFICA (parametri e regole usati da rotazione.html) ────────
ROTAZIONE = {
    "finestre": {"1M": 21, "3M": 63, "6M": 126},   # in sedute di mercato
    "soglia_pp": 1.0,                               # distanza minima 1°-2° a 3M per parlare di leadership
    "valuta": "EUR",
    "serie": [
        "USA = SPY (USD); Europa = indice Euro Stoxx 50 (EUR); Emergenti = EEM (USD).",
        "SPY si ricava da STOXX50 / (STOXX50/SPY); EEM da (EEM/SPY) x SPY. Le serie in USD sono convertite in EUR dividendo per EUR/USD.",
        "Non sono strumenti investibili in EUR: sono una misura di tendenza delle tre aree.",
    ],
    "calcolo": [
        "Finestre in sedute di mercato: 1M = 21, 3M = 63, 6M = 126.",
        "Performance in EUR = valore oggi / valore N sedute fa - 1.",
        "Performance vs USA = (1 + performance dell'area) / (1 + performance USA) - 1.",
        "Il rango (1°, 2°, 3°) ordina le tre aree per performance in EUR su ciascun orizzonte.",
        "Se manca un dato nelle date usate, l'orizzonte è N/D.",
    ],
    "verdetto": [
        "Nessuna leadership chiara: a 3M la distanza tra la prima e la seconda area è inferiore alla soglia (1 punto percentuale).",
        "Leadership consolidata: la stessa area è prima a 1M, 3M e 6M.",
        "Possibile rotazione in corso: la prima area a 1M è diversa dalla prima a 3M.",
        "Leadership recente: 1M e 3M hanno la stessa prima area, 6M un'altra.",
        "Il verdetto descrive la tendenza passata: non è una previsione.",
    ],
    "limiti": [
        "L'indice Euro Stoxx 50 comprende solo 50 titoli: non rappresenta tutta l'Europa.",
        "Le performance non includono costi, tasse né coperture valutarie degli strumenti reali.",
        "I segnali non costituiscono consulenza finanziaria né raccomandazione di investimento.",
    ],
}

# ─── MAIN ─────────────────────────────────────────────────────

def main():
    global REF_DATE
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M")

    print(f"\n{'='*55}")
    print(f"{VERSION} — {ts} UTC")
    print(f"{'='*55}")

    # Data di riferimento = ultima barra di mercato (non l'orologio)
    ref = None
    for t in ("^VIX", "SPY", "^GSPC"):
        b = fetch_bars(t)
        if b:
            ref = b[-1][0]
            break
    if ref is None:
        print("  ✗ Yahoo non raggiungibile: nessun aggiornamento")
        sys.exit(1)
    REF_DATE = ref
    print(f"  Data di riferimento (ultima barra): {ref}")

    existing = {}
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    history = existing.get("history", [])

    print("\n[0/4] Curva 10Y-2Y (FRED)...")
    fred = fetch_fred()

    if "--rebuild" in sys.argv:
        rebuilt = rebuild_history(fred)
        if rebuilt:
            history = rebuilt
        else:
            print("  Rebuild non eseguito: storico invariato")

    n_yc = normalize_yc(history, fred)
    if n_yc:
        print(f"  ✓ Curva 10Y-2Y allineata a FRED su {n_yc} righe")

    # ── 1. Sentiment / 2. Intermarket ────────────────────────
    print("\n[1/4] Sentiment...")
    cboe = fetch_cboe_putcall(history, ref)

    print("\n[2/4] Intermarket...")
    yc  = fred_valid(asof(fred, ref, 7)) if fred else None
    hyg = sanity_hyg(*hyg_from(fetch_last), history, ref)
    record = build_record(ref, ts, fetch_last, cboe, yc, hyg)
    print(f"  VIX={record['vix']} | SKEW={record['skew']} | VSTOXX={record['vstoxx']}")
    print(f"  HYG/IEI={record['hyg_iei']} ({record['hyg_iei_src']}) | Cu/Au={record['copper_gold']} | YC={yc}")

    # Controllo minimo: se i dati core mancano non si scrive nulla
    core = ["vix", "skew", "stoxx50", "eem_spy", "copper_gold", "tip_ief", "dxy", "eurusd"]
    if sum(record.get(k) is not None for k in core) < 6:
        print("  ✗ Dati core insufficienti (<6/8): nessun aggiornamento")
        sys.exit(1)

    # ── 3. Storico ───────────────────────────────────────────
    history = [r for r in history if r.get("date") != ref]
    history.append(record)
    history.sort(key=lambda r: r["date"])
    history = history[-MAX_HISTORY:]

    # ── 4. Segnali ───────────────────────────────────────────
    print("\n[3/4] Signals...")

    def mk_signal(field, inverted=True, src_field=None):
        cur = record
        rows = history
        if src_field:
            cs = cur.get(src_field)
            rows = [r for r in history if r.get(src_field) == cs] if cs else [cur]
        vals    = [r.get(field) for r in rows]
        current = cur.get(field)
        pct     = percentile_rank(vals, current)
        sig     = signal_from_percentile(pct, inverted)
        ma50    = ma(vals, 50)
        trend   = None
        if current is not None and ma50:
            trend = "SOPRA MA50" if current > ma50 else "SOTTO MA50"
        return {"value": current, "percentile": pct, "signal": sig, "ma50": ma50, "trend": trend}

    nd = {"value": None, "percentile": None, "signal": "N/D", "ma50": None, "trend": None}
    signals = {k: mk_signal(k, inv) for k, _, inv, _ in SENTIMENT}
    ratios_signals = {}
    for k, _, inv, _ in RATIOS:
        ratios_signals[k] = dict(nd) if k == "btp_bund" else mk_signal(k, inv, SRC_FIELDS.get(k))

    # Composito
    all_sigs = list(signals.values()) + list(ratios_signals.values())
    fear_n   = sum(1 for s in all_sigs if s["signal"] in ("FEAR", "RISK-OFF", "TAIL RISK"))
    greed_n  = sum(1 for s in all_sigs if s["signal"] in ("GREED", "RISK-ON", "COMPLACENCY"))
    total_n  = len([s for s in all_sigs if s["signal"] != "N/D"])
    if total_n == 0:               composite = "N/D"
    elif fear_n / total_n >= 0.55: composite = "RISK-OFF"
    elif fear_n / total_n >= 0.35: composite = "CAUTO"
    elif greed_n / total_n >= 0.55: composite = "RISK-ON"
    elif greed_n / total_n >= 0.35: composite = "POSITIVO"
    else:                          composite = "NEUTRO"

    print("\n[4/4] Indice di regime...")
    projection = compute_projection(signals, ratios_signals)

    # Qualità dati
    expected = [k for k in record if k not in ("date", "ts", "hyg_iei_src", "yield_curve_src")]
    missing  = [k for k in expected if record.get(k) is None]
    data_quality = {
        "disponibili": len(expected) - len(missing), "totale": len(expected), "mancanti": missing,
        "fonti": {"hyg_iei": record.get("hyg_iei_src"), "yield_curve": record.get("yield_curve_src")},
    }

    # ── Salva ────────────────────────────────────────────────
    output = {
        "updated": ts, "date": ref, "composite": composite,
        "latest": record, "signals": signals,
        "ratios": ratios_signals, "projection": projection,
        "data_quality": data_quality, "rules": build_rules(),
        "history": history,
    }
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n{'='*55}")
    print(f"COMPOSITO: {composite} | DATI: {data_quality['disponibili']}/{data_quality['totale']} OK | mancanti: {', '.join(missing) or '-'}")
    print(f"VIX={record['vix']} | VSTOXX={record['vstoxx']} | P/C Eq={record['pc_equity']}")
    print(f"HYG/IEI={record['hyg_iei']} ({record['hyg_iei_src']}) | Cu/Au={record['copper_gold']} | YC={record['yield_curve']}")
    print("INDICE: " + " / ".join(f"{h}={p['regime']}({p['score']}, cop. {p['copertura']}%)" for h, p in projection.items()))
    print(f"Storico: {len(history)} | {DATA_FILE}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
