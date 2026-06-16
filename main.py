"""
╔══════════════════════════════════════════════════════╗
║       NIFTY50 ZONE ASSISTANT BOT — V2               ║
║       Smart Assistant | Manual Trade Confirm         ║
║       Output: BUY CE / BUY PE / WAIT                ║
╚══════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════
# SECTION 1: IMPORTS + CONFIG
# ═══════════════════════════════════════════════════════

import os
import json
import math
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import quote
import pytz
from dotenv import load_dotenv
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()

# ── Logging ──────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)
log = logging.getLogger("ZoneBot")

# ── Timezone ─────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ── ENV Variables ─────────────────────────────────────
UPSTOX_TOKEN  = os.getenv("UPSTOX_ANALYTICS_TOKEN", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Upstox ───────────────────────────────────────────
NIFTY_KEY     = "NSE_INDEX|Nifty 50"
NIFTY_KEY_ENC = quote("NSE_INDEX|Nifty 50")
UPSTOX_V3     = "https://api.upstox.com/v3"
UPSTOX_HDRS   = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json"
}

# ── Bot Settings ──────────────────────────────────────
ZONE_NEAR_PTS       = int(os.getenv("ZONE_NEAR_POINTS",    "25"))
WAIT_COOLDOWN_MIN   = int(os.getenv("WAIT_COOLDOWN_MIN",   "15"))
SIGNAL_COOLDOWN_MIN = int(os.getenv("SIGNAL_COOLDOWN_MIN", "30"))
MAX_ZONES           = int(os.getenv("MAX_ZONES",           "12"))
MAX_AI_RAW_LOG      = int(os.getenv("MAX_AI_RAW_LOG",     "100"))
MIN_CONFIRMATIONS   = int(os.getenv("MIN_CONFIRMATIONS",    "2"))

# ── AI Model ─────────────────────────────────────────
HAIKU_MODEL = "claude-haiku-4-5-20251001"


# ═══════════════════════════════════════════════════════
# SECTION 2: UPSTOX DATA FETCH
# ═══════════════════════════════════════════════════════

def upstox_get(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UPSTOX_HDRS, timeout=15)
            if r.status_code == 200:
                d = r.json()
                if d.get("status") == "success":
                    return d.get("data", {})
            elif r.status_code == 429:
                time.sleep(2 ** attempt)
        except Exception as e:
            log.error(f"API error: {e}")
        time.sleep(1)
    return None


def get_ltp():
    url  = f"{UPSTOX_V3}/market-quote/ltp?instrument_key={NIFTY_KEY_ENC}"
    data = upstox_get(url)
    if data:
        first = next(iter(data.values()), None)
        if first and "last_price" in first:
            return float(first["last_price"])
    log.error("LTP fetch failed")
    return None


def fetch_historical(unit, interval, candles_needed):
    """Fetch historical OHLC — includes yesterday + older data"""
    now     = datetime.now(IST)
    to_date = now.strftime("%Y-%m-%d")

    if unit == "days":
        from_date = (now - timedelta(days=candles_needed + 10)).strftime("%Y-%m-%d")
    elif unit == "hours":
        from_date = (now - timedelta(days=15)).strftime("%Y-%m-%d")
    else:  # minutes
        # Fetch last 3 days → covers yesterday + today
        from_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

    url  = (f"{UPSTOX_V3}/historical-candle/"
            f"{NIFTY_KEY_ENC}/{unit}/{interval}/{to_date}/{from_date}")
    data = upstox_get(url)
    if not data or "candles" not in data:
        return pd.DataFrame()

    df = pd.DataFrame(
        data["candles"],
        columns=["ts", "o", "h", "l", "c", "v", "oi"]
    )
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)
    return df.tail(candles_needed)


def fetch_intraday(unit, interval):
    """Fetch today's intraday OHLC"""
    url  = (f"{UPSTOX_V3}/historical-candle/intraday/"
            f"{NIFTY_KEY_ENC}/{unit}/{interval}")
    data = upstox_get(url)
    if not data or "candles" not in data:
        return pd.DataFrame()

    df = pd.DataFrame(
        data["candles"],
        columns=["ts", "o", "h", "l", "c", "v", "oi"]
    )
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)
    return df


def fetch_all_data():
    """
    Fetch all TF data.
    Works for any start time — 9:20 or 12:00.
    15M/5M historical gives yesterday + today candles.
    """
    log.info("Fetching all TF data...")

    daily  = fetch_historical("days",    1, 15)
    hourly = fetch_historical("hours",   1, 30)

    # 15M → historical last 3 days = yesterday + today context
    m15_hist = fetch_historical("minutes", 15, 60)

    # 5M → intraday (today) + historical fallback
    m5 = fetch_intraday("minutes", 5)
    if m5.empty or len(m5) < 10:
        log.warning("Intraday 5M empty → using historical")
        m5 = fetch_historical("minutes", 5, 80)

    return {
        "daily":  daily,
        "hourly": hourly,
        "m15":    m15_hist,
        "m5":     m5
    }


def fetch_zone_decision_data():
    """Light fetch for zone decision — 15M + 5M only"""
    m15 = fetch_intraday("minutes", 15)
    m5  = fetch_intraday("minutes", 5)

    # Fallback to historical if intraday empty
    if m15.empty:
        m15 = fetch_historical("minutes", 15, 30)
    if m5.empty:
        m5  = fetch_historical("minutes", 5,  40)

    return {"m15": m15, "m5": m5}


# ═══════════════════════════════════════════════════════
# SECTION 3: DATA COMPRESS
# ═══════════════════════════════════════════════════════

def get_base(df):
    if df.empty:
        return 24000
    return math.floor(int(df["l"].min()) / 500) * 500


def compress_ohlc(df, base, max_candles=None):
    """O H L C per line — delta encoded"""
    if df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    lines = []
    for _, row in d.iterrows():
        lines.append(
            f"{int(row['o'])-base} {int(row['h'])-base} "
            f"{int(row['l'])-base} {int(row['c'])-base}"
        )
    return "\n".join(lines)


def compress_hl(df, base, max_candles=None):
    """H L per line — for daily structure"""
    if df.empty:
        return "N/A"
    d = df.tail(max_candles) if max_candles else df
    lines = []
    for _, row in d.iterrows():
        lines.append(f"{int(row['h'])-base} {int(row['l'])-base}")
    return "\n".join(lines)


def drop_incomplete_candle(df, interval_minutes=5, buffer_sec=10):
    """Remove last candle if still forming"""
    if df.empty or len(df) < 2:
        return df
    try:
        last_ts = pd.Timestamp(df.iloc[-1]["ts"])
        if last_ts.tzinfo is None:
            last_ts = IST.localize(last_ts.to_pydatetime())
        else:
            last_ts = last_ts.tz_convert(IST).to_pydatetime()

        complete_at = last_ts + timedelta(
            minutes=interval_minutes, seconds=buffer_sec
        )
        if datetime.now(IST) < complete_at:
            log.info(f"Dropped incomplete {interval_minutes}M candle: {last_ts.strftime('%H:%M')}")
            return df.iloc[:-1].copy()
    except Exception as e:
        log.error(f"drop_incomplete_candle: {e}")
    return df


def build_first_analysis_string(tf_data):
    """Build compressed data string for first analysis + hourly reanalysis"""
    daily  = tf_data.get("daily",  pd.DataFrame())
    hourly = tf_data.get("hourly", pd.DataFrame())
    m15    = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5     = drop_incomplete_candle(tf_data.get("m5",  pd.DataFrame()), 5)

    if daily.empty:
        return None

    d_base  = get_base(daily)
    h_base  = get_base(hourly)
    m15_base= get_base(m15)
    m5_base = get_base(m5)

    # Pre-calc: PDH/PDL
    pdc_row = None
    try:
        today = datetime.now(IST).date()
        last_ts = pd.to_datetime(daily.iloc[-1]["ts"]).date()
        pdc_row = daily.iloc[-2] if last_ts == today else daily.iloc[-1]
    except Exception:
        pass

    pdh = int(pdc_row["h"]) if pdc_row is not None else 0
    pdl = int(pdc_row["l"]) if pdc_row is not None else 0
    pdc = int(pdc_row["c"]) if pdc_row is not None else 0
    ltp_approx = int(m5.iloc[-1]["c"]) if not m5.empty else 0

    now_str = datetime.now(IST).strftime("%d-%m-%Y %H:%M")

    return f"""=== NIFTY50 ZONE ANALYSIS | {now_str} ===

[CONTEXT - Python Calculated]
PDH:{pdh} | PDL:{pdl} | PDC:{pdc}
LTP_approx:{ltp_approx}

[DAILY - 15 candles | BASE:{d_base}]
(H L per candle | oldest→newest)
{compress_hl(daily, d_base, 15)}

[1H - 30 candles | BASE:{h_base}]
(O H L C per candle | oldest→newest)
{compress_ohlc(hourly, h_base, 30)}

[15M - last 40 candles | BASE:{m15_base}]
(O H L C | oldest→newest)
{compress_ohlc(m15, m15_base, 40)}

[5M - last 60 candles | BASE:{m5_base}]
(O H L C | oldest→newest)
{compress_ohlc(m5, m5_base, 60)}"""


def build_zone_decision_string(tf_data, ltp, touched_zone, all_zones, morning_ctx):
    """Build compact string for zone touch decision"""
    m15 = drop_incomplete_candle(tf_data.get("m15", pd.DataFrame()), 15)
    m5  = drop_incomplete_candle(tf_data.get("m5",  pd.DataFrame()), 5)

    base    = get_base(m5) if not m5.empty else get_base(m15)
    m15_str = compress_ohlc(m15, base, 20)
    m5_str  = compress_ohlc(m5,  base, 30)

    # Other zones context (except touched)
    other = [
        f"  {z['id']}:{z['type']} {z['low']}-{z['high']} [{z['strength']}]"
        for z in all_zones if z.get("id") != touched_zone.get("id")
    ]
    other_str = "\n".join(other[:6]) if other else "None"

    now_str = datetime.now(IST).strftime("%H:%M")
    bias    = morning_ctx.get("bias", "?")
    struct  = morning_ctx.get("structure", "?")

    return f"""=== ZONE DECISION | {now_str} ===

[MORNING CONTEXT]
Bias:{bias} | Structure:{struct}
Day:{morning_ctx.get('day_type','?')}
Summary:{morning_ctx.get('summary','')}

[TOUCHED ZONE]
ID:{touched_zone.get('id')} | Type:{touched_zone.get('type')}
Range:{touched_zone.get('low')}-{touched_zone.get('high')}
Strength:{touched_zone.get('strength')}
Preferred:{touched_zone.get('preferred_action','?')}
Why:{touched_zone.get('why','')}

[OTHER ACTIVE ZONES]
{other_str}

[CURRENT LTP]
{ltp}

[15M - last 20 candles | BASE:{base}]
(O H L C | oldest→newest)
{m15_str}

[5M - last 30 candles | BASE:{base}]
(O H L C | oldest→newest)
{m5_str}"""


# ═══════════════════════════════════════════════════════
# SECTION 4: ZONE MANAGER (Redis)
# ═══════════════════════════════════════════════════════

_memory = {}

def _redis():
    try:
        import redis
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None

_r = _redis()
log.info("✅ Redis connected" if _r else "⚠️ RAM mode")


def _set(key, value, ttl=86400):
    v = json.dumps(value)
    if _r:
        _r.setex(key, ttl, v)
    else:
        _memory[key] = v


def _get(key):
    try:
        raw = _r.get(key) if _r else _memory.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _delete(key):
    if _r:
        _r.delete(key)
    else:
        _memory.pop(key, None)


# Zone CRUD
def save_zones(zones):
    _set("zones", zones[:MAX_ZONES])
    log.info(f"✅ {len(zones)} zones saved")


def get_zones():
    return _get("zones") or []


def save_morning_context(ctx):
    _set("morning_context", ctx)


def get_morning_context():
    return _get("morning_context")


def flush_day():
    for k in ["zones", "morning_context", "signal_history", "ai_raw_log"]:
        _delete(k)
    log.info("🧹 Day data flushed")


# Signal history
def get_signal_history():
    return _get("signal_history") or []


def save_signal_log(entry):
    h = get_signal_history()
    h.append(entry)
    _set("signal_history", h)


# AI raw log (debug)
def save_ai_raw_log(entry):
    logs = _get("ai_raw_log") or []
    logs.append(entry)
    _set("ai_raw_log", logs[-MAX_AI_RAW_LOG:])


# Zone touch check
def get_touched_zone(ltp, zones):
    """Return first zone where LTP is inside or within ZONE_NEAR_PTS"""
    for z in zones:
        low  = z.get("low", 0)
        high = z.get("high", 0)
        if (ltp >= low - ZONE_NEAR_PTS) and (ltp <= high + ZONE_NEAR_PTS):
            return z
    return None


# Cooldown
def zone_cooldown_ok(zone_id):
    key = f"cooldown_{zone_id}"
    cd  = _get(key)
    if cd:
        until = datetime.fromisoformat(cd["until"])
        if datetime.now(IST) < until:
            rem = int((until - datetime.now(IST)).total_seconds() / 60)
            log.info(f"Zone {zone_id} cooldown: {rem}min left")
            return False
    return True


def mark_zone_cooldown(zone_id, signal_type):
    mins = (
        SIGNAL_COOLDOWN_MIN if signal_type in ["BUY_CE", "BUY_PE"]
        else WAIT_COOLDOWN_MIN
    )
    until = (datetime.now(IST) + timedelta(minutes=mins)).isoformat()
    _set(f"cooldown_{zone_id}", {"until": until}, ttl=mins*60+60)
    log.info(f"Zone {zone_id} cooldown set: {mins}min")


# Zone merge — hourly reanalysis
def merge_zones(existing, new_zones, ltp):
    """
    Merge new zones into existing:
    - 50% overlap → replace
    - No overlap → add
    - Too far from LTP (>500 pts) → drop
    - Max MAX_ZONES zones
    """
    def overlap(a, b):
        ol = max(0, min(a["high"], b["high"]) - max(a["low"], b["low"]))
        span_a = max(a["high"] - a["low"], 1)
        return ol / span_a

    result = list(existing)
    for nz in new_zones:
        if abs((nz["low"] + nz["high"]) / 2 - ltp) > 500:
            continue  # too far

        replaced = False
        for i, ez in enumerate(result):
            if overlap(ez, nz) >= 0.5:
                result[i] = nz  # replace with fresh zone
                replaced = True
                break

        if not replaced:
            result.append(nz)

    # Keep max zones, prefer closer to LTP
    result.sort(key=lambda z: abs((z["low"] + z["high"]) / 2 - ltp))
    return result[:MAX_ZONES]


# Track open signals ref analytics
def track_open_signals(ltp):
    """Check if ref_sl or ref_target hit for open signals"""
    history = get_signal_history()
    updated = False
    for sig in history:
        if sig.get("result") != "OPEN":
            continue

        ref_sl  = sig.get("ref_sl", 0)
        ref_tgt = sig.get("ref_target", 0)
        stype   = sig.get("signal", "")

        if stype == "BUY_CE":
            if ref_sl and ltp <= ref_sl:
                sig["result"] = "REF_SL_HIT"
                updated = True
            elif ref_tgt and ltp >= ref_tgt:
                sig["result"] = "REF_TARGET_HIT"
                updated = True
        elif stype == "BUY_PE":
            if ref_sl and ltp >= ref_sl:
                sig["result"] = "REF_SL_HIT"
                updated = True
            elif ref_tgt and ltp <= ref_tgt:
                sig["result"] = "REF_TARGET_HIT"
                updated = True

    if updated:
        _set("signal_history", history)


# ═══════════════════════════════════════════════════════
# SECTION 5: AI CALLS (Haiku)
# ═══════════════════════════════════════════════════════

FIRST_ANALYSIS_SYSTEM = """You are an expert NIFTY50 price-action zone analyst.
The user manually confirms every trade. This is NOT auto-trading.

DATA FORMAT: Each line = one candle (O H L C or H L).
Values are delta-encoded (add BASE to get real price).
Candles: oldest → newest.

TASK: Analyze given multi-timeframe data and create practical intraday trading zones.

Create:
- Support zones
- Resistance zones
- Flip zones (old S now R, or vice versa)
- Liquidity zones (equal highs/lows, sweep targets)
- No-trade / sideways zone if applicable

RULES:
- LTP nearby zones are HIGHEST priority (actionable today)
- Far HTF levels → context only, not main zones
- Use 5M/15M for actionable zone boundaries
- Daily/1H for bias and context only
- Zones = price RANGES (low to high), not single lines
- Max 8–10 useful zones
- If unclear structure → create WAIT/NO_TRADE zone
- Do NOT force bullish or bearish bias

SWEEP/RECLAIM RULE:
If recent candles show price swept below a support then reclaimed it →
mark that area as FLIP or LIQUIDITY zone with preferred_action BUY_CE.
Mirror for bearish: swept above resistance then rejected → BUY_PE zone.

RESPOND ONLY in valid JSON. No text outside JSON."""


ZONE_DECISION_SYSTEM = """You are an expert NIFTY50 intraday directional signal analyst.
The user manually confirms every trade. This is NOT auto-trading.

You receive:
- Morning bias/structure context
- Touched zone details
- All active zones
- Current LTP
- Last completed 15M and 5M candles (delta-encoded)

TASK: Decide ONE output for price touching/entering a saved zone:
  BUY_CE / BUY_PE / WAIT

RULES:
- Focus on the TOUCHED ZONE first
- Check: respecting / rejecting / breaking / trapping around zone
- Use last completed 5M and 15M candles for confirmation
- Do NOT chase big candles already moved
- Weak candle confirmation → WAIT
- Inside sideways/no-trade zone → WAIT
- Setup forming but not confirmed → WAIT + mention "setup forming"
- Need minimum 2 REAL confirmations for BUY_CE or BUY_PE
- confidence LOW → WAIT
- Direction unclear → WAIT. Never force signal.

CHoCH RULE:
Bullish: price swept below support → reclaimed above → minor lower-high broken
→ BUY_CE if last completed 5M close confirms reclaim.
Bearish: price swept above resistance → rejected back below → minor higher-low broken
→ BUY_PE if last completed 5M close confirms rejection.

RESPOND ONLY in valid JSON. No text outside JSON."""


def call_haiku(system_prompt, user_prompt):
    url  = "https://api.anthropic.com/v1/messages"
    hdrs = {
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json"
    }
    body = {
        "model":      HAIKU_MODEL,
        "max_tokens": 1500,
        "system":     system_prompt,
        "messages":   [{"role": "user", "content": user_prompt}]
    }
    try:
        r = requests.post(url, headers=hdrs, json=body, timeout=45)
        if r.status_code == 200:
            return r.json()["content"][0]["text"]
        log.error(f"Haiku error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.error(f"Haiku call failed: {e}")
    return None


def parse_json(raw):
    if not raw:
        return None
    try:
        clean = raw.strip().replace("```json","").replace("```","").strip()
        s = clean.find("{")
        e = clean.rfind("}") + 1
        return json.loads(clean[s:e]) if s != -1 else None
    except Exception as ex:
        log.error(f"JSON parse error: {ex}")
        return None


def run_first_analysis(tf_data, mode="FIRST"):
    """Run first analysis or hourly reanalysis"""
    log.info(f"🌅 Running {mode} analysis...")

    data_str = build_first_analysis_string(tf_data)
    if not data_str:
        log.error("Data build failed")
        return None

    user_prompt = data_str + """

RETURN ONLY this JSON:
{
  "bias": "LONG/SHORT/NEUTRAL",
  "structure": "HH_HL/LH_LL/SIDEWAYS/UNCLEAR",
  "day_type": "TRENDING/RANGING/VOLATILE/UNCLEAR",
  "summary": "short market explanation (1-2 lines)",
  "zones": [
    {
      "id": "S1",
      "type": "SUPPORT/RESISTANCE/FLIP/LIQUIDITY/NO_TRADE",
      "low": 0,
      "high": 0,
      "strength": "STRONG/MED/WEAK",
      "preferred_action": "BUY_CE/BUY_PE/WAIT",
      "why": "short reason"
    }
  ],
  "no_trade_zone": {
    "low": 0,
    "high": 0,
    "why": "short reason"
  }
}"""

    raw    = call_haiku(FIRST_ANALYSIS_SYSTEM, user_prompt)
    result = parse_json(raw)

    save_ai_raw_log({
        "time":   datetime.now(IST).strftime("%H:%M"),
        "mode":   mode,
        "raw":    raw[:500] if raw else None,
        "parsed": bool(result)
    })

    if result:
        log.info(f"✅ {mode} done: {result.get('bias')} | {result.get('day_type')}")
    return result


def run_zone_decision(tf_data, ltp, touched_zone, all_zones, morning_ctx):
    """Run zone touch AI decision"""
    log.info(f"🎯 Zone decision: {touched_zone.get('id')} @ LTP:{ltp}")

    data_str = build_zone_decision_string(
        tf_data, ltp, touched_zone, all_zones, morning_ctx
    )

    user_prompt = data_str + """

RETURN ONLY this JSON:
{
  "signal": "BUY_CE/BUY_PE/WAIT",
  "confidence": "HIGH/MED/LOW",
  "zone_id": "id of touched zone",
  "zone_type": "SUPPORT/RESISTANCE/FLIP/LIQUIDITY/NO_TRADE",
  "zone_reaction": "BOUNCE/REJECTION/BREAKOUT/BREAKDOWN/TRAP/NO_REACTION",
  "confirmations": [],
  "confirmation_count": 0,
  "reason": "short reason (2 lines max)",
  "risk_note": "what user should manually check",
  "message": "short Hinglish summary for Telegram",
  "reference": {
    "ref_sl": 0,
    "ref_target": 0,
    "valid_for_minutes": 45,
    "note": "analytics only, not trade advice"
  }
}"""

    raw    = call_haiku(ZONE_DECISION_SYSTEM, user_prompt)
    result = parse_json(raw)

    save_ai_raw_log({
        "time":    datetime.now(IST).strftime("%H:%M"),
        "mode":    "ZONE_DECISION",
        "zone_id": touched_zone.get("id"),
        "ltp":     ltp,
        "raw":     raw[:500] if raw else None,
        "parsed":  bool(result)
    })

    return result


# ═══════════════════════════════════════════════════════
# SECTION 6: SIGNAL ANALYTICS + EOD
# ═══════════════════════════════════════════════════════

def validate_decision(result, ltp):
    """Basic validation — check confirmations"""
    if not result:
        return False

    sig   = result.get("signal", "WAIT")
    conf  = result.get("confidence", "LOW")
    confs = result.get("confirmations", [])
    count = result.get("confirmation_count", len(confs))

    if sig == "WAIT":
        return True  # WAIT always valid

    if conf == "LOW":
        log.info("Rejected: LOW confidence")
        return False

    # Filter fake confirmations
    fake = {"none","na","n/a","null","","factor1","factor2","reason1","reason2"}
    real = [c for c in confs if str(c).strip().lower() not in fake and len(str(c)) > 5]

    if len(real) < MIN_CONFIRMATIONS:
        log.info(f"Rejected: only {len(real)}/{MIN_CONFIRMATIONS} real confirmations")
        return False

    return True


def run_eod_summary():
    """3:30 PM — send day summary to Telegram"""
    history = get_signal_history()
    today   = datetime.now(IST).strftime("%d %b %Y")

    buy_ce = [s for s in history if s.get("signal") == "BUY_CE"]
    buy_pe = [s for s in history if s.get("signal") == "BUY_PE"]
    waits  = [s for s in history if s.get("signal") == "WAIT"]

    ref_hit = sum(1 for s in history if s.get("result") == "REF_TARGET_HIT")
    ref_sl  = sum(1 for s in history if s.get("result") == "REF_SL_HIT")
    expired = sum(1 for s in history if s.get("result") == "EXPIRED")
    still_open = sum(1 for s in history if s.get("result") == "OPEN")

    # Signal detail lines
    detail = ""
    for s in history:
        r  = s.get("result","?")
        em = ("✅" if r=="REF_TARGET_HIT" else
              "❌" if r=="REF_SL_HIT" else
              "⏰" if r=="EXPIRED" else
              "⏳" if r=="OPEN" else "⚪")
        detail += f"\n{em} {s['time']} {s['signal']} @ {s.get('zone_id','?')} → {r}"

    msg = f"""📈 <b>DAY SUMMARY | {today}</b>

Signals  : {len(history)}
🟢 BUY CE: {len(buy_ce)}
🔴 BUY PE: {len(buy_pe)}
⚪ WAIT  : {len(waits)}

<b>Ref Analytics:</b>
✅ Ref Target: {ref_hit}
❌ Ref SL    : {ref_sl}
⏰ Expired   : {expired}
⏳ Open      : {still_open}
{detail}"""

    tg_send(msg)
    log.info("✅ EOD summary sent")


# ═══════════════════════════════════════════════════════
# SECTION 7: TELEGRAM
# ═══════════════════════════════════════════════════════

def tg_send(text):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.error(f"Telegram error: {e}")


def send_zone_brief(ctx, zones):
    """Morning/hourly zone brief"""
    today = datetime.now(IST).strftime("%d %b %Y %H:%M")
    bias  = ctx.get("bias","?")
    struct= ctx.get("structure","?")
    dtype = ctx.get("day_type","?")
    summ  = ctx.get("summary","")

    zone_lines = ""
    for z in zones[:8]:
        em = ("🟢" if z.get("preferred_action")=="BUY_CE" else
              "🔴" if z.get("preferred_action")=="BUY_PE" else "⚪")
        zone_lines += (
            f"\n{em} {z['id']}: {z['low']}–{z['high']} "
            f"[{z['strength']}] → {z.get('preferred_action','?')}"
            f"\n   {z.get('why','')}"
        )

    nt  = ctx.get("no_trade_zone",{})
    nt_str = (f"\n❌ No Trade: {nt.get('low')}–{nt.get('high')}"
              if nt and nt.get("low") else "")

    msg = f"""📊 <b>NIFTY ZONES | {today}</b>

Bias      : {bias}
Structure : {struct}
Day Type  : {dtype}

{summ}

<b>Active Zones:</b>{zone_lines}{nt_str}

Manual chart confirm karach trade ghe!"""

    tg_send(msg)
    log.info("✅ Zone brief sent")


def send_signal(result, ltp, touched_zone):
    """Send zone decision to Telegram"""
    sig   = result.get("signal","WAIT")
    conf  = result.get("confidence","?")
    ztype = result.get("zone_type","?")
    react = result.get("zone_reaction","?")
    reason= result.get("reason","")
    risk  = result.get("risk_note","")
    msg_h = result.get("message","")
    confs = result.get("confirmations",[])

    ref   = result.get("reference",{})
    ref_sl  = ref.get("ref_sl",0)
    ref_tgt = ref.get("ref_target",0)

    emoji = ("🟢" if sig=="BUY_CE" else
             "🔴" if sig=="BUY_PE" else "⚪")

    conf_lines = "\n".join([f"  ✔ {c}" for c in confs[:5] if c])
    conf_str   = f"\n{conf_lines}" if conf_lines else ""

    ref_str = ""
    if sig != "WAIT" and (ref_sl or ref_tgt):
        ref_str = f"\n\n📊 <i>Ref Analytics (not advice):</i>\nRef SL:{ref_sl} | Ref T:{ref_tgt}"

    now_str = datetime.now(IST).strftime("%H:%M")

    msg = f"""{emoji} <b>{sig} | {now_str}</b>

LTP       : {ltp}
Zone      : {touched_zone.get('id')} ({touched_zone.get('low')}–{touched_zone.get('high')})
Reaction  : {react}
Confidence: {conf}

Confirmations:{conf_str}

Reason    : {reason}
Risk Note : {risk}

{msg_h}{ref_str}

<i>Manual chart check karunch trade ghe!</i>"""

    tg_send(msg)
    log.info(f"✅ Signal sent: {sig}")


# ═══════════════════════════════════════════════════════
# SECTION 8: ORCHESTRATOR
# ═══════════════════════════════════════════════════════

def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    from datetime import time as dtime
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


def morning_job():
    """
    9:20 IST — First Analysis.
    If bot starts at 12 PM, same function runs immediately.
    Uses yesterday + today candles automatically.
    """
    log.info("=" * 50)
    log.info("🌅 FIRST ANALYSIS JOB")
    log.info("=" * 50)
    try:
        tf_data = fetch_all_data()
        result  = run_first_analysis(tf_data, mode="FIRST")

        if not result:
            tg_send("⚠️ First analysis failed — check logs")
            return

        zones = result.get("zones", [])
        save_morning_context(result)
        save_zones(zones)
        send_zone_brief(result, zones)

    except Exception as e:
        log.error(f"Morning job error: {e}")
        tg_send(f"⚠️ Morning job error: {str(e)[:100]}")


def hourly_job():
    """Every hour at :02 — Reanalysis + zone merge"""
    if not is_market_open():
        return
    log.info("🔄 HOURLY REANALYSIS")
    try:
        ltp = get_ltp()
        if not ltp:
            return

        tf_data = fetch_all_data()
        result  = run_first_analysis(tf_data, mode="HOURLY")
        if not result:
            return

        # Merge zones
        existing  = get_zones()
        new_zones = result.get("zones", [])
        merged    = merge_zones(existing, new_zones, ltp)

        # Update context
        ctx = get_morning_context() or {}
        ctx.update({
            "bias":      result.get("bias", ctx.get("bias")),
            "structure": result.get("structure", ctx.get("structure")),
            "day_type":  result.get("day_type", ctx.get("day_type")),
            "summary":   result.get("summary", ctx.get("summary"))
        })

        save_morning_context(ctx)
        save_zones(merged)

        now_str = datetime.now(IST).strftime("%H:%M")
        tg_send(
            f"🔄 <b>Zone Update | {now_str}</b>\n"
            f"Bias:{result.get('bias')} | {len(merged)} zones active"
        )
        log.info(f"✅ Hourly done: {len(merged)} zones")

    except Exception as e:
        log.error(f"Hourly job error: {e}")


def zone_monitor_job():
    """Every 5:10 — Check if LTP touches a saved zone"""
    if not is_market_open():
        return

    now = datetime.now(IST)
    if now.hour == 9 and now.minute < 25:
        return

    try:
        # Track ref analytics (no AI)
        ltp = get_ltp()
        if not ltp:
            return
        track_open_signals(ltp)

        # Check morning context
        morning_ctx = get_morning_context()
        if not morning_ctx:
            log.warning("No morning context — running first analysis")
            morning_job()
            morning_ctx = get_morning_context()
            if not morning_ctx:
                return

        log.info(f"📍 LTP: {ltp}")

        # Zone touch check (FREE)
        zones = get_zones()
        if not zones:
            log.info("No zones saved yet")
            return

        touched = get_touched_zone(ltp, zones)
        if not touched:
            log.info("No zone touched")
            return

        zone_id = touched.get("id", "?")
        log.info(f"🎯 Zone touched: {zone_id} ({touched.get('low')}-{touched.get('high')})")

        # Cooldown check
        if not zone_cooldown_ok(zone_id):
            return

        # Fetch data + run AI
        tf_data = fetch_zone_decision_data()
        result  = run_zone_decision(
            tf_data, ltp, touched, zones, morning_ctx
        )

        if not result:
            return

        # Validate
        if not validate_decision(result, ltp):
            tg_send(
                f"⚠️ Signal rejected: low confidence / weak confirmations\n"
                f"Zone: {zone_id} | LTP: {ltp}"
            )
            mark_zone_cooldown(zone_id, "WAIT")
            return

        sig = result.get("signal", "WAIT")

        # Set cooldown
        mark_zone_cooldown(zone_id, sig)

        # Save to history
        ref = result.get("reference", {})
        save_signal_log({
            "time":       now.strftime("%H:%M"),
            "zone_id":    zone_id,
            "signal":     sig,
            "confidence": result.get("confidence"),
            "ref_sl":     ref.get("ref_sl", 0),
            "ref_target": ref.get("ref_target", 0),
            "result":     "OPEN" if sig != "WAIT" else "WAIT_SENT"
        })

        # Send Telegram
        send_signal(result, ltp, touched)

    except Exception as e:
        log.error(f"Zone monitor error: {e}")


def closing_job():
    """3:30 PM — EOD summary + flush"""
    log.info("🔔 CLOSING JOB")
    run_eod_summary()
    flush_day()


# ═══════════════════════════════════════════════════════
# SECTION 9: FLASK + SCHEDULER + MAIN
# ═══════════════════════════════════════════════════════

def main():
    log.info("╔══════════════════════════════════════════╗")
    log.info("║  NIFTY50 ZONE ASSISTANT BOT Starting...  ║")
    log.info("╚══════════════════════════════════════════╝")

    if not UPSTOX_TOKEN:
        log.error("❌ UPSTOX_ANALYTICS_TOKEN missing!")
        return
    if not ANTHROPIC_KEY:
        log.error("❌ ANTHROPIC_API_KEY missing!")
        return
    if not TG_BOT_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN missing!")
        return

    now = datetime.now(IST)
    tg_send(
        f"🚀 <b>Zone Assistant Bot Started!</b>\n"
        f"Time: {now.strftime('%H:%M IST')}\n"
        f"Running first analysis now..."
    )

    # Run first analysis immediately on start
    # Works whether bot starts at 9:20 or 12:00
    if is_market_open():
        morning_job()

    # ── Scheduler ────────────────────────────────────
    scheduler = BackgroundScheduler(timezone=IST)

    # First analysis — 9:20 IST
    scheduler.add_job(
        morning_job, "cron",
        hour=9, minute=20, second=0,
        id="morning"
    )

    # Hourly reanalysis — :02 of each hour
    scheduler.add_job(
        hourly_job, "cron",
        minute=2, second=0,
        hour="10,11,12,13,14",
        id="hourly"
    )

    # Zone monitor — every 5 min at :10 sec
    scheduler.add_job(
        zone_monitor_job, "cron",
        minute="0,5,10,15,20,25,30,35,40,45,50,55",
        second=10,
        hour="9,10,11,12,13,14,15",
        id="zone_monitor"
    )

    # EOD — 3:30 PM
    scheduler.add_job(
        closing_job, "cron",
        hour=15, minute=30, second=0,
        id="closing"
    )

    scheduler.start()
    log.info("✅ Scheduler started")
    log.info("   First analysis : 9:20 IST")
    log.info("   Hourly reanalysis : :02 of each hour")
    log.info("   Zone monitor : Every 5min :10sec")
    log.info("   EOD summary : 15:30 IST")

    # ── Flask ─────────────────────────────────────────
    flask_app = Flask(__name__)

    @flask_app.route("/")
    def index():
        return "NIFTY50 Zone Assistant Bot ✅", 200

    @flask_app.route("/health")
    def health():
        ctx    = get_morning_context()
        zones  = get_zones()
        history= get_signal_history()
        return {
            "status":          "ok",
            "time_ist":        datetime.now(IST).strftime("%d-%m-%Y %H:%M:%S"),
            "morning_context": "loaded" if ctx else "missing",
            "bias":            ctx.get("bias","?") if ctx else "?",
            "zones_active":    len(zones),
            "signals_today":   len(history),
            "scheduler":       "running" if scheduler.running else "stopped"
        }, 200

    port = int(os.getenv("PORT", 8000))
    log.info(f"🌐 Flask on port {port}")
    flask_app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
