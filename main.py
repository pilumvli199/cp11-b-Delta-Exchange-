"""
╔══════════════════════════════════════════════════════╗
║         NIFTY50 AI TELEGRAM TRADING BOT              ║
║         Pure Price Action + Claude Haiku             ║
║         Upstox V3 API + Redis + APScheduler          ║
╚══════════════════════════════════════════════════════╝
"""

# ═══════════════════════════════════════════════════════
# SECTION 1: IMPORTS + LOGGING + CONFIG
# ═══════════════════════════════════════════════════════

import os
import json
import math
import time
import logging
import threading
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
log = logging.getLogger("NiftyBot")

# ── Timezone ─────────────────────────────────────────
IST = pytz.timezone("Asia/Kolkata")

# ── ENV Variables ─────────────────────────────────────
UPSTOX_TOKEN    = os.getenv("UPSTOX_ANALYTICS_TOKEN", "")
ANTHROPIC_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
TG_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID", "")
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Upstox Constants ──────────────────────────────────
NIFTY_KEY       = "NSE_INDEX|Nifty 50"
NIFTY_KEY_ENC   = quote("NSE_INDEX|Nifty 50")
UPSTOX_V3       = "https://api.upstox.com/v3"
UPSTOX_V2       = "https://api.upstox.com/v2"

UPSTOX_HEADERS  = {
    "Authorization": f"Bearer {UPSTOX_TOKEN}",
    "Accept": "application/json"
}

# ── TF Candle Counts ──────────────────────────────────
WEEKLY_COUNT    = 15
DAILY_OLD       = 35
DAILY_RECENT    = 15
HOURLY_COUNT    = 30
M15_MORNING     = 20
M5_MORNING      = 3

INTRADAY_M15    = 10
INTRADAY_M5     = 20

# ── Trigger Settings (env configurable) ──────────────
SR_THRESHOLD    = 0.003
MAX_DAY_SIGNALS = int(os.getenv("MAX_DAY_SIGNALS",    "6"))
SIGNAL_EXPIRY   = int(os.getenv("SIGNAL_EXPIRY",      "45"))
MIN_RR          = float(os.getenv("MIN_RR",           "1.5"))
MAX_ENTRY_DIST  = float(os.getenv("MAX_ENTRY_DISTANCE","40"))


# ═══════════════════════════════════════════════════════
# SECTION 2: UPSTOX DATA FETCHER
# ═══════════════════════════════════════════════════════

def upstox_get(url, retries=3):
    """Retry logic for Upstox API calls"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=UPSTOX_HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    return data.get("data", {})
            elif resp.status_code == 429:
                log.warning(f"Rate limit, waiting {2**attempt}s...")
                time.sleep(2 ** attempt)
            else:
                log.warning(f"API error {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            log.error(f"Request error: {e}")
        time.sleep(1)
    return None


def get_ltp():
    """Get current Nifty50 LTP — dynamic key parse"""
    url = f"{UPSTOX_V3}/market-quote/ltp?instrument_key={NIFTY_KEY_ENC}"
    data = upstox_get(url)
    if data:
        first = next(iter(data.values()), None)
        if first and "last_price" in first:
            ltp = float(first["last_price"])
            log.info(f"✅ LTP: {ltp}")
            return ltp
    log.error(f"LTP fetch failed | response: {data}")
    return None


def fetch_historical(unit, interval, candles_needed):
    """
    Fetch historical candles using Upstox V3 API
    unit: 'weeks' | 'days' | 'hours' | 'minutes'
    interval: 1, 5, 15, etc.
    """
    now     = datetime.now(IST)
    to_date = now.strftime("%Y-%m-%d")

    if unit == "weeks":
        from_date = (now - timedelta(weeks=candles_needed + 5)).strftime("%Y-%m-%d")
    elif unit == "days":
        from_date = (now - timedelta(days=candles_needed + 10)).strftime("%Y-%m-%d")
    elif unit == "hours":
        from_date = (now - timedelta(days=15)).strftime("%Y-%m-%d")
    else:  # minutes
        from_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    url = (
        f"{UPSTOX_V3}/historical-candle/"
        f"{NIFTY_KEY_ENC}/{unit}/{interval}/"
        f"{to_date}/{from_date}"
    )

    data = upstox_get(url)
    if not data or "candles" not in data:
        log.error(f"Historical fetch failed: {unit}/{interval}")
        return pd.DataFrame()

    candles = data["candles"]
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "o", "h", "l", "c", "v", "oi"])
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)

    return df.tail(candles_needed)


def fetch_intraday(unit, interval):
    """Fetch today's intraday candles using V3"""
    url = (
        f"{UPSTOX_V3}/historical-candle/intraday/"
        f"{NIFTY_KEY_ENC}/{unit}/{interval}"
    )

    data = upstox_get(url)
    if not data or "candles" not in data:
        log.error(f"Intraday fetch failed: {unit}/{interval}")
        return pd.DataFrame()

    candles = data["candles"]
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles, columns=["ts", "o", "h", "l", "c", "v", "oi"])
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values("ts").reset_index(drop=True)
    df[["o","h","l","c"]] = df[["o","h","l","c"]].round(0).astype(int)

    return df


def fetch_all_tf_morning():
    """Fetch all timeframes for morning analysis"""
    log.info("Fetching all TF data for morning analysis...")
    return {
        "weekly": fetch_historical("weeks",   1,  WEEKLY_COUNT),
        "daily":  fetch_historical("days",    1,  DAILY_OLD + DAILY_RECENT),
        "hourly": fetch_historical("hours",   1,  HOURLY_COUNT),
        "m15":    fetch_historical("minutes", 15, M15_MORNING),
        "m5":     fetch_intraday("minutes",   5),
    }


def fetch_all_tf_intraday():
    """Fetch timeframes for intraday analysis"""
    log.info("Fetching intraday TF data...")
    return {
        "m15": fetch_intraday("minutes", 15),
        "m5":  fetch_intraday("minutes", 5),
    }


# ═══════════════════════════════════════════════════════
# SECTION 3: DATA COMPRESSOR
# ═══════════════════════════════════════════════════════

def get_base(df):
    """Dynamic base = nearest 500 below min low"""
    if df.empty:
        return 24000
    min_low = int(df["l"].min())
    return math.floor(min_low / 500) * 500


def compress_hl(df, base):
    """H,L only — for weekly + old daily (structure only)"""
    lines = []
    for _, row in df.iterrows():
        h = int(row["h"]) - base
        l = int(row["l"]) - base
        lines.append(f"{h} {l}")
    return "\n".join(lines)


def compress_ohlc(df, base):
    """Full O,H,L,C — for recent candles"""
    lines = []
    for _, row in df.iterrows():
        o = int(row["o"]) - base
        h = int(row["h"]) - base
        l = int(row["l"]) - base
        c = int(row["c"]) - base
        lines.append(f"{o} {h} {l} {c}")
    return "\n".join(lines)


def pre_calculate(weekly_df, daily_df):
    """Python calculates structure data — saves AI tokens"""
    if daily_df.empty or weekly_df.empty:
        return {}

    if len(daily_df) < 3 or len(weekly_df) < 3:
        log.warning(
            f"pre_calculate: not enough candles "
            f"(daily={len(daily_df)}, weekly={len(weekly_df)}) — skipping"
        )
        return {}

    try:
        today_date = datetime.now(IST).date()
        last_ts    = pd.to_datetime(daily_df.iloc[-1]["ts"])
        last_date  = last_ts.date() if hasattr(last_ts, 'date') else today_date

        if last_date == today_date:
            prev_row    = daily_df.iloc[-2]
            current_row = daily_df.iloc[-1]
            log.info("Daily: today's candle detected → using iloc[-2] as prev day")
        else:
            prev_row    = daily_df.iloc[-1]
            current_row = daily_df.iloc[-1]
            log.info("Daily: no today's candle → using iloc[-1] as prev day")

        pdh = int(prev_row["h"])
        pdl = int(prev_row["l"])
        pdc = int(prev_row["c"])

        pwh = int(weekly_df.iloc[-2]["h"])
        pwl = int(weekly_df.iloc[-2]["l"])
        pwc = int(weekly_df.iloc[-2]["c"])

        last50     = daily_df.tail(50)
        range_high = int(last50["h"].max())
        range_low  = int(last50["l"].min())
        range_mid  = int((range_high + range_low) / 2)
        current    = int(current_row["c"])
        range_pct  = round((current - range_low) /
                           max(range_high - range_low, 1) * 100, 1)

        is_ranging = (range_high - range_low) < (range_high * 0.08)

        swing_h = int(last50["h"].max())
        swing_l = int(last50["l"].min())

        return {
            "pdh": pdh, "pdl": pdl, "pdc": pdc,
            "pwh": pwh, "pwl": pwl, "pwc": pwc,
            "range_high": range_high, "range_low": range_low,
            "range_mid": range_mid,   "range_pct": range_pct,
            "swing_h": swing_h,       "swing_l": swing_l,
            "is_ranging": is_ranging, "current": current
        }
    except Exception as e:
        log.error(f"Pre-calculate error: {e}")
        return {}


def build_morning_string(tf_data):
    """Build full compressed string for morning AI prompt"""
    w  = tf_data["weekly"]
    d  = tf_data["daily"]
    h  = tf_data["hourly"]
    m15= tf_data["m15"]
    m5 = tf_data["m5"].tail(M5_MORNING) if not tf_data["m5"].empty else pd.DataFrame()

    if d.empty:
        return None, None

    calc = pre_calculate(w, d)
    if not calc:
        return None, None

    d_old    = d.head(DAILY_OLD)
    d_recent = d.tail(DAILY_RECENT)

    w_base   = get_base(w)
    d_base   = get_base(d)
    h_base   = get_base(h)
    m15_base = get_base(m15)

    w_str   = compress_hl(w, w_base)          if not w.empty    else "N/A"
    do_str  = compress_hl(d_old, d_base)      if not d_old.empty else "N/A"
    dr_str  = compress_ohlc(d_recent, d_base) if not d_recent.empty else "N/A"
    h_str   = compress_ohlc(h, h_base)        if not h.empty    else "N/A"
    m15_str = compress_ohlc(m15, m15_base)    if not m15.empty  else "N/A"
    m5_str  = compress_ohlc(m5, d_base)       if not m5.empty   else "N/A"

    c = calc
    today = datetime.now(IST).strftime("%d-%m-%Y")

    data_string = f"""=== NIFTY50 MORNING ANALYSIS | {today} ===

[MACRO - Python Calculated]
Current:{c['current']} | PDH:{c['pdh']} PDL:{c['pdl']} PDC:{c['pdc']}
PWH:{c['pwh']} PWL:{c['pwl']} PWC:{c['pwc']}
50D_Range:{c['range_low']}-{c['range_high']} | Mid:{c['range_mid']}
Range_Position:{c['range_pct']}% | Ranging:{c['is_ranging']}
SwingH:{c['swing_h']} | SwingL:{c['swing_l']}

[WEEKLY - {WEEKLY_COUNT} candles | BASE:{w_base}]
(H L per candle | oldest to newest)
{w_str}

[DAILY OLD - {DAILY_OLD} candles | BASE:{d_base}]
(H L per candle)
{do_str}

[DAILY RECENT - {DAILY_RECENT} candles | BASE:{d_base}]
(O H L C per candle)
{dr_str}

[HOURLY - {HOURLY_COUNT} candles | BASE:{h_base}]
(O H L C per candle)
{h_str}

[15MIN - {M15_MORNING} candles | BASE:{m15_base}]
(O H L C per candle)
{m15_str}

[5MIN OPENING - {M5_MORNING} candles | BASE:{d_base}]
(O H L C per candle)
{m5_str}"""

    return data_string, calc


def drop_incomplete_candle(df, interval_minutes=5, buffer_seconds=10):
    """
    Remove last candle from df if it's still forming.
    Prevents AI from analyzing incomplete candle data.
    """
    if df.empty or len(df) < 2:
        return df
    try:
        last_ts      = _to_ist_datetime(df.iloc[-1]["ts"])
        complete_at  = last_ts + timedelta(
            minutes=interval_minutes, seconds=buffer_seconds
        )
        if datetime.now(IST) < complete_at:
            log.info(
                f"Dropped incomplete candle: {last_ts.strftime('%H:%M')} "
                f"(completes at {complete_at.strftime('%H:%M:%S')})"
            )
            return df.iloc[:-1].copy()
    except Exception as e:
        log.error(f"drop_incomplete_candle error: {e}")
    return df


def build_intraday_string(tf_data, morning_ctx, current_price, state):
    """Build compressed string for intraday AI prompt"""
    m15 = tf_data["m15"].tail(INTRADAY_M15)
    m5  = tf_data["m5"].tail(INTRADAY_M5)

    m15 = drop_incomplete_candle(m15, interval_minutes=15)
    m5  = drop_incomplete_candle(m5,  interval_minutes=5)

    if m15.empty or m5.empty:
        return None

    combined_min = min(
        m15["l"].min() if not m15.empty else 99999,
        m5["l"].min()  if not m5.empty  else 99999
    )
    base = math.floor(int(combined_min) / 500) * 500

    m15_str = compress_ohlc(m15, base)
    m5_str  = compress_ohlc(m5,  base)

    levels = morning_ctx.get("levels", [])
    r_levels = [l for l in levels if l["type"] == "R"][:3]
    s_levels = [l for l in levels if l["type"] == "S"][:3]
    r_str = " | ".join([f"R{i+1}:{l['price']}[{l['strength'][0]}]"
                        for i, l in enumerate(r_levels)])
    s_str = " | ".join([f"S{i+1}:{l['price']}[{l['strength'][0]}]"
                        for i, l in enumerate(s_levels)])

    history = get_signal_history()
    hist_str = "NONE"
    if history:
        hist_str = " | ".join([
            f"{s['time']}:{s['type']}@{s['level']}→{s['result']}"
            for s in history[-3:]
        ])

    now_str       = datetime.now(IST).strftime("%H:%M")
    state_name    = state[0]
    near_level    = state[1]
    lvl_strength  = state[2]
    trap_watch    = morning_ctx.get("opening", {}).get("trap_watch", "NONE")

    return f"""=== NIFTY INTRADAY | {now_str} ===

[MORNING CONTEXT]
Trend:{morning_ctx.get('trend',{}).get('daily','?')} | Bias:{morning_ctx.get('trend',{}).get('bias','?')}
Structure:{morning_ctx.get('structure',{}).get('type','?')}
{r_str}
{s_str}
LiqAbove:{morning_ctx.get('liquidity',{}).get('pool_above','?')}
LiqBelow:{morning_ctx.get('liquidity',{}).get('pool_below','?')}
HuntBias:{morning_ctx.get('liquidity',{}).get('hunt_bias','?')}
DayType:{morning_ctx.get('day_type','?')} | TrapWatch:{trap_watch}

[CURRENT STATE]
Price:{current_price} | State:{state_name}
NearLevel:{near_level}[{lvl_strength}] | Time:{now_str}

[TODAY SIGNALS]
{hist_str}

[15MIN - {INTRADAY_M15} candles | BASE:{base}]
(O H L C | oldest to newest)
{m15_str}

[5MIN - {INTRADAY_M5} candles | BASE:{base}]
(O H L C | oldest to newest)
{m5_str}"""


# ═══════════════════════════════════════════════════════
# SECTION 4: REDIS BRAIN
# ═══════════════════════════════════════════════════════

_memory = {}

def _redis_client():
    try:
        import redis
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None

_r = _redis_client()
if _r:
    log.info("✅ Redis connected")
else:
    log.warning("⚠️  Redis unavailable — using RAM mode")


# ── CHANGE 3: redis_status helper ────────────────────
def redis_status():
    return "connected" if _r else "RAM mode"


def _set(key, value, ttl=86400):
    val = json.dumps(value)
    if _r:
        _r.setex(key, ttl, val)
    else:
        _memory[key] = val


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


def save_morning_context(ctx):
    _set("morning_context", ctx)
    log.info("✅ Morning context saved to Redis")


def get_morning_context():
    return _get("morning_context")


def save_active_signal(signal):
    _set("active_signal", signal)


def get_active_signal():
    return _get("active_signal")


def delete_active_signal():
    _delete("active_signal")


def save_signal_history(history):
    _set("signal_history", history)


def get_signal_history():
    return _get("signal_history") or []


def flush_daily_data():
    """3:30 PM — clear all daily data"""
    for key in ["morning_context", "active_signal", "signal_history"]:
        _delete(key)
    log.info("🧹 Daily Redis data flushed")


# ═══════════════════════════════════════════════════════
# SECTION 5: PRICE STATE DETECTOR (FREE — No AI)
# ═══════════════════════════════════════════════════════

def detect_price_state(current_price, morning_ctx):
    """
    Returns: (state_name, level, strength)
    States:
        AT_SUPPORT / AT_RESISTANCE
        BREAKOUT_ZONE / BREAKDOWN_ZONE
        AT_LIQUIDITY_ABOVE / AT_LIQUIDITY_BELOW
        SIDEWAYS_MIDDLE
    """
    threshold = current_price * SR_THRESHOLD
    levels    = morning_ctx.get("levels", [])
    liq       = morning_ctx.get("liquidity", {})

    for lvl in levels:
        price    = lvl["price"]
        strength = lvl["strength"]
        ltype    = lvl["type"]

        if abs(current_price - price) <= threshold:
            state = "AT_RESISTANCE" if ltype == "R" else "AT_SUPPORT"
            return (state, price, strength)

        if ltype == "R" and price < current_price < price * 1.005:
            return ("BREAKOUT_ZONE", price, strength)

        if ltype == "S" and price * 0.995 < current_price < price:
            return ("BREAKDOWN_ZONE", price, strength)

    pool_above = liq.get("pool_above", 0)
    pool_below = liq.get("pool_below", 0)

    if pool_above and abs(current_price - pool_above) <= threshold:
        return ("AT_LIQUIDITY_ABOVE", pool_above, "STRONG")

    if pool_below and abs(current_price - pool_below) <= threshold:
        return ("AT_LIQUIDITY_BELOW", pool_below, "STRONG")

    return ("SIDEWAYS_MIDDLE", 0, "NONE")


def should_call_ai(state, morning_ctx):
    """Decide whether to call AI — saves tokens"""
    now    = datetime.now(IST)
    hour   = now.hour
    minute = now.minute

    if hour == 9 and minute < 25:
        return False, "Opening wait (before 9:25)"

    if hour == 15 and minute > 15:
        return False, "Market closing"

    active = get_active_signal()
    if active and active.get("status") == "OPEN":
        return False, "Active signal exists"

    history = get_signal_history()
    if len(history) >= MAX_DAY_SIGNALS:
        return False, "Max signals reached"

    call_states = [
        "AT_SUPPORT", "AT_RESISTANCE",
        "BREAKOUT_ZONE", "BREAKDOWN_ZONE",
        "AT_LIQUIDITY_ABOVE", "AT_LIQUIDITY_BELOW"
    ]
    if state[0] not in call_states:
        return False, f"State: {state[0]} — skip"

    already_analyzed = [
        f"{s.get('state', '')}_{s.get('level', 0)}"
        for s in history
    ]
    current_key = f"{state[0]}_{state[1]}"
    if current_key in already_analyzed:
        return False, f"Already analyzed: {current_key}"

    return True, f"Setup at {state[0]}: {state[1]}"


# ═══════════════════════════════════════════════════════
# SECTION 6: SIGNAL TRACKER (FREE — No AI)
# ═══════════════════════════════════════════════════════

def track_active_signal(current_price):
    """
    Check SL / Target / Expiry — no AI needed
    Returns: update message or None
    """
    sig = get_active_signal()
    if not sig or sig.get("status") != "OPEN":
        return None

    stype   = sig["type"]
    sl      = sig["sl"]
    t1      = sig["target1"]
    t2      = sig["target2"]
    t1_hit  = sig.get("t1_hit", False)
    entry   = sig["entry"]

    if stype == "STRONG_BUY":
        if current_price <= sl:
            return _close_signal(sig, "SL_HIT")

        if not t1_hit and current_price >= t1:
            sig["t1_hit"] = True
            sig["sl"]     = entry
            save_active_signal(sig)
            return {"event": "T1_HIT", "signal": sig}

        if t1_hit and current_price >= t2:
            return _close_signal(sig, "TARGET_HIT")

    elif stype == "STRONG_SELL":
        if current_price >= sl:
            return _close_signal(sig, "SL_HIT")

        if not t1_hit and current_price <= t1:
            sig["t1_hit"] = True
            sig["sl"]     = entry
            save_active_signal(sig)
            return {"event": "T1_HIT", "signal": sig}

        if t1_hit and current_price <= t2:
            return _close_signal(sig, "TARGET_HIT")

    try:
        created_at = sig.get("created_at")

        if created_at:
            sig_time = datetime.fromisoformat(created_at)
        else:
            sig_time = datetime.strptime(
                sig["time"], "%H:%M"
            ).replace(
                year=datetime.now(IST).year,
                month=datetime.now(IST).month,
                day=datetime.now(IST).day
            )
            sig_time = IST.localize(sig_time)

        elapsed = (datetime.now(IST) - sig_time).total_seconds() / 60
        if elapsed > SIGNAL_EXPIRY:
            return _close_signal(sig, "EXPIRED")

    except Exception as e:
        log.error(f"Expiry check error: {e}")

    return None


def _close_signal(sig, result):
    """Close active signal and update history"""
    sig["status"] = result
    delete_active_signal()

    history = get_signal_history()
    for h in history:
        if h.get("time") == sig.get("time"):
            h["result"] = result
    save_signal_history(history)

    return {"event": result, "signal": sig}


def save_new_signal(ai_resp, level, current_price, state_name=""):
    """Save new signal from AI response"""
    sig = {
        "type":       ai_resp["signal"],
        "entry":      ai_resp.get("entry", current_price),
        "sl":         ai_resp["sl"],
        "target1":    ai_resp["target1"],
        "target2":    ai_resp["target2"],
        "level":      level,
        "state":      state_name,
        "time":       datetime.now(IST).strftime("%H:%M"),
        "created_at": datetime.now(IST).isoformat(),
        "status":     "OPEN",
        "t1_hit":     False
    }
    save_active_signal(sig)

    history = get_signal_history()
    history.append({
        "time":   sig["time"],
        "type":   sig["type"],
        "level":  level,
        "state":  state_name,
        "result": "OPEN"
    })
    save_signal_history(history)
    log.info(f"✅ Signal saved: {sig['type']} @ {sig['entry']}")


# ═══════════════════════════════════════════════════════
# SECTION 7: AI ANALYSIS — Claude Haiku
# ═══════════════════════════════════════════════════════

MORNING_SYSTEM_PROMPT = """You are an expert Nifty50 market structure analyst.

You receive multi-timeframe compressed OHLC data.
FORMAT: Each line = one candle.
- H L format = weekly/old daily structure candles
- O H L C format = recent/intraday candles
All values are delta-encoded: add BASE to get real price.
Candles ordered oldest to newest.

YOUR JOB:
1. Identify major S/R levels with strength rating
2. Detect liquidity pools (equal highs/lows)
3. Determine market structure (HH/HL or LH/LL)
4. Set intraday directional bias
5. Find high probability reaction zones for today
6. Predict day type
7. Identify possible trap setups

RULES:
- Use pure price action only, no indicators
- Rate every S/R: STRONG / MEDIUM / WEAK
- Respond ONLY in valid JSON, no text outside JSON"""

INTRADAY_SYSTEM_PROMPT = """You are an expert Nifty50 intraday price action trader.

DATA FORMAT: Each line = one candle (O H L C), oldest to newest.
Values are delta-encoded (add BASE to get real price).

ANALYSIS ORDER (strict):
1. Check price vs morning S/R levels
2. Detect liquidity hunt or trap (wick/sweep check)
3. Identify candlestick pattern (last 3 candles)
4. Check chart pattern (last 10-15 candles)
5. Check 15M + 5M alignment
6. Give final signal

CONCEPTS TO USE:
Liquidity Hunt, Liquidity Swap, Bull Trap, Bear Trap,
BOS, CHoCH, HH/HL, LH/LL, Engulfing, Hammer,
Shooting Star, Pin Bar, Marubozu, Inside Bar,
Double Top/Bottom, Flag, Triangle, Breakout, Breakdown

CONFIRMATION RULES (strictly follow):
- For STRONG_BUY or STRONG_SELL:
  * confirmations array MUST contain minimum 3 REAL factors
  * Each confirmation must describe an actual pattern/level/concept found
  * confirmation_count MUST equal the length of confirmations array
  * Do NOT use placeholder text like "reason1", "factor1", etc.
  * Example valid confirmations:
    "Bullish engulfing at S1:24480"
    "15M and 5M both showing HH_HL structure"
    "Liquidity sweep below S1, price closed above"
- If confirmation_count < 3, signal MUST be WAIT
- If conflicting TFs → WAIT, never force a signal
- AVOID when price is in middle of range

RESPOND ONLY in valid JSON. No text outside JSON."""


def call_haiku(system_prompt, user_prompt):
    """Call Claude Haiku API"""
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json"
    }
    body = {
        "model":      "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "system":     system_prompt,
        "messages":   [{"role": "user", "content": user_prompt}]
    }

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=30)
        if resp.status_code == 200:
            content = resp.json()["content"][0]["text"]
            return content
        else:
            log.error(f"Haiku API error {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        log.error(f"Haiku call failed: {e}")
        return None


def parse_ai_response(raw_text):
    """Extract and validate JSON from AI response"""
    if not raw_text:
        return None
    try:
        clean = raw_text.strip()
        clean = clean.replace("```json", "").replace("```", "").strip()

        start = clean.find("{")
        end   = clean.rfind("}") + 1
        if start == -1 or end == 0:
            return None

        return json.loads(clean[start:end])
    except Exception as e:
        log.error(f"JSON parse error: {e}")
        log.debug(f"Raw: {raw_text[:300]}")
        return None


def validate_signal(ai, current_price):
    """
    BUY:  SL < Entry < T1 < T2
    SELL: SL > Entry > T1 > T2
    Entry must be within MAX_ENTRY_DIST pts of current price
    """
    try:
        sig   = ai.get("signal", "")
        entry = float(ai.get("entry") or current_price)
        sl    = float(ai.get("sl")     or 0)
        t1    = float(ai.get("target1") or 0)
        t2    = float(ai.get("target2") or 0)

        entry_diff_pts = abs(entry - current_price)
        if entry_diff_pts > MAX_ENTRY_DIST:
            log.warning(f"Signal rejected: entry {entry} too far from LTP {current_price} ({entry_diff_pts:.0f} pts > {MAX_ENTRY_DIST})")
            return False

        if sig == "STRONG_BUY":
            valid = sl < entry < t1 < t2
        elif sig == "STRONG_SELL":
            valid = sl > entry > t1 > t2
        else:
            return False

        if not valid:
            log.warning(f"Signal rejected: invalid SL/Target structure | {sig} E:{entry} SL:{sl} T1:{t1} T2:{t2}")
        return valid

    except Exception as e:
        log.error(f"Signal validation error: {e}")
        return False


def rr_ok(ai):
    """Minimum 1:1.5 Risk-Reward check"""
    try:
        entry  = float(ai.get("entry", 0))
        sl     = float(ai.get("sl", 0))
        t1     = float(ai.get("target1", 0))
        risk   = abs(entry - sl)
        reward = abs(t1 - entry)

        if risk <= 0:
            return False

        rr = reward / risk
        if rr < MIN_RR:
            log.warning(f"Signal rejected: RR {rr:.2f} < {MIN_RR} minimum")
            return False

        log.info(f"✅ RR check passed: 1:{rr:.2f} (min required: 1:{MIN_RR})")
        return True

    except Exception as e:
        log.error(f"RR check error: {e}")
        return False


def confirmation_ok(ai):
    """Minimum 3 REAL confirmations required — fake words rejected"""
    confirmations = ai.get("confirmations", [])
    count = ai.get("confirmation_count", len(confirmations))

    try:
        count = int(count)
    except Exception:
        count = len(confirmations)

    fake_words = {
        "reason1", "reason2", "reason3", "reason4", "reason5",
        "none", "na", "n/a", "null", "factor1", "factor2",
        "confirmation1", "confirmation2", "confirmation3", ""
    }

    clean = [
        str(c).strip().lower()
        for c in confirmations
        if str(c).strip().lower() not in fake_words
        and len(str(c).strip()) > 5
    ]

    if count < 3 or len(clean) < 3:
        log.warning(
            f"Signal rejected: weak confirmations | "
            f"count={count}, real={len(clean)} | {confirmations}"
        )
        return False

    log.info(f"✅ Confirmation check passed: {len(clean)} real factors")
    return True


def _to_ist_datetime(ts):
    """Convert any timestamp to IST-aware datetime"""
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        return IST.localize(ts.to_pydatetime())
    return ts.tz_convert(IST).to_pydatetime()


def get_last_completed_m5_candle(tf_data, buffer_seconds=10):
    """
    Return last COMPLETED 5min candle.
    If latest candle still forming → use previous one.
    """
    m5 = tf_data.get("m5", pd.DataFrame())
    if m5.empty:
        return None

    now          = datetime.now(IST)
    last         = m5.iloc[-1]
    last_ts      = _to_ist_datetime(last["ts"])
    complete_at  = last_ts + timedelta(minutes=5, seconds=buffer_seconds)

    if now >= complete_at:
        log.info(f"✅ Latest 5m candle complete: {last_ts.strftime('%H:%M')}")
        return last

    if len(m5) >= 2:
        prev    = m5.iloc[-2]
        prev_ts = _to_ist_datetime(prev["ts"])
        log.info(
            f"5m candle {last_ts.strftime('%H:%M')} still forming "
            f"→ using {prev_ts.strftime('%H:%M')} instead"
        )
        return prev

    log.warning("No completed 5m candle available yet")
    return None


def candle_close_ok(tf_data, signal_type):
    """
    Last COMPLETED 5m candle must confirm signal direction.
    BUY  → close > open (bullish candle)
    SELL → close < open (bearish candle)
    """
    try:
        candle = get_last_completed_m5_candle(tf_data)
        if candle is None:
            log.warning("Signal rejected: no completed 5m candle")
            return False

        close = int(candle["c"])
        opn   = int(candle["o"])

        if signal_type == "STRONG_BUY":
            result = close > opn
            log.info(f"Candle close BUY: C:{close} > O:{opn} = {result}")
            return result

        if signal_type == "STRONG_SELL":
            result = close < opn
            log.info(f"Candle close SELL: C:{close} < O:{opn} = {result}")
            return result

        return False

    except Exception as e:
        log.error(f"Candle close check error: {e}")
        return False


def run_morning_analysis(tf_data):
    """Build prompt → Call Haiku → Save to Redis"""
    log.info("🌅 Running morning analysis...")

    data_string, calc = build_morning_string(tf_data)
    if not data_string:
        log.error("Morning data build failed")
        return None

    user_prompt = data_string + """

ANALYZE and return ONLY this JSON:
{
  "trend": {
    "daily": "BULLISH/BEARISH/SIDEWAYS",
    "hourly": "BULLISH/BEARISH/SIDEWAYS",
    "bias": "LONG/SHORT/NEUTRAL",
    "bias_reason": "1 line max"
  },
  "structure": {
    "type": "HH_HL/LH_LL/SIDEWAYS",
    "last_bos": 0,
    "key_swing_h": 0,
    "key_swing_l": 0
  },
  "levels": [
    {"price": 0, "type": "R", "strength": "STRONG", "note": "brief"},
    {"price": 0, "type": "R", "strength": "MEDIUM", "note": "brief"},
    {"price": 0, "type": "R", "strength": "WEAK",   "note": "brief"},
    {"price": 0, "type": "S", "strength": "STRONG", "note": "brief"},
    {"price": 0, "type": "S", "strength": "MEDIUM", "note": "brief"},
    {"price": 0, "type": "S", "strength": "WEAK",   "note": "brief"}
  ],
  "liquidity": {
    "pool_above": 0,
    "pool_below": 0,
    "equal_highs": 0,
    "equal_lows": 0,
    "hunt_bias": "ABOVE_FIRST/BELOW_FIRST/NEUTRAL"
  },
  "day_type": "TRENDING/RANGING/VOLATILE/INSIDE_DAY",
  "high_prob_zones": [0, 0],
  "avoid_zone": "price range to avoid",
  "opening": {
    "gap_type": "GAP_UP/GAP_DOWN/FLAT",
    "gap_pts": 0,
    "trap_watch": "BULL_TRAP/BEAR_TRAP/NONE",
    "first_move_bias": "UP/DOWN/WAIT"
  },
  "traps_today": {
    "possible": "YES/NO",
    "type": "BULL/BEAR/BOTH/NONE",
    "zone": 0
  }
}"""

    raw = call_haiku(MORNING_SYSTEM_PROMPT, user_prompt)
    result = parse_ai_response(raw)

    if result:
        result["_calc"] = calc
        save_morning_context(result)
        log.info(f"✅ Morning analysis done: {result.get('trend',{}).get('bias','?')} bias | {result.get('day_type','?')}")
        return result
    else:
        log.error("Morning analysis failed — no valid response")
        return None


def run_intraday_analysis(tf_data, morning_ctx, current_price, state):
    """Build prompt → Call Haiku → Return signal"""
    log.info(f"📊 Running intraday analysis | State: {state[0]} @ {state[1]}")

    user_prompt = build_intraday_string(
        tf_data, morning_ctx, current_price, state
    )
    if not user_prompt:
        log.error("Intraday data build failed")
        return None

    full_prompt = user_prompt + """

ANALYZE and return ONLY this JSON:
{
  "structure": {
    "current": "HH_HL/LH_LL/SIDEWAYS",
    "bos_detected": "YES/NO",
    "choch_detected": "YES/NO"
  },
  "pattern_15m": {
    "candle": "pattern name or NONE",
    "chart":  "pattern name or NONE"
  },
  "pattern_5m": {
    "candle": "pattern name or NONE",
    "chart":  "pattern name or NONE"
  },
  "liquidity": {
    "hunt_detected": "YES/NO",
    "hunt_side": "ABOVE/BELOW/NONE",
    "sweep_confirmed": "YES/NO"
  },
  "trap": {
    "detected": "YES/NO",
    "type": "BULL/BEAR/NONE",
    "confidence": "HIGH/MED/LOW"
  },
  "tf_alignment": "BOTH/ONLY_15M/ONLY_5M/CONFLICT",
  "confirmations": [],
  "confirmation_count": 0,
  "signal": "STRONG_BUY/STRONG_SELL/WAIT/AVOID",
  "confidence": "HIGH/MED/LOW",
  "entry": 0,
  "sl": 0,
  "target1": 0,
  "target2": 0,
  "reason": "max 2 lines"
}"""

    raw    = call_haiku(INTRADAY_SYSTEM_PROMPT, full_prompt)
    result = parse_ai_response(raw)

    if result:
        log.info(f"✅ Intraday analysis: {result.get('signal')} | {result.get('confidence')}")
    return result


# ═══════════════════════════════════════════════════════
# SECTION 8: TELEGRAM
# ═══════════════════════════════════════════════════════

# ── CHANGE 1: tg_send returns True/False + status check ──
def tg_send(text):
    """Send message to Telegram and return True/False"""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        log.warning("Telegram not configured")
        return False

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id":    TG_CHAT_ID,
            "text":       text,
            "parse_mode": "HTML"
        }, timeout=10)

        if resp.status_code != 200:
            log.error(f"Telegram send failed {resp.status_code}: {resp.text[:200]}")
            return False

        return True

    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def send_morning_brief(ctx, calc):
    """Send morning analysis summary to Telegram"""
    trend  = ctx.get("trend", {})
    liq    = ctx.get("liquidity", {})
    levels = ctx.get("levels", [])
    opening= ctx.get("opening", {})

    r_levels = [l for l in levels if l["type"] == "R"]
    s_levels = [l for l in levels if l["type"] == "S"]

    r_str = "\n".join([f"  R{i+1}: {l['price']} [{l['strength']}]"
                       for i,l in enumerate(r_levels[:3])])
    s_str = "\n".join([f"  S{i+1}: {l['price']} [{l['strength']}]"
                       for i,l in enumerate(s_levels[:3])])

    today = datetime.now(IST).strftime("%d %b %Y")

    msg = f"""📊 <b>MORNING BRIEF | {today}</b>

Trend  : {trend.get('daily','?')}
Bias   : {trend.get('bias','?')}
Reason : {trend.get('bias_reason','?')}
Type   : {ctx.get('day_type','?')}

<b>RESISTANCE:</b>
{r_str}

<b>SUPPORT:</b>
{s_str}

Liq Above  : {liq.get('pool_above','?')}
Liq Below  : {liq.get('pool_below','?')}
Hunt Bias  : {liq.get('hunt_bias','?')}
Avoid Zone : {ctx.get('avoid_zone','?')}

Gap    : {opening.get('gap_type','?')} ({opening.get('gap_pts',0)} pts)
Trap?  : {opening.get('trap_watch','?')}"""

    tg_send(msg)
    log.info("✅ Morning brief sent to Telegram")


def send_signal(ai_resp, current_price):
    """Send trade signal to Telegram"""
    stype  = ai_resp.get("signal", "")
    emoji  = "🟢" if stype == "STRONG_BUY" else "🔴"
    action = "BUY" if stype == "STRONG_BUY" else "SELL"
    now    = datetime.now(IST).strftime("%H:%M")

    trap   = ai_resp.get("trap", {})
    liq    = ai_resp.get("liquidity", {})
    confs  = ai_resp.get("confirmations", [])

    setup_parts = []
    if trap.get("detected") == "YES":
        setup_parts.append(f"{trap.get('type','?')} Trap")
    if liq.get("hunt_detected") == "YES":
        setup_parts.append("Liq Hunt")
    p15 = ai_resp.get("pattern_15m", {}).get("candle", "NONE")
    p5  = ai_resp.get("pattern_5m", {}).get("candle", "NONE")
    if p15 != "NONE":
        setup_parts.append(p15)
    if p5 != "NONE" and p5 != p15:
        setup_parts.append(p5)
    setup_str = " + ".join(setup_parts) if setup_parts else "Price Action"

    conf_str = ""
    if confs:
        conf_str = "\n" + "\n".join([f"  ✔ {c}" for c in confs[:5]])

    msg = f"""{emoji} <b>{action} SIGNAL | {now}</b>

Entry  : {ai_resp.get('entry', current_price):,}
SL     : {ai_resp.get('sl', 0):,}
Target : {ai_resp.get('target1', 0):,} / {ai_resp.get('target2', 0):,}

Setup  : {setup_str}
TF     : {ai_resp.get('tf_alignment','?')}
Conf   : {ai_resp.get('confidence','?')} ({ai_resp.get('confirmation_count',0)} factors){conf_str}

{ai_resp.get('reason','')}"""

    tg_send(msg)
    log.info(f"✅ Signal sent: {stype}")


def send_signal_update(event, signal):
    """Send SL hit / target hit / expired update"""
    stype = signal.get("type", "")

    if event == "T1_HIT":
        msg = f"""🎯 <b>TARGET 1 HIT | {signal.get('time')}</b>

{stype} @ {signal.get('entry'):,}
T1: {signal.get('target1'):,} ✅
SL moved to entry — free trade now!
Riding for T2: {signal.get('target2'):,}"""

    elif event == "TARGET_HIT":
        msg = f"""🎯🎯 <b>TARGET 2 HIT | {signal.get('time')}</b>

{stype} @ {signal.get('entry'):,}
Full profit! Trade closed ✅"""

    elif event == "SL_HIT":
        msg = f"""🔴 <b>SL HIT | {signal.get('time')}</b>

{stype} @ {signal.get('entry'):,}
SL: {signal.get('sl'):,}
Trade closed."""

    elif event == "EXPIRED":
        msg = f"""⏰ <b>SIGNAL EXPIRED | {signal.get('time')}</b>

{stype} @ {signal.get('entry'):,}
No movement in {SIGNAL_EXPIRY}min. Closed."""
    else:
        return

    tg_send(msg)


def send_day_summary():
    """Send EOD summary to Telegram"""
    history = get_signal_history()
    total   = len(history)
    wins    = sum(1 for s in history if s.get("result") == "TARGET_HIT")
    losses  = sum(1 for s in history if s.get("result") == "SL_HIT")
    expired = sum(1 for s in history if s.get("result") == "EXPIRED")

    today = datetime.now(IST).strftime("%d %b %Y")

    detail = ""
    for s in history:
        r = s.get("result","?")
        emoji = "✅" if r == "TARGET_HIT" else "🔴" if r == "SL_HIT" else "⏰"
        detail += f"\n{emoji} {s['time']} {s['type']}@{s['level']} → {r}"

    msg = f"""📈 <b>DAY SUMMARY | {today}</b>

Signals : {total}
✅ Wins  : {wins}
🔴 Loss  : {losses}
⏰ Exp   : {expired}
{detail}"""

    tg_send(msg)
    log.info("✅ Day summary sent")


# ═══════════════════════════════════════════════════════
# SECTION 9: ORCHESTRATOR
# ═══════════════════════════════════════════════════════

# ── Duplicate run prevention ─────────────────────────
FIRST_ANALYSIS_LOCK = threading.Lock()


def is_market_open():
    """Weekend + market hours check"""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        log.info("Weekend — market closed")
        return False
    from datetime import time as dtime
    return dtime(9, 15) <= now.time() <= dtime(15, 30)


# ── CHANGE 5: morning_job renamed to first_analysis_job ──
def first_analysis_job():
    """Runs at 9:20:10 IST — Full first analysis"""

    if not FIRST_ANALYSIS_LOCK.acquire(blocking=False):
        log.info("⏭️ First analysis already running — skip duplicate")
        return

    try:
        log.info("=" * 55)
        log.info("🌅 FIRST ANALYSIS JOB STARTED")
        log.info("=" * 55)

        tf_data = fetch_all_tf_morning()
        ctx = run_morning_analysis(tf_data)

        if ctx:
            calc = pre_calculate(
                tf_data["weekly"], tf_data["daily"]
            )
            send_morning_brief(ctx, calc)
        else:
            tg_send("⚠️ First analysis failed — check logs")

    except Exception as e:
        log.error(f"First analysis job error: {e}")
        tg_send(f"⚠️ First analysis job error: {str(e)[:100]}")

    finally:
        FIRST_ANALYSIS_LOCK.release()


def intraday_job():
    """Runs every 5:10 — Price state check + AI if needed"""
    if not is_market_open():
        return

    try:
        # ── CHANGE 7: auto-run first_analysis_job if context missing ──
        morning_ctx = get_morning_context()

        if not morning_ctx:
            log.warning("No first analysis context — running first_analysis_job now")
            first_analysis_job()
            morning_ctx = get_morning_context()

            if not morning_ctx:
                log.warning("First analysis still missing — skipping intraday")
                tg_send("⚠️ First analysis failed, intraday analysis skipped")
                return

        current_price = get_ltp()
        if not current_price:
            log.warning("LTP fetch failed — skipping")
            return

        log.info(f"📍 LTP: {current_price}")

        update = track_active_signal(current_price)
        if update:
            event  = update["event"]
            signal = update["signal"]
            log.info(f"📡 Signal update: {event}")
            send_signal_update(event, signal)
            return

        state = detect_price_state(current_price, morning_ctx)
        log.info(f"📊 State: {state[0]} | Level: {state[1]}")

        should_call, reason = should_call_ai(state, morning_ctx)
        if not should_call:
            log.info(f"⏭️  Skip AI: {reason}")
            return

        log.info(f"🤖 Calling AI — {reason}")

        tf_data = fetch_all_tf_intraday()

        ai_resp = run_intraday_analysis(
            tf_data, morning_ctx, current_price, state
        )

        if not ai_resp:
            return

        signal_type = ai_resp.get("signal", "AVOID")
        confidence  = ai_resp.get("confidence", "LOW")

        if signal_type in ["STRONG_BUY", "STRONG_SELL"]:

            structure     = ai_resp.get("structure", {})
            structure_val = str(structure.get("current", "")).upper()
            if structure_val == "SIDEWAYS":
                log.info("⏭️  Signal rejected: SIDEWAYS structure")
                tg_send("⚠️ Signal rejected: SIDEWAYS structure — wait for clear direction")
                return

            if not validate_signal(ai_resp, current_price):
                tg_send(f"⚠️ AI signal rejected: invalid SL/Target\n{ai_resp.get('reason','')}")
                return

            if not confirmation_ok(ai_resp):
                tg_send(
                    f"⚠️ AI signal rejected: less than 3 confirmations\n"
                    f"{ai_resp.get('reason','')}"
                )
                return

            if not rr_ok(ai_resp):
                tg_send(
                    f"⚠️ AI signal rejected: poor RR ratio\n"
                    f"Entry:{ai_resp.get('entry')} SL:{ai_resp.get('sl')} T1:{ai_resp.get('target1')}"
                )
                return

            if not candle_close_ok(tf_data, signal_type):
                direction = "bullish" if signal_type == "STRONG_BUY" else "bearish"
                log.info(f"⏭️  Signal rejected: candle close not {direction}")
                tg_send(
                    f"⚠️ Signal rejected: candle close not confirmed\n"
                    f"{signal_type} needs {direction} close"
                )
                return

            if confidence == "HIGH":
                save_new_signal(ai_resp, state[1], current_price, state[0])
                send_signal(ai_resp, current_price)
            elif confidence == "MED":
                now_str = datetime.now(IST).strftime("%H:%M")
                tg_send(
                    f"⚠️ <b>SETUP FORMING | {now_str}</b>\n"
                    f"{signal_type} | Watch {state[1]}\n"
                    f"{ai_resp.get('reason','')}"
                )
        else:
            log.info(f"No signal: {signal_type}")

    except Exception as e:
        log.error(f"Intraday job error: {e}")


def closing_job():
    """Runs at 3:30 PM — Summary + flush"""
    log.info("🔔 CLOSING JOB")
    send_day_summary()
    flush_daily_data()


# ═══════════════════════════════════════════════════════
# SECTION 10: STARTUP STATUS
# ═══════════════════════════════════════════════════════

BOT_STARTED_AT = datetime.now(IST)


# ── CHANGE 4: startup self-check function ────────────
def send_startup_status():
    """Send startup status to Telegram"""
    upstox_ok = False
    ltp_text  = "not tested"

    try:
        ltp = get_ltp()
        if ltp:
            upstox_ok = True
            ltp_text  = str(ltp)
        else:
            ltp_text = "failed"
    except Exception as e:
        ltp_text = f"error: {str(e)[:60]}"

    ai_key_ok = bool(ANTHROPIC_KEY)
    tg_ok     = bool(TG_BOT_TOKEN and TG_CHAT_ID)

    msg = f"""🚀 <b>Nifty50 AI Bot Started</b>

<b>SELF CHECK:</b>
Telegram : {'✅ OK' if tg_ok else '❌ Missing'}
Upstox   : {'✅ OK' if upstox_ok else '❌ Failed'} | LTP: {ltp_text}
Claude   : {'✅ API key found' if ai_key_ok else '❌ Missing'}
Redis    : {redis_status()}

Scheduler: ✅ Starting
Mode     : Data + AI Analysis only
AutoTrade: ❌ OFF"""

    tg_send(msg)


# ═══════════════════════════════════════════════════════
# SECTION 11: SCHEDULER + MAIN
# ═══════════════════════════════════════════════════════

def main():
    log.info("╔══════════════════════════════════════════╗")
    log.info("║   NIFTY50 AI BOT — Starting Up...        ║")
    log.info("╚══════════════════════════════════════════╝")

    # ── Validate env vars ────────────────────────────
    if not UPSTOX_TOKEN:
        log.error("❌ UPSTOX_ANALYTICS_TOKEN missing!")
        return
    if not ANTHROPIC_KEY:
        log.error("❌ ANTHROPIC_API_KEY missing!")
        return
    if not TG_BOT_TOKEN:
        log.error("❌ TELEGRAM_BOT_TOKEN missing!")
        return
    # ── CHANGE 2: TG_CHAT_ID validation ──────────────
    if not TG_CHAT_ID:
        log.error("❌ TELEGRAM_CHAT_ID missing!")
        return

    # ── CHANGE 9: startup status replace ─────────────
    send_startup_status()

    # ── BackgroundScheduler ───────────────────────────
    scheduler = BackgroundScheduler(timezone=IST)

    # ── CHANGE 6: first_analysis_job in scheduler ────
    scheduler.add_job(
        first_analysis_job,
        "cron",
        hour=9, minute=20, second=10,
        id="first_analysis_job"
    )

    # Intraday loop — every 5min at :10 seconds
    scheduler.add_job(
        intraday_job,
        "cron",
        minute="25,30,35,40,45,50,55",
        second=10,
        hour="9",
        id="intraday_job_9"
    )
    scheduler.add_job(
        intraday_job,
        "cron",
        minute="0,5,10,15,20,25,30,35,40,45,50,55",
        second=10,
        hour="10,11,12,13,14",
        id="intraday_job_main"
    )
    scheduler.add_job(
        intraday_job,
        "cron",
        minute="0,5,10",
        second=10,
        hour="15",
        id="intraday_job_15"
    )

    # Closing job — 3:30 PM
    scheduler.add_job(
        closing_job,
        "cron",
        hour=15, minute=30, second=30,
        id="closing_job"
    )

    scheduler.start()
    log.info("✅ Scheduler started (background)")
    log.info("   First Analysis : 9:20:10 IST")
    log.info("   Intraday       : Every 5min (9:25 → 15:10)")
    log.info("   Closing        : 15:30:30 IST")

    # ── CHANGE 8: startup first analysis auto-schedule ──
    now        = datetime.now(IST)
    start_time = datetime.strptime("09:20", "%H:%M").time()
    end_time   = datetime.strptime("15:10", "%H:%M").time()

    if (
        now.weekday() < 5
        and start_time <= now.time() <= end_time
        and not get_morning_context()
    ):
        scheduler.add_job(
            first_analysis_job,
            "date",
            run_date=datetime.now(IST) + timedelta(seconds=10),
            id="startup_first_analysis",
            replace_existing=True
        )
        log.info("⚡ Startup first analysis scheduled (market hours, context missing)")

    # ── Flask Web Server (Koyeb health check) ─────────
    flask_app = Flask(__name__)

    @flask_app.route("/")
    def index():
        return "Nifty50 AI Bot Running ✅", 200

    @flask_app.route("/health")
    def health():
        now     = datetime.now(IST).strftime("%d-%m-%Y %H:%M:%S")
        ctx     = get_morning_context()
        active  = get_active_signal()
        history = get_signal_history()
        return {
            "status":          "ok",
            "time_ist":        now,
            "morning_context": "loaded" if ctx else "missing",
            "active_signal":   active.get("type") if active else "none",
            "signals_today":   len(history),
            "scheduler":       "running" if scheduler.running else "stopped"
        }, 200

    port = int(os.getenv("PORT", 8000))
    log.info(f"🌐 Flask server on port {port}")

    # ── CHANGE 10: use_reloader=False ─────────────────
    flask_app.run(host="0.0.0.0", port=port, use_reloader=False)


if __name__ == "__main__":
    main()
