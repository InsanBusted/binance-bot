#!/usr/bin/env python3
# bot-btcusdc-v4-hybrid.py
# BTCUSDC USD-M Futures (REAL) - HYBRID Trend scalping + Range sniper
# - Regime detection ADX on 15m with hysteresis (TREND vs RANGE)
# - TREND mode EMA200 15m bias + EMA9/21 pullback on 5m + RSI confirm
# - RANGE mode Donchian(20) 5m touch + rejection back inside range, TP small
# - Safety: daily stop, loss streak stop, cooldown, max trades/day
# - Logs: logs/loop.csv, logs/trades.csv, logs/daily.csv

import os
import time
import json
import csv
import random
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Tuple, Dict, Any, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from requests.exceptions import ReadTimeout, ConnectionError


# =========================
# RETRY WRAPPER
# =========================
def call_with_retry(fn, *args, retries: int = 5, base_sleep: float = 1.0, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i) + random.uniform(0, 0.5))
    raise last_err


# =========================
# CONFIG
# =========================
SYMBOL = "BTCUSDC"
LEVERAGE = 15

USE_TESTNET = True

ENABLE_RANGE_MODE = False
USE_CLOSED_CANDLE_ONLY = True

TF_REGIME = "15m"   # for ADX regime + EMA200 bias
TF_ENTRY = "5m"     # for entries

# Regime detection (15m ADX) with hysteresis
ADX_LEN = 14
ADX_TREND_ON = 20.0   # switch to TREND if >= this
ADX_RANGE_ON = 19.0   # switch to RANGE if <= this
# If between 19..23 - keep previous mode (prevents flip-flop)

# Trend bias (15m)
EMA_TREND_LEN = 200
TREND_DEADBAND_PCT = 0.0012  # if price within +-0.12% of EMA200 - avoid trading

# TREND entry (5m)
EMA_FAST = 9
EMA_SLOW = 21
RSI_LEN = 14
RSI_TREND_LONG_MIN = 55
RSI_TREND_SHORT_MAX = 45
TREND_RR = 1.5
TREND_SL_ATR_MULT = 1.0
TREND_SL_MIN_PCT = 0.0025   # 0.25%
TREND_SL_MAX_PCT = 0.0100   # 1.00%

# RANGE entry (5m)
DONCHIAN_LEN = 20
RSI_RANGE_LONG_MIN = 45
RSI_RANGE_SHORT_MAX = 55
RANGE_RR = 0.9
RANGE_SL_ATR_MULT = 0.55
RANGE_SL_MIN_PCT = 0.0020   # 0.20%
RANGE_SL_MAX_PCT = 0.0080   # 0.80%

# Risk
RISK_PCT = 0.003  # 0.3% equity risk per trade
MAX_NOTIONAL_FRACTION_OF_EQUITY = 0.3

# Safety & pacing
MAX_TRADES_PER_DAY = 12
COOLDOWN_MINUTES = 10
LOSS_STREAK_LIMIT = 5
MAX_DAILY_DRAWDOWN_PCT = 0.04  # -4% hard stop (include unrealized)

SLEEP_SECONDS = 10
RECV_WINDOW = 10_000
ORDER_TYPE_ENTRY = "MARKET"

TG_PREFIX = "BTCUSDC V4 HYBRID"

STATE_FILE = Path(".state_btcusdc_v4_hybrid.json")


# =========================
# LOGGING
# =========================
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOOP_LOG = LOG_DIR / "loop.csv"
TRADES_LOG = LOG_DIR / "trades.csv"
DAILY_LOG = LOG_DIR / "daily.csv"

def _ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def _append_csv(path: Path, header: list, row: dict):
    _ensure_log_dir()
    new_file = not path.exists()
    with path.open('a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=header)
        if new_file:
            w.writeheader()
        clean = {k: row.get(k, '') for k in header}
        w.writerow(clean)

def log_loop(now, equity, mode, bias, ok, reason, dbg: dict):
    row = {
        "ts": now.isoformat(),
        "equity": round(float(equity), 4),
        "mode": mode,
        "bias": bias,
        "ok": bool(ok),
        "reason": str(reason),
    }

    for k in [
        "price", "adx15", "ema200_15m", "dist_ema200_pct",
        "ema9", "ema21", "rsi5", "atr5",
        "don_hi", "don_lo",
        "touch", "rejection", "confirm"
    ]:
        if k in dbg:
            row[k] = dbg.get(k)

    header = [
        "ts","equity","mode","bias","ok","reason",
        "price","adx15","ema200_15m","dist_ema200_pct",
        "ema9","ema21","rsi5","atr5",
        "don_hi","don_lo","touch","rejection","confirm"
    ]
    _append_csv(LOOP_LOG, header, row)

def log_trade_open(now, side, price, qty, sl, tp, risk_usd, notional, dbg):
    row = {
        "ts": now.isoformat(),
        "event": "OPEN",
        "side": side,
        "mode": dbg.get("mode", ""),
        "bias": dbg.get("bias", ""),
        "price": round(float(price), 2),
        "qty": float(qty),
        "sl": round(float(sl), 2),
        "tp": round(float(tp), 2),
        "risk_usd": round(float(risk_usd), 4),
        "notional": round(float(notional), 2),
        "adx15": round(float(dbg.get("adx15", 0.0)), 2),
        "rsi5": round(float(dbg.get("rsi5", 0.0)), 2),
        "atr5": round(float(dbg.get("atr5", 0.0)), 2),
        "ema200_15m": round(float(dbg.get("ema200_15m", 0.0)), 2),
        "ema9": round(float(dbg.get("ema9", 0.0)), 2),
        "ema21": round(float(dbg.get("ema21", 0.0)), 2),
        "don_hi": round(float(dbg.get("don_hi", 0.0)), 2) if dbg.get("don_hi") is not None else "",
        "don_lo": round(float(dbg.get("don_lo", 0.0)), 2) if dbg.get("don_lo") is not None else "",
    }
    header = ["ts","event","side","mode","bias","price","qty","sl","tp","risk_usd","notional","adx15","rsi5","atr5","ema200_15m","ema9","ema21","don_hi","don_lo"]
    _append_csv(TRADES_LOG, header, row)

def log_trade_close(now, realized_pnl, loss_streak, trades_today, daily_realized_pnl):
    row = {
        "ts": now.isoformat(),
        "event": "CLOSE",
        "realized_pnl": round(float(realized_pnl), 6),
        "loss_streak": int(loss_streak),
        "trades_today": int(trades_today),
        "daily_realized_pnl": round(float(daily_realized_pnl), 6),
    }
    header = ["ts","event","realized_pnl","loss_streak","trades_today","daily_realized_pnl"]
    _append_csv(TRADES_LOG, header, row)

def log_daily_summary(day_key, start_equity_today, daily_realized_pnl, trades_today, loss_streak):
    row = {
        "day_key": str(day_key),
        "start_equity": round(float(start_equity_today), 4),
        "daily_realized_pnl": round(float(daily_realized_pnl), 6),
        "trades_today": int(trades_today),
        "loss_streak_end": int(loss_streak),
    }
    header = ["day_key","start_equity","daily_realized_pnl","trades_today","loss_streak_end"]
    _append_csv(DAILY_LOG, header, row)


# =========================
# STATE
# =========================
def _dt_to_iso(dt):
    return dt.isoformat() if dt else None

def _iso_to_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s)

def load_state(defaults: dict) -> dict:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            defaults.update(data)
    except Exception as e:
        print("WARN load_state:", e)
    return defaults

def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))
    except Exception as e:
        print("WARN save_state:", e)


# =========================
# ENV + CLIENT
# =========================
load_dotenv(dotenv_path=".env")

API_KEY = (os.getenv("BINANCE_TESTNET_API_KEY") or "").strip()
API_SECRET = (os.getenv("BINANCE_API_SECRET") or "").strip()
TG_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not API_KEY or not API_SECRET:
    raise SystemExit("Missing BINANCE_TESTNET_API_KEY / BINANCE_API_SECRET in .env")

client = Client(API_KEY, API_SECRET, testnet=USE_TESTNET)
client.REQUEST_TIMEOUT = 60

def sync_time_offset() -> bool:
    try:
        server_time = call_with_retry(client.futures_time)["serverTime"]
        local_time = int(time.time() * 1000)
        client.timestamp_offset = server_time - local_time
        return True
    except Exception as e:
        print("WARN sync_time_offset:", e)
        return False

sync_time_offset()


# =========================
# TELEGRAM
# =========================
def send_telegram(msg: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg}, timeout=8)
    except Exception as e:
        print("WARN telegram:", e)

_last_tg = {"key": None, "ts": 0.0}
def send_telegram_throttled(key: str, msg: str, min_seconds: int = 60) -> None:
    now = time.time()
    if _last_tg["key"] == key and (now - _last_tg["ts"]) < min_seconds:
        return
    _last_tg["key"] = key
    _last_tg["ts"] = now
    send_telegram(msg)


# =========================
# BINANCE HELPERS
# =========================
_exchange_cache = {}

def _get_exchange_info():
    if _exchange_cache.get("info"):
        return _exchange_cache["info"]
    info = call_with_retry(client.futures_exchange_info)
    _exchange_cache["info"] = info
    return info

def _get_symbol_filters(symbol: str):
    info = _get_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            return {f["filterType"]: f for f in s["filters"]}
    raise RuntimeError(f"Symbol not found in exchangeInfo: {symbol}")

def get_min_notional(symbol: str, fallback: float = 100.0) -> float:
    try:
        f = _get_symbol_filters(symbol)
        if "MIN_NOTIONAL" in f:
            v = f["MIN_NOTIONAL"].get("notional")
            if v is not None:
                return float(v)
        if "NOTIONAL" in f:
            v = f["NOTIONAL"].get("minNotional")
            if v is not None:
                return float(v)
    except Exception as e:
        print("WARN get_min_notional:", e)
    return float(fallback)

def _quantize_step(value: float, step: float) -> float:
    if step == 0:
        return float(value)
    d = Decimal(str(value))
    s = Decimal(str(step))
    return float((d / s).to_integral_value(rounding=ROUND_DOWN) * s)

def _round_tick(value: float, tick: float) -> float:
    if tick == 0:
        return float(value)
    d = Decimal(str(value))
    t = Decimal(str(tick))
    return float((d / t).to_integral_value(rounding=ROUND_DOWN) * t)

def set_leverage_safe(symbol: str, lev: int):
    try:
        call_with_retry(client.futures_change_leverage, symbol=symbol, leverage=lev, recvWindow=RECV_WINDOW)
    except Exception as e:
        print("WARN set_leverage:", e)

def get_wallet_balance_usdc() -> float:
    try:
        bal = call_with_retry(client.futures_account_balance, recvWindow=RECV_WINDOW)
        for b in bal:
            if b.get("asset") == "USDC":
                return float(b.get("balance", 0.0))
        return 0.0
    except Exception as e:
        print("WARN get_wallet_balance:", e)
        return 0.0

def get_position_amt() -> float:
    try:
        pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        if not pos:
            return 0.0
        return float(pos[0].get("positionAmt", 0.0))
    except Exception as e:
        print("WARN get_position_amt:", e)
        return 0.0

def has_open_position() -> bool:
    return abs(get_position_amt()) > 0.0

def get_unrealized_pnl() -> float:
    try:
        pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        if not pos:
            return 0.0
        return float(pos[0].get("unRealizedProfit", 0.0))
    except Exception as e:
        print("WARN get_unrealized_pnl:", e)
        return 0.0

def cancel_all_open_orders():
    try:
        call_with_retry(client.futures_cancel_all_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except Exception as e:
        print("WARN cancel_all_open_orders:", e)

def get_mark_price() -> float:
    mp = call_with_retry(client.futures_mark_price, symbol=SYMBOL)
    return float(mp["markPrice"])

def klines_df(symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
    raw = call_with_retry(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    cols = [
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","num_trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df


# =========================
# INDICATORS
# =========================
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = avg_gain / (avg_loss.replace(0, 1e-12))
    return 100 - (100 / (1 + rs))

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def adx(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    up = high.diff()
    down = -low.diff()

    plus_dm = pd.Series(0.0, index=df.index)
    minus_dm = pd.Series(0.0, index=df.index)

    plus_dm[(up > down) & (up > 0)] = up[(up > down) & (up > 0)]
    minus_dm[(down > up) & (down > 0)] = down[(down > up) & (down > 0)]

    tr = pd.concat([
        (high - low),
        (high - close.shift(1)).abs(),
        (low - close.shift(1)).abs()
    ], axis=1).max(axis=1)

    atr_s = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/length, adjust=False).mean() / (atr_s.replace(0, 1e-12)))
    minus_di = 100 * (minus_dm.ewm(alpha=1/length, adjust=False).mean() / (atr_s.replace(0, 1e-12)))

    dx = (100 * (plus_di - minus_di).abs() / ((plus_di + minus_di).replace(0, 1e-12)))
    return dx.ewm(alpha=1/length, adjust=False).mean()


# =========================
# REGIME + BIAS
# =========================
def compute_regime_and_bias(prev_mode: str) -> Tuple[str, str, dict]:
    df15 = klines_df(SYMBOL, TF_REGIME, limit=max(EMA_TREND_LEN + 60, 320))
    df15["ema200"] = ema(df15["close"], EMA_TREND_LEN)
    df15["adx"] = adx(df15, ADX_LEN)

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df15.iloc[idx]
    price = float(last["close"])
    e200 = float(last["ema200"])
    ax = float(last["adx"])

    dist_pct = 0.0
    if e200 > 0:
        dist_pct = (price - e200) / e200

    # bias from EMA200 with deadband
    bias = "NONE"
    if abs(dist_pct) >= TREND_DEADBAND_PCT:
        bias = "LONG" if price > e200 else "SHORT"

    # hysteresis mode switch
    mode = prev_mode if prev_mode in ("TREND","RANGE") else "RANGE"
    if ax >= ADX_TREND_ON:
        mode = "TREND"
    elif ax <= ADX_RANGE_ON:
        mode = "RANGE"

    dbg = {
        "price": price,
        "adx15": ax,
        "ema200_15m": e200,
        "dist_ema200_pct": dist_pct,
    }
    return mode, bias, dbg


# =========================
# SIGNALS
# =========================
def compute_entry_indicators_5m() -> Tuple[pd.DataFrame, dict]:
    df5 = klines_df(SYMBOL, TF_ENTRY, limit=260)

    df5["ema9"] = ema(df5["close"], EMA_FAST)
    df5["ema21"] = ema(df5["close"], EMA_SLOW)
    df5["rsi"] = rsi(df5["close"], RSI_LEN)
    df5["atr"] = atr(df5, 14)

    hi = df5["high"].rolling(DONCHIAN_LEN).max().shift(1)
    lo = df5["low"].rolling(DONCHIAN_LEN).min().shift(1)

    df5["don_hi"] = hi
    df5["don_lo"] = lo

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    dbg = {
        "ema9": float(last["ema9"]),
        "ema21": float(last["ema21"]),
        "rsi5": float(last["rsi"]),
        "atr5": float(last["atr"]),
        "don_hi": float(last["don_hi"]) if pd.notna(last["don_hi"]) else None,
        "don_lo": float(last["don_lo"]) if pd.notna(last["don_lo"]) else None,
    }

    return df5, dbg

def signal_trend_mode(df5: pd.DataFrame, bias: str) -> Tuple[bool, Optional[str], dict]:
    if bias not in ("LONG", "SHORT"):
        return False, None, {"reason": "bias_none_or_deadband"}

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]
    
    ema9v = float(last["ema9"])
    ema21v = float(last["ema21"])
    rsiv = float(last["rsi"])
    atrv = float(last["atr"])

    if atrv <= 0:
        return False, None, {"reason": "atr_bad"}

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])

    pullback_touch_long = float(last["low"]) <= ema21v
    pullback_touch_short = float(last["high"]) >= ema21v

    dbg = {"touch": False, "rejection": False, "confirm": False}

    if bias == "LONG":
        if ema9v <= ema21v:
            return False, None, {"reason": "ema_not_aligned_long", **dbg}
        if rsiv < RSI_TREND_LONG_MIN:
            return False, None, {"reason": "rsi_low_trend_long", **dbg}
        if not pullback_touch_long:
            return False, None, {"reason": "no_pullback_touch", **dbg}
        if (not candle_bull) or (float(last["close"]) < ema9v):
            dbg["touch"] = True
            return False, None, {"reason": "no_bull_confirm", **dbg}

        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return True, "BUY", {"reason": "trend_entry", **dbg}

    # SHORT
    if ema9v >= ema21v:
        return False, None, {"reason": "ema_not_aligned_short", **dbg}
    if rsiv > RSI_TREND_SHORT_MAX:
        return False, None, {"reason": "rsi_high_trend_short", **dbg}
    if not pullback_touch_short:
        return False, None, {"reason": "no_pullback_touch", **dbg}
    if (not candle_bear) or (float(last["close"]) > ema9v):
        dbg["touch"] = True
        return False, None, {"reason": "no_bear_confirm", **dbg}

    dbg.update({"touch": True, "rejection": True, "confirm": True})
    return True, "SELL", {"reason": "trend_entry", **dbg}

def signal_range_mode(df5: pd.DataFrame) -> Tuple[bool, Optional[str], dict]:
    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]
    don_hi = last["don_hi"]
    don_lo = last["don_lo"]
    atrv = float(last["atr"])
    rsiv = float(last["rsi"])
    if pd.isna(don_hi) or pd.isna(don_lo) or atrv <= 0:
        return False, None, {"reason": "donchian_not_ready"}

    don_hi = float(don_hi)
    don_lo = float(don_lo)

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])

    # Touch logic
    touch_low = float(last["low"]) <= don_lo
    touch_high = float(last["high"]) >= don_hi

    # Rejection back inside
    rej_long = touch_low and (float(last["close"]) > don_lo) and candle_bull
    rej_short = touch_high and (float(last["close"]) < don_hi) and candle_bear

    dbg_base = {"touch": False, "rejection": False, "confirm": False}

    # LONG at lower band
    if rej_long and rsiv >= RSI_RANGE_LONG_MIN:
        dbg_base.update({"touch": True, "rejection": True, "confirm": True})
        return True, "BUY", {"reason": "range_long_reject", **dbg_base}

    # SHORT at upper band
    if rej_short and rsiv <= RSI_RANGE_SHORT_MAX:
        dbg_base.update({"touch": True, "rejection": True, "confirm": True})
        return True, "SELL", {"reason": "range_short_reject", **dbg_base}

    if touch_low and not rej_long:
        dbg_base.update({"touch": True})
        return False, None, {"reason": "range_touch_low_no_reject", **dbg_base}
    if touch_high and not rej_short:
        dbg_base.update({"touch": True})
        return False, None, {"reason": "range_touch_high_no_reject", **dbg_base}

    return False, None, {"reason": "range_no_touch", **dbg_base}


# =========================
# RISK & ORDERS
# =========================
def calc_sl_tp(price: float, side: str, atr_val: float, mode: str) -> Tuple[float,float,float]:
    if mode == "TREND":
        sl_dist = atr_val * TREND_SL_ATR_MULT
        sl_min = price * TREND_SL_MIN_PCT
        sl_max = price * TREND_SL_MAX_PCT
        rr = TREND_RR
    else:
        sl_dist = atr_val * RANGE_SL_ATR_MULT
        sl_min = price * RANGE_SL_MIN_PCT
        sl_max = price * RANGE_SL_MAX_PCT
        rr = RANGE_RR

    sl_dist = max(sl_min, min(sl_dist, sl_max))

    if side == "BUY":
        sl = price - sl_dist
        tp = price + sl_dist * rr
    else:
        sl = price + sl_dist
        tp = price - sl_dist * rr

    return sl, tp, sl_dist

def calc_qty_from_risk(equity: float, price: float, sl_dist: float) -> Tuple[float,float,float]:
    risk_usd = equity * RISK_PCT
    if sl_dist <= 0:
        return 0.0, 0.0, 0.0

    qty = risk_usd / sl_dist

    # cap notional exposure
    max_notional = equity * LEVERAGE * MAX_NOTIONAL_FRACTION_OF_EQUITY
    approx_notional = qty * price
    if max_notional > 0 and approx_notional > max_notional:
        qty = max_notional / price
        approx_notional = qty * price

    return qty, risk_usd, approx_notional

def place_entry_and_bracket(side: str, qty: float, sl_price: float, tp_price: float) -> Tuple[float, float, float]:
    filters = _get_symbol_filters(SYMBOL)
    step = float(filters["LOT_SIZE"]["stepSize"])
    tick = float(filters["PRICE_FILTER"]["tickSize"])

    qty_q = _quantize_step(qty, step)
    if qty_q <= 0:
        raise RuntimeError("qty <= 0 after quantize")

    sl_q = _round_tick(sl_price, tick)
    tp_q = _round_tick(tp_price, tick)

    bracket_side = "SELL" if side == "BUY" else "BUY"

    def _has_position() -> bool:
        try:
            return abs(get_position_amt()) > 0.0
        except Exception:
            return False

    def _emergency_close_position() -> None:
        pos_amt_raw = get_position_amt()
        if abs(pos_amt_raw) <= 0:
            return

        emergency_side = "SELL" if pos_amt_raw > 0 else "BUY"
        qty_close = _quantize_step(abs(pos_amt_raw), step)
        if qty_close <= 0:
            raise RuntimeError(f"emergency close qty invalid: pos_amt={pos_amt_raw}")

        call_with_retry(
            client.futures_create_order,
            symbol=SYMBOL,
            side=emergency_side,
            type="MARKET",
            quantity=qty_close,
            recvWindow=RECV_WINDOW
        )
        
    def _verify_brackets(expected_side: str) -> Tuple[bool, bool]:
        open_orders = call_with_retry(
            client.futures_get_open_orders,
            symbol=SYMBOL,
            recvWindow=RECV_WINDOW
        )

        has_sl = False
        has_tp = False

        for o in open_orders:
            o_side = o.get("side")
            o_type = o.get("type")

            if o_side != expected_side:
                continue

            if o_type == "STOP_MARKET":
                has_sl = True
            elif o_type == "TAKE_PROFIT_MARKET":
                has_tp = True

        return has_sl, has_tp

    cancel_all_open_orders()

    entry_resp = call_with_retry(
        client.futures_create_order,
        symbol=SYMBOL,
        side=side,
        type=ORDER_TYPE_ENTRY,
        quantity=qty_q,
        recvWindow=RECV_WINDOW
    )

    time.sleep(0.35)

    try:
        sl_resp = call_with_retry(
            client.futures_create_order,
            symbol=SYMBOL,
            side=bracket_side,
            type="STOP_MARKET",
            stopPrice=sl_q,
            closePosition=True,
            recvWindow=RECV_WINDOW
        )

        tp_resp = call_with_retry(
            client.futures_create_order,
            symbol=SYMBOL,
            side=bracket_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=tp_q,
            closePosition=True,
            recvWindow=RECV_WINDOW
        )

        # Retry verify untuk hindari false alarm karena API / sync delay
        has_sl = False
        has_tp = False

        for _ in range(5):
            time.sleep(0.6)
            has_sl, has_tp = _verify_brackets(bracket_side)
            if has_sl and has_tp:
                return qty_q, sl_q, tp_q

        raise RuntimeError(
            f"Bracket missing after placement | has_sl={has_sl} has_tp={has_tp} "
            f"| entryId={entry_resp.get('orderId')} "
            f"| slId={sl_resp.get('orderId')} "
            f"| tpId={tp_resp.get('orderId')}"
        )

    except Exception as e:
        # Bersihkan order sisa dulu
        try:
            cancel_all_open_orders()
        except Exception as cancel_err:
            print("WARN cancel after bracket failure:", cancel_err)

        # Kalau posisi masih ada, tutup darurat
        try:
            if _has_position():
                _emergency_close_position()
                send_telegram(
                    f"🚨 EMERGENCY: Gagal pasang SL/TP. Posisi DITUTUP OTOMATIS!\nErr: {e}"
                )
            else:
                send_telegram(
                    f"⚠️ Bracket gagal, tapi posisi sudah tidak ada.\nErr: {e}"
                )
        except Exception as close_err:
            send_telegram(
                f"🛑 FATAL: Bracket gagal dan emergency close gagal!\n"
                f"BracketErr: {e}\nCloseErr: {close_err}"
            )
            raise RuntimeError(
                f"Bracket failed and emergency close failed | bracket_err={e} | close_err={close_err}"
            )

        raise RuntimeError(f"Bracket failed after entry: {e}")

# =========================
# PNL (income history)
# =========================
def realized_pnl_since(start_ms: int) -> float:
    try:
        incomes = call_with_retry(
            client.futures_income_history,
            symbol=SYMBOL,
            incomeType="REALIZED_PNL",
            startTime=start_ms,
            limit=1000,
            recvWindow=RECV_WINDOW
        )
        total = 0.0
        for it in incomes or []:
            total += float(it.get("income", 0.0))
        return total
    except Exception as e:
        print("WARN realized_pnl_since:", e)
        return 0.0

def realized_pnl_since_retry(start_ms: int, retries: int = 5, sleep_s: float = 1.5) -> float:
    last = 0.0
    for _ in range(retries):
        v = realized_pnl_since(start_ms)
        last = v
        if abs(v) > 1e-9:
            return v
        time.sleep(sleep_s)
    return last


# =========================
# MAIN
# =========================
def main():
    set_leverage_safe(SYMBOL, LEVERAGE)

    min_notional = get_min_notional(SYMBOL, fallback=100.0)

    print("MinNotional:", min_notional)
    print(f"{SYMBOL} V4 HYBRID START (REAL) | Lev:{LEVERAGE} | Regime:{TF_REGIME} ADX{ADX_LEN} | Entry:{TF_ENTRY}")

    send_telegram_throttled(
        "startup",
        f"🟢 {TG_PREFIX} started\n"
        f"Symbol: {SYMBOL}\nLev: {LEVERAGE}\n"
        f"Mode switch: TREND if ADX15>={ADX_TREND_ON}, RANGE if ADX15<={ADX_RANGE_ON}\n"
        f"Trend bias: EMA{EMA_TREND_LEN} {TF_REGIME} (deadband {TREND_DEADBAND_PCT*100:.2f}%)\n"
        f"TREND entry: EMA{EMA_FAST}/EMA{EMA_SLOW} {TF_ENTRY} + RSI (long>={RSI_TREND_LONG_MIN}, short<={RSI_TREND_SHORT_MAX})\n"
        f"RANGE entry: Donchian{DONCHIAN_LEN} {TF_ENTRY} reject + RSI (long>={RSI_RANGE_LONG_MIN}, short<={RSI_RANGE_SHORT_MAX})\n"
        f"Risk: {RISK_PCT*100:.2f}% | DailyStop: {MAX_DAILY_DRAWDOWN_PCT*100:.1f}% | MaxTrades/day: {MAX_TRADES_PER_DAY}\n"
        f"Cooldown: {COOLDOWN_MINUTES}m | LossStreakStop: {LOSS_STREAK_LIMIT}\n"
        f"MinNotional: {min_notional}",
        min_seconds=5,
    )

    defaults = {
        "day_key": datetime.now(timezone.utc).date().isoformat(),
        "trades_today": 0,
        "loss_streak": 0,
        "daily_locked": False,
        "cooldown_until": None,
        "start_equity_today": get_wallet_balance_usdc(),
        "prev_in_position": has_open_position(),
        "last_pnl_check_ms": int(time.time() * 1000) - 60_000,
        "daily_realized_pnl": 0.0,
        "mode": "RANGE",
    }
    st = load_state(defaults)

    day_key = st["day_key"]
    trades_today = int(st["trades_today"])
    loss_streak = int(st["loss_streak"])
    daily_locked = bool(st["daily_locked"])
    cooldown_until = _iso_to_dt(st["cooldown_until"])
    start_equity_today = float(st["start_equity_today"])
    prev_in_position = bool(st["prev_in_position"])
    last_pnl_check_ms = int(st["last_pnl_check_ms"])
    daily_realized_pnl = float(st.get("daily_realized_pnl", 0.0))
    mode = st.get("mode", "RANGE")

    def _save_state():
        save_state(
            {
                "day_key": day_key,
                "trades_today": trades_today,
                "loss_streak": loss_streak,
                "daily_locked": daily_locked,
                "cooldown_until": _dt_to_iso(cooldown_until),
                "start_equity_today": start_equity_today,
                "prev_in_position": prev_in_position,
                "last_pnl_check_ms": last_pnl_check_ms,
                "daily_realized_pnl": daily_realized_pnl,
                "mode": mode,
            }
        )

    last_time_sync = 0.0
    TIME_SYNC_EVERY_S = 30 * 60

    while True:
        try:
            now = datetime.now(timezone.utc)

            # periodic timestamp resync
            if (time.time() - last_time_sync) >= TIME_SYNC_EVERY_S:
                if sync_time_offset():
                    last_time_sync = time.time()

            # daily reset
            cur_day = now.date().isoformat()
            if cur_day != day_key:
                if day_key:
                    log_daily_summary(day_key, start_equity_today, daily_realized_pnl, trades_today, loss_streak)

                day_key = cur_day
                trades_today = 0
                loss_streak = 0
                daily_locked = False
                cooldown_until = None
                start_equity_today = get_wallet_balance_usdc()
                last_pnl_check_ms = int(time.time() * 1000) - 60_000
                daily_realized_pnl = 0.0
                mode = "RANGE"

                send_telegram_throttled(
                    "new_day",
                    f"🗓 New day {SYMBOL}\nStart equity: {round(start_equity_today,2)} USDC\nLocks reset.",
                    min_seconds=5,
                )
                _save_state()

            if daily_locked:
                time.sleep(SLEEP_SECONDS)
                continue

            equity_now = get_wallet_balance_usdc()
            in_pos = has_open_position()

            if start_equity_today <= 0:
                start_equity_today = equity_now

            unrealized_pnl = get_unrealized_pnl() if in_pos else 0.0
            total_daily_pnl = daily_realized_pnl + unrealized_pnl
            daily_dd = (total_daily_pnl / start_equity_today) if start_equity_today > 0 else 0.0

            # daily hard stop
            if daily_dd <= -abs(MAX_DAILY_DRAWDOWN_PCT):
                daily_locked = True
                send_telegram_throttled(
                    "daily_hard_stop",
                    f"🛑 DAILY HARD STOP {SYMBOL}\nDD: {round(daily_dd*100,2)}%\nTotalPnLToday: {round(total_daily_pnl,2)} USDC\nLocked until tomorrow.",
                    min_seconds=30,
                )
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            # trade limit
            if trades_today >= MAX_TRADES_PER_DAY:
                daily_locked = True
                send_telegram_throttled(
                    "trade_limit",
                    f"⚠️ Trade limit reached {SYMBOL}\ntrades_today: {trades_today}\nLocked until tomorrow.",
                    min_seconds=30,
                )
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            # cooldown
            if cooldown_until and now < cooldown_until:
                time.sleep(SLEEP_SECONDS)
                continue

            # if in position, just wait
            if in_pos:
                prev_in_position = True
                time.sleep(SLEEP_SECONDS)
                continue

            # detect just closed position
            if prev_in_position and not in_pos:
                end_ms = int(time.time() * 1000)
                pnl = realized_pnl_since_retry(last_pnl_check_ms)
                last_pnl_check_ms = end_ms
                daily_realized_pnl += pnl

                log_trade_close(now, pnl, loss_streak, trades_today, daily_realized_pnl)

                if pnl < 0:
                    loss_streak += 1
                else:
                    loss_streak = 0

                send_telegram(
                    f"✅ Position closed {SYMBOL}\nRealizedPnL~: {round(pnl,4)} USDC\nLossStreak: {loss_streak}\nCooldown: {COOLDOWN_MINUTES}m"
                )

                if loss_streak >= LOSS_STREAK_LIMIT:
                    daily_locked = True
                    send_telegram(f"🧯 LOSS STREAK STOP {SYMBOL}\nLossStreak: {loss_streak}\nLocked until tomorrow.")
                    prev_in_position = False
                    _save_state()
                    time.sleep(SLEEP_SECONDS)
                    continue

                cooldown_until = now + pd.Timedelta(minutes=COOLDOWN_MINUTES)
                prev_in_position = False
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            prev_in_position = False

            # -------- REGIME + BIAS --------
            mode_new, bias, dbg15 = compute_regime_and_bias(mode)
            mode = mode_new

            # -------- ENTRY INDICATORS --------
            df5, dbg5 = compute_entry_indicators_5m()

            # -------- SIGNAL --------
            if mode == "TREND":
                ok, side, sig_dbg = signal_trend_mode(df5, bias)

            elif mode == "RANGE" and ENABLE_RANGE_MODE:
                ok, side, sig_dbg = signal_range_mode(df5)

            else:
                ok, side, sig_dbg = False, None, {"reason": "range_disabled"}

            dbg = {**dbg15, **dbg5, **sig_dbg, "mode": mode, "bias": bias}
            reason = sig_dbg.get("reason", "-")

            print(
                now,
                "eq:", round(equity_now, 2),
                "| mode:", mode,
                "| bias:", bias,
                "| adx15:", round(float(dbg15.get("adx15", 0.0)), 2),
                "| ok:", ok,
                "|", reason,
            )
            log_loop(now, equity_now, mode, bias, ok, reason, dbg)

            if (not ok) or (side is None):
                time.sleep(SLEEP_SECONDS)
                continue

            # price & sl/tp
            price = get_mark_price()
            atr_val = float(dbg.get("atr5", 0.0))
            if atr_val <= 0:
                time.sleep(SLEEP_SECONDS)
                continue

            sl_price, tp_price, sl_dist = calc_sl_tp(price, side, atr_val, mode)
            qty, risk_usd, _ = calc_qty_from_risk(equity_now, price, sl_dist)

            filters = _get_symbol_filters(SYMBOL)
            step = float(filters["LOT_SIZE"]["stepSize"])
            qty_q = _quantize_step(qty, step)
            notional_q = qty_q * price

            if (qty_q <= 0) or (notional_q < min_notional):
                send_telegram_throttled(
                    "min_notional_skip",
                    f"⚠️ Skip entry (minNotional)\n"
                    f"Need >= {min_notional} USDC\n"
                    f"After quantize: qty={qty_q} notional~{round(notional_q,2)}\n"
                    f"Eq: {round(equity_now,2)} | Risk~: {round(risk_usd,2)} | SLdist: {round(sl_dist,2)}",
                    min_seconds=120,
                )
                time.sleep(SLEEP_SECONDS)
                continue

            qty_final, sl_final, tp_final = place_entry_and_bracket(side, qty_q, sl_price, tp_price)
            trades_today += 1
            cooldown_until = now + pd.Timedelta(minutes=COOLDOWN_MINUTES)
            _save_state()

            send_telegram(
                f"🚀 ENTRY {SYMBOL} {('LONG' if side=='BUY' else 'SHORT')}\n"
                f"Mode: {mode} | Bias: {bias}\n"
                f"Qty: {qty_final}\nMark: {round(price,2)}\n"
                f"SL: {round(sl_final,2)} | TP: {round(tp_final,2)}\n"
                f"Risk~: {round(risk_usd,2)} USDC | Notional~: {round(notional_q,2)}\n"
                f"ADX15: {round(float(dbg.get('adx15',0.0)),2)} | RSI5: {round(float(dbg.get('rsi5',0.0)),2)} | ATR5: {round(float(dbg.get('atr5',0.0)),2)}\n"
                f"TradesToday: {trades_today}"
            )

            log_trade_open(now, side, price, qty_final, sl_final, tp_final, risk_usd, notional_q, dbg)
            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            msg = str(e)
            if ("Timestamp for this request" in msg) or ("recvWindow" in msg) or ("-1021" in msg):
                sync_time_offset()
                time.sleep(2)
                continue

            send_telegram_throttled(
                "runtime_error",
                f"❌ ERROR {SYMBOL}\n{repr(e)}",
                min_seconds=60,
            )
            print("ERROR:", repr(e))
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()