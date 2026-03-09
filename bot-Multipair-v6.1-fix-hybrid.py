#!/usr/bin/env python3
# bot-multipair-v6-hybrid.py
# Multipair USD-M Futures - HYBRID Trend + Range
# Safer execution engine:
# - Actual entry price after fill
# - Bracket placement without aggressive open-order verification
# - Emergency close with reduceOnly=True
# - Bracket fail streak auto-lock
# - Atomic state save
# - Realized PnL tracking with tranId dedupe

import os
import time
import json
import csv
import random
import traceback
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Tuple, Optional

import requests
import pandas as pd
import numpy as np  # Ditambahkan untuk perhitungan CHOP
from dotenv import load_dotenv
from binance.client import Client
from requests.exceptions import ReadTimeout, ConnectionError


# =========================
# RETRY WRAPPER
# =========================
# =========================
# RETRY WRAPPER & RATE LIMITER
# =========================
from binance.exceptions import BinanceAPIException

def call_with_retry(fn, *args, retries: int = 5, base_sleep: float = 1.5, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except BinanceAPIException as e:
            last_err = e
            msg = str(e)
            
            # Jika terdeteksi ban atau rate limit dari Binance
            if getattr(e, "code", None) == -1003 or "banned until" in msg.lower() or "too many requests" in msg.lower():
                sleep_for = min(120, int(base_sleep * (2 ** i) * 10)) # Tidur agak lama (15s - 120s)
                print(f"WARN rate limit/bin ban detected. Sleep {sleep_for}s")
                time.sleep(sleep_for)
                continue

            if getattr(e, "code", None) in (-1021,):
                print("WARN timestamp drift, resync time")
                sync_time_offset()
                time.sleep(2)
                continue

            time.sleep(base_sleep * (i + 1))
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            time.sleep(base_sleep * (i + 1))
        except Exception as e:
            last_err = e
            time.sleep(base_sleep * (i + 1))
    raise last_err


# =========================
# CONFIG
# =========================
PAIR = "BCHUSDT"
BASE_ASSET = PAIR[:-4]
QUOTE_ASSET = PAIR[-4:]

SYMBOL = PAIR
LEVERAGE = 15
USE_TESTNET = True

# Diubah ke False agar bot fokus menangkap tren (Trend-Following)
ENABLE_RANGE_MODE =True
USE_CLOSED_CANDLE_ONLY = True

TF_REGIME = "15m"
TF_ENTRY = "5m"

# Regime detection (MENGGUNAKAN CHOP)
CHOP_LEN = 14
CHOP_TREND_ON = 38.2
CHOP_RANGE_ON = 61.8

# Trend bias
EMA_TREND_LEN = 200
TREND_DEADBAND_PCT = 0.0008

# TREND entry (Optimasi EMA 20/50 & RSI 50)
EMA_FAST = 20
EMA_SLOW = 50
RSI_LEN = 14
RSI_TREND_LONG_MIN = 50
RSI_TREND_SHORT_MAX = 50
TREND_RR = 1.0  # RR dinaikkan untuk memaksimalkan cuan di saat tren
TREND_SL_ATR_MULT = 2.5  # SL dilonggarkan sedikit
TREND_SL_MIN_PCT = 0.0060
TREND_SL_MAX_PCT = 0.0400

# V6 - structure aware stop loss
USE_STRUCTURE_SL = True
SWING_LOOKBACK_BARS = 20
TREND_SWING_BUFFER_ATR_MULT = 1.5  # Buffer di bawah swing dilebarkan aman dari jarum
RANGE_SWING_BUFFER_ATR_MULT = 1.0
TREND_STRUCTURE_SL_HARD_MAX_PCT = 0.0500
RANGE_STRUCTURE_SL_HARD_MAX_PCT = 0.0250

# RANGE entry (Tetap ada tapi tidak dieksekusi selama ENABLE_RANGE_MODE = False)
DONCHIAN_LEN = 16
RSI_RANGE_LONG_MIN = 43
RSI_RANGE_SHORT_MAX = 57
RANGE_RR = 0.8
RANGE_SL_ATR_MULT = 1.5
RANGE_SL_MIN_PCT = 0.0018
RANGE_SL_MAX_PCT = 0.0200

# Risk
RISK_PCT = 0.005
MAX_NOTIONAL_FRACTION_OF_EQUITY = 0.35
MAX_FORCED_RISK_PCT = 0.03

# Safety
MAX_TRADES_PER_DAY = 40
COOLDOWN_MINUTES = 5
LOSS_STREAK_LIMIT = 6
MAX_DAILY_DRAWDOWN_PCT = 0.05
BRACKET_FAIL_LOCK_LIMIT = 3

# V6 - jangan spam telegram saat close tapi PnL Binance belum muncul
PNL_FETCH_RETRIES_PER_LOOP = 1
PNL_FETCH_WAIT_SEC = 2.0
PNL_RECHECK_DELAY_MINUTES = 7

# V6.1 - hybrid TP exit
USE_HYBRID_TP_EXIT = True
TP_LIMIT_MAKER_OFFSET_TICKS = 1
TP_FALLBACK_BUFFER_ATR_MULT = 0.12
TP_FALLBACK_MIN_PCT = 0.00025
TP_FALLBACK_MAX_PCT = 0.00080
TP_FALLBACK_COOLDOWN_SECONDS = 45

# anti rate-limit tuning
IN_POSITION_SLEEP_SECONDS = 10
TP_MANAGE_MARK_CHECK_COOLDOWN_SECONDS = 12

SLEEP_SECONDS = 30
RECV_WINDOW = 10_000

TG_PREFIX = f"{PAIR} V6 HYBRID {'TESTNET' if USE_TESTNET else 'REAL'}"

state_mode = "testnet" if USE_TESTNET else "real"
STATE_FILE = Path(f".state_{PAIR.lower()}_v6_hybrid_{state_mode}.json")

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOOP_LOG = LOG_DIR / f"loop_{PAIR.lower()}_v6.csv"
TRADES_LOG = LOG_DIR / f"trades_{PAIR.lower()}_v6.csv"
DAILY_LOG = LOG_DIR / f"daily_{PAIR.lower()}_v6.csv"


# =========================
# LOGGING
# =========================
def _ensure_log_dir():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

def _append_csv(path: Path, header: list, row: dict):
    _ensure_log_dir()
    new_file = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if new_file:
            w.writeheader()
        clean = {k: row.get(k, "") for k in header}
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
        "price", "chop15", "ema200_15m", "dist_ema200_pct",
        "ema_fast", "ema_slow", "rsi5", "atr5",
        "don_hi", "don_lo", "touch", "rejection", "confirm"
    ]:
        if k in dbg:
            row[k] = dbg.get(k)

    header = [
        "ts", "equity", "mode", "bias", "ok", "reason",
        "price", "chop15", "ema200_15m", "dist_ema200_pct",
        "ema_fast", "ema_slow", "rsi5", "atr5",
        "don_hi", "don_lo", "touch", "rejection", "confirm"
    ]
    _append_csv(LOOP_LOG, header, row)

def log_trade_open(now, side, entry_price, qty, sl, tp, risk_usd, notional, dbg):
    row = {
        "ts": now.isoformat(),
        "event": "OPEN",
        "side": side,
        "mode": dbg.get("mode", ""),
        "bias": dbg.get("bias", ""),
        "entry": round(float(entry_price), 6),
        "qty": float(qty),
        "sl": round(float(sl), 6),
        "tp": round(float(tp), 6),
        "risk_usd": round(float(risk_usd), 6),
        "notional": round(float(notional), 6),
        "chop15": round(float(dbg.get("chop15", 0.0)), 4),
        "rsi5": round(float(dbg.get("rsi5", 0.0)), 4),
        "atr5": round(float(dbg.get("atr5", 0.0)), 6),
        "ema200_15m": round(float(dbg.get("ema200_15m", 0.0)), 6),
        "ema_fast": round(float(dbg.get("ema_fast", 0.0)), 6),
        "ema_slow": round(float(dbg.get("ema_slow", 0.0)), 6),
        "don_hi": round(float(dbg.get("don_hi", 0.0)), 6) if dbg.get("don_hi") is not None else "",
        "don_lo": round(float(dbg.get("don_lo", 0.0)), 6) if dbg.get("don_lo") is not None else "",
    }
    header = [
        "ts", "event", "side", "mode", "bias", "entry", "qty", "sl", "tp",
        "risk_usd", "notional", "chop15", "rsi5", "atr5",
        "ema200_15m", "ema_fast", "ema_slow", "don_hi", "don_lo"
    ]
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
    header = ["ts", "event", "realized_pnl", "loss_streak", "trades_today", "daily_realized_pnl"]
    _append_csv(TRADES_LOG, header, row)

def log_daily_summary(day_key, start_equity_today, daily_realized_pnl, trades_today, loss_streak):
    row = {
        "day_key": str(day_key),
        "start_equity": round(float(start_equity_today), 6),
        "daily_realized_pnl": round(float(daily_realized_pnl), 6),
        "trades_today": int(trades_today),
        "loss_streak_end": int(loss_streak),
    }
    header = ["day_key", "start_equity", "daily_realized_pnl", "trades_today", "loss_streak_end"]
    _append_csv(DAILY_LOG, header, row)


# =========================
# STATE
# =========================
def _dt_to_iso(dt):
    return dt.isoformat() if dt else None

def _iso_to_dt(s):
    try:
        return datetime.fromisoformat(s) if s else None
    except Exception:
        return None

def load_state(defaults: dict) -> dict:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            defaults.update(data)
    except Exception as e:
        print("WARN load_state:", e)

    if "seen_tran_ids" in defaults and isinstance(defaults["seen_tran_ids"], list):
        defaults["seen_tran_ids"] = set(defaults["seen_tran_ids"])
    return defaults

def save_state(state: dict) -> None:
    try:
        st_copy = state.copy()
        if "seen_tran_ids" in st_copy and isinstance(st_copy["seen_tran_ids"], set):
            st_copy["seen_tran_ids"] = list(st_copy["seen_tran_ids"])
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st_copy, indent=2, sort_keys=True))
        tmp.replace(STATE_FILE)
    except Exception as e:
        print("WARN save_state:", e)


# =========================
# ENV + CLIENT
# =========================
load_dotenv(dotenv_path=".env")

if USE_TESTNET:
    API_KEY = (os.getenv("BINANCE_TESTNET_API_KEY") or "").strip()
    API_SECRET = (os.getenv("BINANCE_TESTNET_API_SECRET") or "").strip()
else:
    API_KEY = (os.getenv("BINANCE_API_KEY") or "").strip()
    API_SECRET = (os.getenv("BINANCE_API_SECRET") or "").strip()

TG_TOKEN = (os.getenv("TELEGRAM_TOKEN") or "").strip()
TG_CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()

if not API_KEY or not API_SECRET:
    key_name = "BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET" if USE_TESTNET else "BINANCE_API_KEY / BINANCE_API_SECRET"
    raise SystemExit(f"Missing {key_name} in .env")

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
    now_ts = time.time()
    if _last_tg["key"] == key and (now_ts - _last_tg["ts"]) < min_seconds:
        return
    _last_tg["key"] = key
    _last_tg["ts"] = now_ts
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

def get_wallet_balance_quote() -> float:
    try:
        bal = call_with_retry(client.futures_account_balance, recvWindow=RECV_WINDOW)
        for b in bal:
            if b.get("asset") == QUOTE_ASSET:
                return float(b.get("availableBalance", 0.0))
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

def get_position_snapshot():
    try:
        pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        if not pos:
            return 0.0, 0.0, 0.0
        p = pos[0]
        return (
            float(p.get("positionAmt", 0.0)),
            float(p.get("unRealizedProfit", 0.0)),
            float(p.get("entryPrice", 0.0)),
        )
    except Exception as e:
        print("WARN get_position_snapshot:", e)
        return 0.0, 0.0, 0.0

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

# =========================
# GLOBAL CACHE SYSTEM
# =========================
_cache = {
    "balance": {"value": 0.0, "ts": 0.0},
    "position": {"data": (0.0, 0.0, 0.0), "ts": 0.0},
    "mark_price": {"value": 0.0, "ts": 0.0},
    "klines": {}
}

def get_wallet_balance_quote() -> float:
    now = time.time()
    # Cache balance selama 45 detik
    if (now - _cache["balance"]["ts"]) < 45.0:
        return _cache["balance"]["value"]

    try:
        bal = call_with_retry(client.futures_account_balance, recvWindow=RECV_WINDOW)
        val = 0.0
        for b in bal:
            if b.get("asset") == QUOTE_ASSET:
                val = float(b.get("availableBalance", 0.0))
                break
        _cache["balance"]["value"] = val
        _cache["balance"]["ts"] = now
        return val
    except Exception as e:
        print("WARN get_wallet_balance_cached:", e)
        return _cache["balance"]["value"]

def _get_position_snapshot_cached(ttl: int = 3):
    now = time.time()
    if (now - _cache["position"]["ts"]) < ttl:
        return _cache["position"]["data"]

    try:
        pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        if not pos:
            data = (0.0, 0.0, 0.0)
        else:
            p = pos[0]
            data = (
                float(p.get("positionAmt", 0.0)),
                float(p.get("unRealizedProfit", 0.0)),
                float(p.get("entryPrice", 0.0)),
            )
        _cache["position"]["data"] = data
        _cache["position"]["ts"] = now
        return data
    except Exception as e:
        print("WARN _get_position_snapshot_cached:", e)
        return _cache["position"]["data"]

# Tiga fungsi di bawah ini sekarang hanya mengambil data dari cache (tidak nembak API baru)
def get_position_amt() -> float:
    pos_amt, _, _ = _get_position_snapshot_cached()
    return pos_amt

def get_position_snapshot():
    return _get_position_snapshot_cached()

def has_open_position() -> bool:
    pos_amt, _, _ = _get_position_snapshot_cached()
    return abs(pos_amt) > 0.0

def get_unrealized_pnl() -> float:
    _, upnl, _ = _get_position_snapshot_cached()
    return upnl

def get_mark_price() -> float:
    now = time.time()
    # Cache mark price selama 2.5 detik
    if _cache["mark_price"]["value"] > 0 and (now - _cache["mark_price"]["ts"]) < 2.5:
        return _cache["mark_price"]["value"]

    try:
        mp = call_with_retry(client.futures_mark_price, symbol=SYMBOL)
        val = float(mp["markPrice"])
        _cache["mark_price"]["value"] = val
        _cache["mark_price"]["ts"] = now
        return val
    except Exception as e:
        print("WARN get_mark_price_cached:", e)
        return _cache["mark_price"]["value"]

def interval_seconds(interval: str) -> int:
    mp = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "1h": 3600}
    return mp.get(interval, 300)

def klines_df(symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
    key = f"{symbol}_{interval}_{limit}"
    now = time.time()
    
    # Cache klines: 5m -> valid 60 detik, 15m -> valid 180 detik
    ttl = max(15, int(interval_seconds(interval) * 0.2)) 

    cached = _cache["klines"].get(key)
    if cached and (now - cached["ts"]) < ttl:
        return cached["df"]

    raw = call_with_retry(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    _cache["klines"][key] = {"df": df, "ts": now}
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
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
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
    return tr.ewm(alpha=1 / length, adjust=False).mean()

def chop(df: pd.DataFrame, length: int = 14) -> pd.Series:
    # Menghitung True Range (TR)
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    
    atr_sum = tr.rolling(length).sum()
    highest_high = df["high"].rolling(length).max()
    lowest_low = df["low"].rolling(length).min()
    
    # Mencegah error pembagian dengan nol
    range_hl = (highest_high - lowest_low).replace(0, 1e-12)
    
    # Rumus asli Choppiness Index
    chop_idx = 100 * np.log10(atr_sum / range_hl) / np.log10(length)
    return chop_idx

def vwap(df: pd.DataFrame) -> pd.Series:
    # Typical Price = (High + Low + Close) / 3
    tp = (df["high"] + df["low"] + df["close"]) / 3
    tp_vol = tp * df["volume"]
    
    # Kita buat dataframe sementara untuk menghitung akumulasi harian
    temp_df = pd.DataFrame({
        'date': df['open_time'].dt.date,
        'tp_vol': tp_vol,
        'volume': df["volume"]
    })
    
    # Menghitung nilai kumulatif (reset setiap berganti hari/tanggal)
    cum_tp_vol = temp_df.groupby('date')['tp_vol'].cumsum()
    cum_vol = temp_df.groupby('date')['volume'].cumsum()
    
    return cum_tp_vol / cum_vol

# =========================
# REGIME + BIAS
# =========================
def compute_regime_and_bias(prev_mode: str) -> Tuple[str, str, dict]:
    df15 = klines_df(SYMBOL, TF_REGIME, limit=max(EMA_TREND_LEN + 60, 320))
    df15["ema200"] = ema(df15["close"], EMA_TREND_LEN)
    df15["chop"] = chop(df15, CHOP_LEN)

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df15.iloc[idx]
    price = float(last["close"])
    e200 = float(last["ema200"])
    chop_val = float(last["chop"])

    dist_pct = 0.0
    if e200 > 0:
        dist_pct = (price - e200) / e200

    bias = "NONE"
    if abs(dist_pct) >= TREND_DEADBAND_PCT:
        bias = "LONG" if price > e200 else "SHORT"

    mode = prev_mode if prev_mode in ("TREND", "RANGE") else "RANGE"
    
    # Penentuan Mode menggunakan CHOP
    if chop_val <= CHOP_TREND_ON:
        mode = "TREND"
    elif chop_val >= CHOP_RANGE_ON:
        mode = "RANGE"

    dbg = {
        "price": price,
        "chop15": chop_val,
        "ema200_15m": e200,
        "dist_ema200_pct": dist_pct,
    }
    return mode, bias, dbg


# =========================
# SIGNALS
# =========================
def compute_entry_indicators_5m() -> Tuple[pd.DataFrame, dict]:
    df5 = klines_df(SYMBOL, TF_ENTRY, limit=260)

    df5["ema_fast"] = ema(df5["close"], EMA_FAST)
    df5["ema_slow"] = ema(df5["close"], EMA_SLOW)
    df5["rsi"] = rsi(df5["close"], RSI_LEN)
    df5["atr"] = atr(df5, 14)
    df5["don_hi"] = df5["high"].rolling(DONCHIAN_LEN).max().shift(1)
    df5["don_lo"] = df5["low"].rolling(DONCHIAN_LEN).min().shift(1)
    
    # --- TAMBAHKAN BARIS INI ---
    df5["vwap"] = vwap(df5)
    # ---------------------------

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    dbg = {
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
        "rsi5": float(last["rsi"]),
        "atr5": float(last["atr"]),
        "don_hi": float(last["don_hi"]) if pd.notna(last["don_hi"]) else None,
        "don_lo": float(last["don_lo"]) if pd.notna(last["don_lo"]) else None,
        # --- TAMBAHKAN BARIS INI JUGA ---
        "vwap": float(last["vwap"]),
        # --------------------------------
    }
    return df5, dbg

def signal_trend_mode(df5: pd.DataFrame, bias: str) -> Tuple[bool, Optional[str], dict]:
    if bias not in ("LONG", "SHORT"):
        return False, None, {"reason": "bias_none_or_deadband"}

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    ema_fast_v = float(last["ema_fast"])
    ema_slow_v = float(last["ema_slow"])
    rsiv = float(last["rsi"])
    atrv = float(last["atr"])
    vwap_v = float(last["vwap"])     # <--- Tambahkan ini
    close_price = float(last["close"]) # <--- Tambahkan ini agar lebih rapi

    if atrv <= 0:
        return False, None, {"reason": "atr_bad"}

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])

    # OPTIMASI PULLBACK: Cek apakah 3 candle terakhir ada yang menyentuh EMA 20
    lookback = 3
    if USE_CLOSED_CANDLE_ONLY:
        window = df5.iloc[-(lookback+1):-1]
    else:
        window = df5.iloc[-lookback:]

    pullback_touch_long = any(float(row["low"]) <= float(row["ema_fast"]) for _, row in window.iterrows())
    pullback_touch_short = any(float(row["high"]) >= float(row["ema_fast"]) for _, row in window.iterrows())

    dbg = {"touch": False, "rejection": False, "confirm": False}

    if bias == "LONG":
        # --- TAMBAHKAN FILTER VWAP INI ---
        if close_price <= vwap_v:
            return False, None, {"reason": "price_below_vwap", **dbg}
        # ---------------------------------
        
        if ema_fast_v <= ema_slow_v:
            return False, None, {"reason": "ema_not_aligned_long", **dbg}
        if rsiv < RSI_TREND_LONG_MIN:
            return False, None, {"reason": "rsi_low_trend_long", **dbg}
        if not pullback_touch_long:
            return False, None, {"reason": "no_pullback_touch", **dbg}
        if (not candle_bull) or (float(last["close"]) < ema_fast_v):
            dbg["touch"] = True
            return False, None, {"reason": "no_bull_confirm", **dbg}

        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return True, "BUY", {"reason": "trend_entry", **dbg}


    # ====================================================
    # MULAI BLOK LOGIKA SHORT (SELL)
    # ====================================================

    # --- TAMBAHKAN FILTER VWAP INI UNTUK SHORT ---
    if close_price >= vwap_v:
        return False, None, {"reason": "price_above_vwap", **dbg}
    # ---------------------------------------------

    if ema_fast_v >= ema_slow_v:
        return False, None, {"reason": "ema_not_aligned_short", **dbg}
        
    if rsiv > RSI_TREND_SHORT_MAX:
        return False, None, {"reason": "rsi_high_trend_short", **dbg}
        
    if not pullback_touch_short:
        return False, None, {"reason": "no_pullback_touch", **dbg}
        
    if (not candle_bear) or (float(last["close"]) > ema_fast_v):
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

    touch_low = float(last["low"]) <= don_lo
    touch_high = float(last["high"]) >= don_hi

    rej_long = touch_low and (float(last["close"]) > don_lo) and candle_bull
    rej_short = touch_high and (float(last["close"]) < don_hi) and candle_bear

    dbg = {"touch": False, "rejection": False, "confirm": False}

    if rej_long and rsiv >= RSI_RANGE_LONG_MIN:
        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return True, "BUY", {"reason": "range_long_reject", **dbg}

    if rej_short and rsiv <= RSI_RANGE_SHORT_MAX:
        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return True, "SELL", {"reason": "range_short_reject", **dbg}

    if touch_low and not rej_long:
        dbg.update({"touch": True})
        return False, None, {"reason": "range_touch_low_no_reject", **dbg}
        
    if touch_high and not rej_short:
        dbg.update({"touch": True})
        return False, None, {"reason": "range_touch_high_no_reject", **dbg}

    return False, None, {"reason": "range_no_touch", **dbg}


# =========================
# RISK & ORDER ENGINE
# =========================
def calc_sl_dist(price: float, atr_val: float, mode: str) -> float:
    if mode == "TREND":
        raw_sl_dist = atr_val * TREND_SL_ATR_MULT
        sl_min = price * TREND_SL_MIN_PCT
        sl_max = price * TREND_SL_MAX_PCT
    else:
        raw_sl_dist = atr_val * RANGE_SL_ATR_MULT
        sl_min = price * RANGE_SL_MIN_PCT
        sl_max = price * RANGE_SL_MAX_PCT

    return max(sl_min, min(raw_sl_dist, sl_max))

def _recent_structure_window(df5: pd.DataFrame) -> pd.DataFrame:
    if df5 is None or df5.empty:
        return pd.DataFrame()

    if USE_CLOSED_CANDLE_ONLY:
        start = max(0, len(df5) - (SWING_LOOKBACK_BARS + 1))
        end = len(df5) - 1
        return df5.iloc[start:end].copy()

    return df5.iloc[max(0, len(df5) - SWING_LOOKBACK_BARS):].copy()

def get_structure_stop_price(side: str, df5: pd.DataFrame, atr_val: float, mode: str) -> Optional[float]:
    if (not USE_STRUCTURE_SL) or df5 is None or atr_val <= 0:
        return None

    recent = _recent_structure_window(df5)
    if recent.empty:
        return None

    buffer = atr_val * (TREND_SWING_BUFFER_ATR_MULT if mode == "TREND" else RANGE_SWING_BUFFER_ATR_MULT)

    if side == "BUY":
        return float(recent["low"].min()) - buffer
    if side == "SELL":
        return float(recent["high"].max()) + buffer
    return None

def calc_effective_sl_dist(price: float, atr_val: float, mode: str, side: Optional[str] = None, structure_stop_price: Optional[float] = None) -> float:
    sl_dist = calc_sl_dist(price, atr_val, mode)

    if (not USE_STRUCTURE_SL) or side not in ("BUY", "SELL") or structure_stop_price is None:
        return sl_dist

    if side == "BUY" and structure_stop_price < price:
        structure_dist = price - structure_stop_price
    elif side == "SELL" and structure_stop_price > price:
        structure_dist = structure_stop_price - price
    else:
        structure_dist = 0.0

    if structure_dist <= 0:
        return sl_dist

    hard_max_pct = TREND_STRUCTURE_SL_HARD_MAX_PCT if mode == "TREND" else RANGE_STRUCTURE_SL_HARD_MAX_PCT
    structure_dist = min(structure_dist, price * hard_max_pct)
    return max(sl_dist, structure_dist)

def calc_qty_from_risk(equity: float, price: float, sl_dist: float) -> Tuple[float, float, float]:
    target_risk_usd = equity * RISK_PCT
    if sl_dist <= 0:
        return 0.0, 0.0, 0.0

    qty = target_risk_usd / sl_dist

    max_notional = equity * LEVERAGE * MAX_NOTIONAL_FRACTION_OF_EQUITY
    approx_notional = qty * price
    if max_notional > 0 and approx_notional > max_notional:
        qty = max_notional / price
        approx_notional = qty * price

    actual_risk_usd = qty * sl_dist
    return qty, actual_risk_usd, approx_notional

def _cancel_tp_limit_orders():
    """
    Cancel TP LIMIT reduceOnly saja.
    Return True kalau ada order yang dibatalkan.
    """
    canceled_any = False
    try:
        open_orders = call_with_retry(
            client.futures_get_open_orders,
            symbol=SYMBOL,
            recvWindow=RECV_WINDOW
        )

        for o in open_orders or []:
            if o.get("symbol") != SYMBOL:
                continue
            if o.get("reduceOnly") not in (True, "true", "TRUE"):
                continue
            if o.get("type") != "LIMIT":
                continue

            call_with_retry(
                client.futures_cancel_order,
                symbol=SYMBOL,
                orderId=o["orderId"],
                recvWindow=RECV_WINDOW
            )
            canceled_any = True

    except Exception as e:
        print("WARN cancel tp limit orders:", e)

    return canceled_any

def manage_hybrid_tp_exit(st: dict, now: datetime, current_mark: Optional[float] = None) -> bool:
    if not USE_HYBRID_TP_EXIT:
        return False
    

    tp_price = float(st.get("tp_price", 0.0) or 0.0)
    entry_price = float(st.get("entry_price", 0.0) or 0.0)
    qty_q = float(st.get("qty_q", 0.0) or 0.0)
    side = st.get("entry_order_side", "")
    atr_at_entry = float(st.get("atr_at_entry", 0.0) or 0.0)

    if tp_price <= 0 or entry_price <= 0 or qty_q <= 0 or side not in ("BUY", "SELL"):
        return False

    last_manage_ts = float(st.get("last_tp_manage_ts", 0.0) or 0.0)
    if (time.time() - last_manage_ts) < TP_FALLBACK_COOLDOWN_SECONDS:
        return False

    last_mark_check_ts = float(st.get("last_tp_mark_check_ts", 0.0) or 0.0)
    if current_mark is None:
        if (time.time() - last_mark_check_ts) < TP_MANAGE_MARK_CHECK_COOLDOWN_SECONDS:
            return False
        current_mark = get_mark_price()
        st["last_tp_mark_check_ts"] = time.time()

    buffer_abs = max(
        current_mark * TP_FALLBACK_MIN_PCT,
        min(
            current_mark * TP_FALLBACK_MAX_PCT,
            atr_at_entry * TP_FALLBACK_BUFFER_ATR_MULT if atr_at_entry > 0 else 0.0
        )
    )
    

    if side == "BUY":
        should_fallback = current_mark >= (tp_price - buffer_abs)
        close_side = "SELL"
    else:
        should_fallback = current_mark <= (tp_price + buffer_abs)
        close_side = "BUY"

    if not should_fallback:
        return False

    filters = _get_symbol_filters(SYMBOL)
    step = float(filters["LOT_SIZE"]["stepSize"])
    qty_close = _quantize_step(qty_q, step)
    if qty_close <= 0:
        return False

    _cancel_tp_limit_orders()

    call_with_retry(
        client.futures_create_order,
        symbol=SYMBOL,
        side=close_side,
        type="MARKET",
        quantity=qty_close,
        reduceOnly=True,
        recvWindow=RECV_WINDOW
    )

    st["last_tp_manage_ts"] = time.time()

    send_telegram_throttled(
        "tp_fallback_exit",
        f"🎯 TP fallback market exit {SYMBOL}\n"
        f"Side: {'LONG' if side == 'BUY' else 'SHORT'}\n"
        f"Mark: {round(current_mark, 6)} | Target: {round(tp_price, 6)}\n"
        f"Buffer~: {round(buffer_abs, 6)}",
        min_seconds=30,
    )
    return True

def place_order_with_actual_bracket(side: str, qty_q: float, atr_val: float, mode: str, mark_price: float, structure_stop_price: Optional[float] = None):
    filters = _get_symbol_filters(SYMBOL)
    tick = float(filters["PRICE_FILTER"]["tickSize"])
    step = float(filters["LOT_SIZE"]["stepSize"])

    def _has_valid_order_ref(resp):
        return bool(resp) and (("orderId" in resp) or ("algoId" in resp))

    def _safe_get_mark_price(fallback_price: float) -> float:
        try:
            mp = call_with_retry(
                client.futures_mark_price,
                symbol=SYMBOL,
                recvWindow=RECV_WINDOW
            )
            return float(mp.get("markPrice", fallback_price))
        except Exception as e:
            print("WARN get fresh mark price failed:", e)
            return float(fallback_price)

    def _sanitize_sl_price(side_: str, sl_raw: float, current_mark: float):
        min_gap_ticks = 3
        gap = tick * min_gap_ticks
        sl_adj = _round_tick(sl_raw, tick)

        if side_ == "BUY":
            max_sl = _round_tick(current_mark - gap, tick)
            if sl_adj >= current_mark:
                sl_adj = max_sl
        else:
            min_sl = _round_tick(current_mark + gap, tick)
            if sl_adj <= current_mark:
                sl_adj = min_sl
        return sl_adj

    cancel_all_open_orders()

    entry_resp = call_with_retry(
        client.futures_create_order,
        symbol=SYMBOL,
        side=side,
        type="MARKET",
        quantity=qty_q,
        recvWindow=RECV_WINDOW
    )

    actual_entry = 0.0
    actual_pos_amt = 0.0

    for _ in range(12):
        time.sleep(0.5)
        try:
            pos = call_with_retry(
                client.futures_position_information,
                symbol=SYMBOL,
                recvWindow=RECV_WINDOW
            )
            for p in pos or []:
                if p.get("symbol") == SYMBOL:
                    pos_amt = float(p.get("positionAmt", 0.0))
                    if abs(pos_amt) > 0:
                        actual_pos_amt = abs(pos_amt)
                        actual_entry = float(p.get("entryPrice", 0.0))
                        break
        except Exception as e:
            print("WARN read position after entry:", e)

        if actual_entry > 0 and actual_pos_amt > 0:
            break

    if actual_entry <= 0.0:
        actual_entry = float(mark_price)

    if actual_pos_amt <= 0.0:
        actual_pos_amt = qty_q

    sl_dist = calc_effective_sl_dist(actual_entry, atr_val, mode, side=side, structure_stop_price=structure_stop_price)
    rr = TREND_RR if mode == "TREND" else RANGE_RR

    if side == "BUY":
        sl_price = actual_entry - sl_dist
        tp_price = actual_entry + (sl_dist * rr)
        op_side = "SELL"
        tp_limit_price = _round_tick(tp_price - (tick * TP_LIMIT_MAKER_OFFSET_TICKS), tick)
    else:
        sl_price = actual_entry + sl_dist
        tp_price = actual_entry - (sl_dist * rr)
        op_side = "BUY"
        tp_limit_price = _round_tick(tp_price + (tick * TP_LIMIT_MAKER_OFFSET_TICKS), tick)

    current_mark = _safe_get_mark_price(actual_entry)
    sl_q = _sanitize_sl_price(side, sl_price, current_mark)
    actual_pos_amt_q = _quantize_step(actual_pos_amt, step)

    if actual_pos_amt_q <= 0:
        raise RuntimeError(f"Qty final tidak valid: {actual_pos_amt_q}")

    print(
        f"BRACKET DEBUG | side={side} | entry={actual_entry:.8f} | mark={current_mark:.8f} | "
        f"sl_raw={sl_price:.8f} | tp_raw={tp_price:.8f} | sl={sl_q:.8f} | tp_limit={tp_limit_price:.8f} | "
        f"qty={actual_pos_amt_q}"
    )

    try:
        # 1. Pasang Stop Loss
        sl_resp = call_with_retry(
            client.futures_create_order,
            symbol=SYMBOL,
            side=op_side,
            type="STOP_MARKET",
            stopPrice=sl_q,
            quantity=actual_pos_amt_q,
            reduceOnly=True,
            workingType="MARK_PRICE",
            priceProtect=True,
            recvWindow=RECV_WINDOW
        )

        # 2. Pasang Take Profit dengan penanganan GTX Fallback
        try:
            tp_resp = call_with_retry(
                client.futures_create_order,
                symbol=SYMBOL,
                side=op_side,
                type="LIMIT",
                price=tp_limit_price,
                quantity=actual_pos_amt_q,
                reduceOnly=True,
                timeInForce="GTX", # Memaksa jadi Maker
                recvWindow=RECV_WINDOW
            )
        except Exception as e_tp:
            msg = str(e_tp)
            if "-2010" in msg or "immediately match" in msg:
                print("GTX rejected. Fallback ke LIMIT reduceOnly biasa...")
                tp_resp = call_with_retry(
                    client.futures_create_order,
                    symbol=SYMBOL,
                    side=op_side,
                    type="LIMIT",
                    price=tp_limit_price,
                    quantity=actual_pos_amt_q,
                    reduceOnly=True,
                    timeInForce="GTC",
                    recvWindow=RECV_WINDOW
                )
            else:
                raise e_tp
            
        if not _has_valid_order_ref(sl_resp):
            raise RuntimeError(f"SL order gagal / id tidak ada | resp={sl_resp}")
        if not _has_valid_order_ref(tp_resp):
            raise RuntimeError(f"TP order gagal / id tidak ada | resp={tp_resp}")

    except Exception as e:
        print(f"CRITICAL ERROR: Failed Bracket. Emergency close. Error: {e}")

        try:
            cancel_all_open_orders()
        except Exception as cancel_err:
            print("WARN cancel after bracket failure:", cancel_err)

        try:
            current_pos_amt = 0.0
            pos = call_with_retry(
                client.futures_position_information,
                symbol=SYMBOL,
                recvWindow=RECV_WINDOW
            )
            for p in pos or []:
                if p.get("symbol") == SYMBOL:
                    current_pos_amt = float(p.get("positionAmt", 0.0))
                    break

            if abs(current_pos_amt) > 0:
                emergency_side = "SELL" if current_pos_amt > 0 else "BUY"
                qty_close = _quantize_step(abs(current_pos_amt), step)

                if qty_close > 0:
                    call_with_retry(
                        client.futures_create_order,
                        symbol=SYMBOL,
                        side=emergency_side,
                        type="MARKET",
                        quantity=qty_close,
                        reduceOnly=True,
                        recvWindow=RECV_WINDOW
                    )

                send_telegram(
                    f"🚨 EMERGENCY: Gagal pasang SL/TP. Posisi DITUTUP OTOMATIS!\n"
                    f"Err: {e}\n"
                    f"entryId={entry_resp.get('orderId')}\n"
                    f"mark={round(current_mark, 8)} | sl={round(sl_q, 8)} | tp={round(tp_price, 8)}"
                )
            else:
                send_telegram(
                    f"⚠️ Bracket gagal, tapi posisi sudah tidak ada.\n"
                    f"Err: {e}\n"
                    f"entryId={entry_resp.get('orderId')}"
                )

        except Exception as close_err:
            send_telegram(
                f"🛑 FATAL: Bracket gagal dan emergency close gagal!\n"
                f"BracketErr: {repr(e)}\n"
                f"CloseErr: {repr(close_err)}"
            )
            raise RuntimeError(f"Bracket & Close fail | e={e} | c={close_err}")

        raise RuntimeError(f"Bracket failed after entry: {e}")

    return actual_entry, actual_pos_amt_q, sl_q, tp_price, sl_dist


# =========================
# PNL TRACKING
# =========================
def realized_pnl_since(start_ms: int, seen_tran_ids: set):
    try:
        incomes = call_with_retry(
            client.futures_income_history,
            symbol=SYMBOL,
            startTime=start_ms,
            limit=1000,
            recvWindow=RECV_WINDOW
        )

        total = 0.0
        found_any = False
        new_ids = []

        for it in incomes or []:
            tid = it.get("tranId")
            income_type = it.get("incomeType")

            if not tid or tid in seen_tran_ids:
                continue

            if income_type in ("REALIZED_PNL", "COMMISSION"):
                total += float(it.get("income", 0.0))
                new_ids.append(tid)
                found_any = True

        for tid in new_ids:
            seen_tran_ids.add(tid)

        return total, found_any

    except Exception as e:
        print("WARN realized_pnl_since:", e)
        return 0.0, False

def get_closed_trade_pnl_with_retry(start_ms: int, seen_tran_ids: set, retries: int = 5, wait_sec: float = 2.0):
    total_pnl = 0.0
    got_any = False

    for i in range(retries):
        pnl, found = realized_pnl_since(start_ms, seen_tran_ids)
        if found:
            total_pnl += pnl
            got_any = True
            # Jangan langsung break. Beri jeda 1x lagi untuk menunggu data PnL yang mungkin telat
            if i < retries - 1: 
                time.sleep(wait_sec)
                continue
            else:
                break
                
        time.sleep(wait_sec)

    return total_pnl, got_any

# =========================
# MAIN
# =========================
def main():
    set_leverage_safe(SYMBOL, LEVERAGE)

    min_notional = get_min_notional(SYMBOL, fallback=100.0)
    current_equity = get_wallet_balance_quote()

    print("MinNotional:", min_notional)
    mode_label = "TESTNET" if USE_TESTNET else "REAL"
    print(f"{SYMBOL} V6 HYBRID START ({mode_label}) | Lev:{LEVERAGE} | Regime:{TF_REGIME} CHOP{CHOP_LEN} | Entry:{TF_ENTRY}")

    send_telegram_throttled(
        "startup",
        f"🟢 {TG_PREFIX} started\n"
        f"Symbol: {SYMBOL}\nLev: {LEVERAGE}\n"
        f"Equity: {round(current_equity, 4)} {QUOTE_ASSET}\n"
        f"Trend CHOP<={CHOP_TREND_ON}, Range CHOP>={CHOP_RANGE_ON}\n"
        f"Risk: {RISK_PCT*100:.2f}% | DailyStop: {MAX_DAILY_DRAWDOWN_PCT*100:.1f}%\n"
        f"Cooldown: {COOLDOWN_MINUTES}m | LossStreakStop: {LOSS_STREAK_LIMIT}\n"
        f"BracketFailLock: {BRACKET_FAIL_LOCK_LIMIT}\n"
        f"MinNotional: {min_notional}",
        min_seconds=5,
    )

    st = load_state({
        "day_key": datetime.now(timezone.utc).date().isoformat(),
        "trades_today": 0,
        "loss_streak": 0,
        "daily_locked": False,
        "cooldown_until": None,
        "start_equity_today": current_equity,
        "prev_in_position": has_open_position(),
        "last_pnl_check_ms": int(time.time() * 1000) - 60_000,
        "daily_realized_pnl": 0.0,
        "mode": "RANGE",
        "seen_tran_ids": set(),
        "position_open_ms": 0,
        "entry_price": 0.0,
        "sl_dist_actual": 0.0,
        "pos_side": "",
        "qty_q": 0.0,
        "bracket_fail_streak": 0,
        "awaiting_pnl_sync": False,
        "pnl_pending_notified": False,
        "next_pnl_recheck_at": None,
        "tp_price": 0.0,
        "sl_price": 0.0,
        "entry_order_side": "",
        "atr_at_entry": 0.0,
        "last_tp_manage_ts": 0.0,
        "last_tp_mark_check_ts": 0.0,
    })

    def _save_state():
        save_state(st)

    last_time_sync = 0.0
    TIME_SYNC_EVERY_S = 30 * 60

    while True:
        try:
            now = datetime.now(timezone.utc)

            if (time.time() - last_time_sync) >= TIME_SYNC_EVERY_S:
                if sync_time_offset():
                    last_time_sync = time.time()

            cur_day = now.date().isoformat()
            if cur_day != st["day_key"]:
                if st["day_key"]:
                    log_daily_summary(
                        st["day_key"],
                        st["start_equity_today"],
                        st["daily_realized_pnl"],
                        st["trades_today"],
                        st["loss_streak"]
                    )

                st.update({
                    "day_key": cur_day,
                    "trades_today": 0,
                    "loss_streak": 0,
                    "daily_locked": False,
                    "cooldown_until": None,
                    "start_equity_today": get_wallet_balance_quote(),
                    "prev_in_position": has_open_position(),
                    "last_pnl_check_ms": int(time.time() * 1000) - 60_000,
                    "daily_realized_pnl": 0.0,
                    "mode": "RANGE",
                    "position_open_ms": 0,
                    "entry_price": 0.0,
                    "sl_dist_actual": 0.0,
                    "pos_side": "",
                    "qty_q": 0.0,
                    "bracket_fail_streak": 0,
                    "awaiting_pnl_sync": False,
                    "pnl_pending_notified": False,
                    "next_pnl_recheck_at": None,
                    "tp_price": 0.0,
                    "sl_price": 0.0,
                    "entry_order_side": "",
                    "atr_at_entry": 0.0,
                    "last_tp_manage_ts": 0.0,
                    "last_tp_mark_check_ts": 0.0,
                })
                send_telegram_throttled(
                    "new_day",
                    f"🗓 New day {SYMBOL}\nStart equity: {round(st['start_equity_today'], 4)} {QUOTE_ASSET}\nLocks reset.",
                    min_seconds=5,
                )
                _save_state()

            if st["daily_locked"]:
                time.sleep(SLEEP_SECONDS)
                continue

            equity_now = get_wallet_balance_quote()
            pos_amt, unrealized_pnl, pos_entry_price = get_position_snapshot()
            in_pos = abs(pos_amt) > 0.0

            if st["start_equity_today"] <= 0:
                st["start_equity_today"] = equity_now

            total_daily_pnl = st["daily_realized_pnl"] + (unrealized_pnl if in_pos else 0.0)
            daily_dd = (total_daily_pnl / st["start_equity_today"]) if st["start_equity_today"] > 0 else 0.0

            if daily_dd <= -abs(MAX_DAILY_DRAWDOWN_PCT):
                st["daily_locked"] = True
                send_telegram_throttled(
                    "daily_hard_stop",
                    f"🛑 DAILY HARD STOP {SYMBOL}\nDD: {round(daily_dd * 100, 2)}%\n"
                    f"TotalPnLToday: {round(total_daily_pnl, 4)} {QUOTE_ASSET}\nLocked until tomorrow.",
                    min_seconds=30,
                )
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            if int(st["trades_today"]) >= MAX_TRADES_PER_DAY:
                st["daily_locked"] = True
                send_telegram_throttled(
                    "trade_limit",
                    f"⚠️ Trade limit reached {SYMBOL}\ntrades_today: {st['trades_today']}\nLocked until tomorrow.",
                    min_seconds=30,
                )
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            cooldown_until = _iso_to_dt(st.get("cooldown_until"))
            if cooldown_until and now < cooldown_until:
                time.sleep(SLEEP_SECONDS)
                continue

            if in_pos and not st.get("prev_in_position", False):
                st["prev_in_position"] = True
                if not st.get("position_open_ms"):
                    st["position_open_ms"] = int(time.time() * 1000)
                _save_state()
            if st.get("prev_in_position", False) and not in_pos:
                cancel_all_open_orders()
                st["awaiting_pnl_sync"] = True

            if st.get("awaiting_pnl_sync", False):
                next_pnl_recheck_at = _iso_to_dt(st.get("next_pnl_recheck_at"))
                if next_pnl_recheck_at and now < next_pnl_recheck_at:
                    time.sleep(SLEEP_SECONDS)
                    continue

                pnl_start_ms = int(st.get("position_open_ms", 0)) or int(st.get("last_pnl_check_ms", 0))
                pnl, got_any = get_closed_trade_pnl_with_retry(
                    pnl_start_ms,
                    st["seen_tran_ids"],
                    retries=PNL_FETCH_RETRIES_PER_LOOP,
                    wait_sec=PNL_FETCH_WAIT_SEC
                )

                if not got_any:
                    if not st.get("pnl_pending_notified", False):
                        send_telegram(
                            f"⚠️ Posisi {SYMBOL} sudah tertutup, tapi data PnL Binance belum terbaca. "
                            f"Bot akan cek ulang sekitar {PNL_RECHECK_DELAY_MINUTES} menit lagi."
                        )
                        st["pnl_pending_notified"] = True

                    st["next_pnl_recheck_at"] = _dt_to_iso(now + pd.Timedelta(minutes=PNL_RECHECK_DELAY_MINUTES))
                    _save_state()
                    time.sleep(SLEEP_SECONDS)
                    continue

                st["last_pnl_check_ms"] = int(time.time() * 1000)
                st["daily_realized_pnl"] = float(st.get("daily_realized_pnl", 0.0)) + pnl
                st["loss_streak"] = int(st.get("loss_streak", 0)) + 1 if pnl < 0 else 0
                st["prev_in_position"] = False
                st["position_open_ms"] = 0
                st["entry_price"] = 0.0
                st["sl_dist_actual"] = 0.0
                st["pos_side"] = ""
                st["qty_q"] = 0.0
                st["tp_price"] = 0.0
                st["sl_price"] = 0.0
                st["entry_order_side"] = ""
                st["atr_at_entry"] = 0.0
                st["last_tp_manage_ts"] = 0.0
                st["last_tp_mark_check_ts"] = 0.0
                st["awaiting_pnl_sync"] = False
                st["pnl_pending_notified"] = False
                st["next_pnl_recheck_at"] = None

                log_trade_close(now, pnl, st["loss_streak"], st["trades_today"], st["daily_realized_pnl"])

                send_telegram(
                    f"✅ Position closed {SYMBOL}\n"
                    f"RealizedPnL: {round(pnl, 4)} {QUOTE_ASSET}\n"
                    f"LossStreak: {st['loss_streak']}\n"
                    f"Cooldown: {COOLDOWN_MINUTES}m"
                )

                if st["loss_streak"] >= LOSS_STREAK_LIMIT:
                    st["daily_locked"] = True
                    send_telegram(f"🧯 LOSS STREAK STOP {SYMBOL}\nLossStreak: {st['loss_streak']}\nLocked until tomorrow.")
                    _save_state()
                    time.sleep(SLEEP_SECONDS)
                    continue

                st["cooldown_until"] = _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES))
                _save_state()
                time.sleep(SLEEP_SECONDS)
                continue

            if in_pos:
                st["prev_in_position"] = True

                current_mark = None
                last_tp_mark_check_ts = float(st.get("last_tp_mark_check_ts", 0.0) or 0.0)
                if (time.time() - last_tp_mark_check_ts) >= TP_MANAGE_MARK_CHECK_COOLDOWN_SECONDS:
                    current_mark = get_mark_price()
                    st["last_tp_mark_check_ts"] = time.time()

                if manage_hybrid_tp_exit(st, now, current_mark=current_mark):
                    _save_state()
                    time.sleep(IN_POSITION_SLEEP_SECONDS)
                    continue

                time.sleep(IN_POSITION_SLEEP_SECONDS)
                continue

            st["prev_in_position"] = False

            mode_new, bias, dbg15 = compute_regime_and_bias(st.get("mode", "RANGE"))
            st["mode"] = mode_new

            df5, dbg5 = compute_entry_indicators_5m()

            if st["mode"] == "TREND":
                ok, side, sig_dbg = signal_trend_mode(df5, bias)
            elif st["mode"] == "RANGE" and ENABLE_RANGE_MODE:
                ok, side, sig_dbg = signal_range_mode(df5)
            else:
                ok, side, sig_dbg = False, None, {"reason": "range_disabled"}

            dbg = {**dbg15, **dbg5, **sig_dbg, "mode": st["mode"], "bias": bias}
            reason = sig_dbg.get("reason", "-")

            print(
                now,
                "eq:", round(equity_now, 4),
                "| mode:", st["mode"],
                "| bias:", bias,
                "| chop15:", round(float(dbg15.get("chop15", 0.0)), 2),
                "| ok:", ok,
                "|", reason,
            )
            log_loop(now, equity_now, st["mode"], bias, ok, reason, dbg)

            if (not ok) or (side is None):
                time.sleep(SLEEP_SECONDS)
                continue

            price = get_mark_price()
            atr_val = float(dbg.get("atr5", 0.0))
            if atr_val <= 0:
                time.sleep(SLEEP_SECONDS)
                continue
            structure_stop_price = get_structure_stop_price(side, df5, atr_val, st["mode"])
            sl_dist_est = calc_effective_sl_dist(price, atr_val, st["mode"], side=side, structure_stop_price=structure_stop_price)
            qty, risk_usd, _ = calc_qty_from_risk(equity_now, price, sl_dist_est)

            filters = _get_symbol_filters(SYMBOL)
            step = float(filters["LOT_SIZE"]["stepSize"])
            qty_q = _quantize_step(qty, step)
            notional_q = qty_q * price

            if (qty_q <= 0) or (notional_q < min_notional):
                forced_notional = min_notional * 1.02
                qty_q = _quantize_step(forced_notional / price, step)
                notional_q = qty_q * price

                est_forced_risk_pct = ((qty_q * sl_dist_est) / max(equity_now, 1e-9))
                if est_forced_risk_pct > MAX_FORCED_RISK_PCT:
                    send_telegram_throttled(
                        "min_notional_skip",
                        f"⚠️ Skip entry (minNotional / forced risk too high)\n"
                        f"Need >= {min_notional} {QUOTE_ASSET}\n"
                        f"Forced risk~ {round(est_forced_risk_pct * 100, 2)}%\n"
                        f"Qty={qty_q} | Notional~ {round(notional_q, 4)}",
                        min_seconds=120,
                    )
                    time.sleep(SLEEP_SECONDS)
                    continue

            try:
                actual_entry, qty_final, sl_final, tp_final, sl_dist_actual = place_order_with_actual_bracket(
                    side=side,
                    qty_q=qty_q,
                    atr_val=atr_val,
                    mode=st["mode"],
                    mark_price=price,
                    structure_stop_price=structure_stop_price
                )

                st["trades_today"] = int(st.get("trades_today", 0)) + 1
                st["cooldown_until"] = _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES))
                st["entry_price"] = actual_entry
                st["sl_dist_actual"] = sl_dist_actual
                st["pos_side"] = "LONG" if side == "BUY" else "SHORT"
                st["qty_q"] = qty_final
                st["tp_price"] = tp_final
                st["sl_price"] = sl_final
                st["entry_order_side"] = side
                st["atr_at_entry"] = atr_val
                st["last_tp_manage_ts"] = 0.0
                st["last_tp_mark_check_ts"] = 0.0
                st["position_open_ms"] = int(time.time() * 1000)
                st["bracket_fail_streak"] = 0
                st["awaiting_pnl_sync"] = False
                st["pnl_pending_notified"] = False
                st["next_pnl_recheck_at"] = None
                _save_state()

                send_telegram(
                    f"🚀 ENTRY {SYMBOL} {('LONG' if side == 'BUY' else 'SHORT')}\n"
                    f"Mode: {st['mode']} | Bias: {bias}\n"
                    f"Qty: {qty_final}\n"
                    f"Entry: {round(actual_entry, 6)}\n"
                    f"SL: {round(sl_final, 6)} | TP: {round(tp_final, 6)}\n"
                    f"Risk~: {round(qty_final * sl_dist_actual, 4)} {QUOTE_ASSET}\n"
                    f"Notional~: {round(qty_final * actual_entry, 4)}\n"
                    f"CHOP15: {round(float(dbg.get('chop15', 0.0)), 2)} | "
                    f"RSI5: {round(float(dbg.get('rsi5', 0.0)), 2)} | "
                    f"ATR5: {round(float(dbg.get('atr5', 0.0)), 6)}\n"
                    f"TradesToday: {st['trades_today']}"
                )

                log_trade_open(now, side, actual_entry, qty_final, sl_final, tp_final, risk_usd, qty_final * actual_entry, dbg)

            except Exception as e:
                st["bracket_fail_streak"] = int(st.get("bracket_fail_streak", 0)) + 1
                st["cooldown_until"] = _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES))

                if st["bracket_fail_streak"] >= BRACKET_FAIL_LOCK_LIMIT:
                    st["daily_locked"] = True
                    send_telegram(
                        f"🛑 BOT LOCKED {SYMBOL}\n"
                        f"Bracket gagal {st['bracket_fail_streak']}x berturut-turut.\n"
                        f"Stop trading sampai dicek manual / sampai besok."
                    )

                _save_state()
                raise RuntimeError(f"Order execution failed: {e}")

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            msg = str(e)
            print("ERROR:", repr(e))
            traceback.print_exc()

            if ("Timestamp for this request" in msg) or ("recvWindow" in msg) or ("-1021" in msg):
                sync_time_offset()
                time.sleep(2)
                continue

            send_telegram_throttled(
                "runtime_error",
                f"❌ ERROR {SYMBOL}\n{repr(e)}",
                min_seconds=60,
            )
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()
    