#!/usr/bin/env python3
# bot-ethusdc-v9.8.0-improved.py
# ETHUSDC USD-M Futures - HYBRID Trend & Range
# V9.8.0: Small Account Optimized, BE Improved, Trailing SL, EMA200 Slope Filter,
#         Dynamic Leverage, 15m Close Guard, RSI Range Tightened.

import os
import time
import json
import csv
import random
import traceback
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import requests
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from requests.exceptions import ReadTimeout, ConnectionError

# =========================
# TESTNET FORCE ENTRY MODE
# =========================
FORCE_TEST_ENTRY = False
FORCE_TEST_SIDE = "BUY"

# =========================
# CONFIGURASI BOT & MODAL
# =========================
SYMBOL = "ETHUSDC"
LEVERAGE = 10
TF_REGIME = "15m"
TF_ENTRY = "5m"
TAKER_FEE_PCT = 0.0005

# Parameter Small Account
RISK_PCT = 0.012
MAX_FORCED_RISK_PCT = 0.020          # [V9.8] Diturunkan dari 3.5% -> 2.0%
MAX_DAILY_DRAWDOWN_PCT = 0.06
MAX_MARGIN_FRACTION = 0.50           # [V9.8] Diturunkan dari 0.75 -> 0.50

# =========================
# VOLATILITY ADAPTIVE RISK
# =========================
LOW_VOL_RISK_MULT = 0.85
NORMAL_VOL_RISK_MULT = 1.00
HIGH_VOL_RISK_MULT = 0.60

# Dynamic Leverage saat HIGH_VOL
HIGH_VOL_LEVERAGE = 7                # [V9.8] Leverage otomatis turun saat HIGH_VOL

# TRADE MANAGEMENT
ENABLE_BREAK_EVEN = True             
BE_ACTIVATION_RR = 0.8               # [V9.8] Dinaikkan ke 0.8R
BE_BUFFER_PCT = 0.0006               

# Trailing SL setelah profit tinggi
ENABLE_TRAILING_SL = True            # [V9.8] Fitur baru trailing SL
TRAIL_ACTIVATION_RR = 1.5            # [V9.8] Aktifkan trailing setelah 1.5R profit
TRAIL_LOCK_RR = 1.0                  # [V9.8] Geser SL ke titik lock 1.0R

# VOLATILITY REGIME FILTER
VOL_ATR_LEN_15M = 14
VOL_LOOKBACK_15M = 64
VOL_LOW_MULT = 0.75
VOL_HIGH_MULT = 1.60
HIGH_VOL_WIDEN_SL_MULT = 1.15
HIGH_VOL_TP_MULT = 0.90

# 15M Candle Close Guard
CANDLE_15M_GUARD_MINUTES = 13        # [V9.8] Blok entry >= menit ke-13

# SNIPER PARAMETERS
ADX_LEN = 14
ADX_TREND_ON = 22.0
ADX_RANGE_ON = 18.0
EMA_TREND_LEN = 200
TREND_DEADBAND_PCT = 0.0008

# EMA200 Slope Filter
EMA200_SLOPE_LOOKBACK = 5            # [V9.8] Slope dihitung dari 5 candle sebelumnya

VOL_SPIKE_MULT = 1.20
VOL_PULLBACK_MULT = 0.50

# TREND
EMA_FAST = 9
EMA_SLOW = 21
RSI_LEN = 14
RSI_TREND_LONG_MIN = 48
RSI_TREND_SHORT_MAX = 52
TREND_RR = 1.7
TREND_SL_ATR_MULT = 0.75
TREND_SL_MIN_PCT = 0.0025
TREND_SL_MAX_PCT = 0.0100
PULLBACK_ATR_TOL = 0.25
TREND_CONFIRM_ATR_TOL = 0.10

# RANGE
DONCHIAN_LEN = 20
RSI_RANGE_LONG_MIN = 35              # [V9.8] Diperketat ke 35
RSI_RANGE_SHORT_MAX = 65             # [V9.8] Diperketat ke 65
RANGE_RR = 1.3
RANGE_SL_ATR_MULT = 0.65
RANGE_SL_MIN_PCT = 0.0020
RANGE_SL_MAX_PCT = 0.0080
VOL_OK_RATIO_RANGE = 0.50
DONCHIAN_ATR_TOL = 0.20

MAX_TRADES_PER_DAY = 12
COOLDOWN_MINUTES = 6
LOSS_STREAK_LIMIT = 5

SLEEP_SLOW = 10
SLEEP_FAST = 3
RECV_WINDOW = 10000

TG_PREFIX = "ETHUSDC V9.8.0"
STATE_FILE = Path(".state_ethusdc_v98.json")
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOOP_LOG = LOG_DIR / "loop_eth_v98.csv"

# =========================
# FUNGSI DASAR & STATE
# =========================
def call_with_retry(fn, retries=5, base_sleep=1.0, kwargs=None):
    if kwargs is None: kwargs = {}
    last_err = None
    for i in range(retries):
        try:
            return fn(**kwargs)
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i) + random.uniform(0, 0.5))
    raise last_err

def _dt_to_iso(dt):
    return dt.isoformat() if dt else None

def load_state(defaults):
    try:
        if STATE_FILE.exists():
            defaults.update(json.loads(STATE_FILE.read_text()))
    except:
        pass
    if "seen_tran_ids" in defaults and isinstance(defaults["seen_tran_ids"], list):
        defaults["seen_tran_ids"] = set(defaults["seen_tran_ids"])
    return defaults

def save_state(state):
    try:
        st_copy = state.copy()
        if "seen_tran_ids" in st_copy:
            st_copy["seen_tran_ids"] = list(st_copy["seen_tran_ids"])
        temp_file = STATE_FILE.with_suffix('.tmp')
        temp_file.write_text(json.dumps(st_copy, indent=2, sort_keys=True))
        temp_file.replace(STATE_FILE)
    except Exception as e:
        print(f"Gagal save state: {e}")

def log_loop(now, equity, mode, bias, active_bias, reason, dbg):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not LOOP_LOG.exists()
    row = {"ts": now.isoformat(), "equity": round(equity, 2), "mode": mode, "bias": bias, "active_bias": active_bias, "reason": reason}
    for k, v in dbg.items():
        row[k] = round(v, 6) if isinstance(v, float) else v
    header = ["ts","equity","mode","bias","active_bias","reason","price","adx15","ema9","ema21","rsi5","atr5",
              "vol","vol_sma","don_hi","don_lo","vol_regime","atr_pct15","atrp_low","atrp_high","ema200_slope"]
    with LOOP_LOG.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if new_file: w.writeheader()
        w.writerow(row)

# =========================
# SETUP BINANCE
# =========================
load_dotenv(dotenv_path=".env")
client = Client(os.getenv("BINANCE_TESTNET_API_KEY"), os.getenv("BINANCE_TESTNET_API_SECRET"), testnet=True)
client.REQUEST_TIMEOUT = 60

def sync_time_offset():
    try:
        client.timestamp_offset = call_with_retry(client.futures_time)['serverTime'] - int(time.time() * 1000)
        return True
    except:
        return False
sync_time_offset()

def send_telegram(msg: str):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg}, timeout=8)
        except:
            pass

# =========================
# HELPERS API BINANCE
# =========================
_exchange_cache = {}
def _get_symbol_filters(symbol):
    if not _exchange_cache.get("info"):
        _exchange_cache["info"] = call_with_retry(client.futures_exchange_info)
    for s in _exchange_cache["info"]["symbols"]:
        if s["symbol"] == symbol:
            return {f["filterType"]: f for f in s["filters"]}
    raise RuntimeError("Symbol not found")

def get_min_notional(symbol):
    try:
        f = _get_symbol_filters(symbol)
        if "MIN_NOTIONAL" in f:
            return float(f["MIN_NOTIONAL"].get("notional", 100))
    except:
        pass
    return 100.0

def _round_tick(value, tick):
    if tick == 0: return float(value)
    return float((Decimal(str(value)) / Decimal(str(tick))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(tick)))

def _quantize_step(value, step):
    if step == 0: return float(value)
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))

def get_wallet_balance_usdc():
    try:
        balances = call_with_retry(client.futures_account_balance, kwargs={"recvWindow": RECV_WINDOW})
        return next((float(b.get("availableBalance", 0.0)) for b in balances if b.get("asset") == "USDC"), 0.0)
    except:
        return 0.0

def get_position_amt():
    try:
        pos = call_with_retry(client.futures_position_information, kwargs={"symbol": SYMBOL, "recvWindow": RECV_WINDOW})
        return float(pos[0].get("positionAmt", 0.0)) if pos else 0.0
    except:
        return 0.0

def get_unrealized_pnl():
    try:
        pos = call_with_retry(client.futures_position_information, kwargs={"symbol": SYMBOL, "recvWindow": RECV_WINDOW})
        return float(pos[0].get("unRealizedProfit", 0.0)) if pos else 0.0
    except:
        return 0.0

def get_mark_price():
    return float(call_with_retry(client.futures_mark_price, kwargs={"symbol": SYMBOL})["markPrice"])

def cancel_all_open_orders():
    try:
        call_with_retry(client.futures_cancel_all_open_orders, kwargs={"symbol": SYMBOL, "recvWindow": RECV_WINDOW})
    except:
        pass

def klines_df(symbol, interval, limit):
    raw = call_with_retry(client.futures_klines, kwargs={"symbol": symbol, "interval": interval, "limit": limit})
    df = pd.DataFrame(raw, columns=["open_time","open","high","low","close","volume","close_time","qav","nt","tbb","tbq","ign"])
    for c in ["open","high","low","close","volume"]:
        df[c] = df[c].astype(float)
    return df

# =========================
# [V9.8] DYNAMIC LEVERAGE
# =========================
def set_leverage_for_regime(vol_regime: str):
    target_lev = HIGH_VOL_LEVERAGE if vol_regime == "HIGH_VOL" else LEVERAGE
    try:
        call_with_retry(client.futures_change_leverage, kwargs={"symbol": SYMBOL, "leverage": target_lev, "recvWindow": RECV_WINDOW})
        return target_lev
    except Exception as e:
        print(f"Gagal set leverage {target_lev}x: {e}")
        return LEVERAGE

# =========================
# INDIKATOR & SINYAL
# =========================
def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def rsi(series, length=14):
    delta = series.diff()
    rs = (delta.clip(lower=0).ewm(alpha=1/length, adjust=False).mean()) / \
         (-delta.clip(upper=0).ewm(alpha=1/length, adjust=False).mean().replace(0, 1e-12))
    return 100 - (100 / (1 + rs))

def atr(df, length=14):
    tr = pd.concat([(df["high"] - df["low"]),
                    (df["high"] - df["close"].shift(1)).abs(),
                    (df["low"] - df["close"].shift(1)).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def adx(df, length=14):
    up, down = df["high"].diff(), -df["low"].diff()
    plus_dm, minus_dm = pd.Series(0.0, index=df.index), pd.Series(0.0, index=df.index)
    plus_dm[(up > down) & (up > 0)] = up[(up > down) & (up > 0)]
    minus_dm[(down > up) & (down > 0)] = down[(down > up) & (down > 0)]
    tr = pd.concat([(df["high"] - df["low"]),
                    (df["high"] - df["close"].shift(1)).abs(),
                    (df["low"] - df["close"].shift(1)).abs()], axis=1).max(axis=1)
    atr_s = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/length, adjust=False).mean() / atr_s.replace(0, 1e-12))
    minus_di = 100 * (minus_dm.ewm(alpha=1/length, adjust=False).mean() / atr_s.replace(0, 1e-12))
    return (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-12)).ewm(alpha=1/length, adjust=False).mean()

def compute_regime_and_bias(prev_mode):
    df15 = klines_df(SYMBOL, TF_REGIME, limit=max(220, VOL_LOOKBACK_15M + 20))
    df15["ema200"] = ema(df15["close"], EMA_TREND_LEN)
    df15["adx"] = adx(df15, ADX_LEN)

    last = df15.iloc[-2] if len(df15) > 1 else df15.iloc[-1]
    price, e200, ax = float(last["close"]), float(last["ema200"]), float(last["adx"])

    # [V9.8] EMA200 Slope Filter
    ema200_slope = 0.0
    if len(df15) > EMA200_SLOPE_LOOKBACK + 2:
        ema200_now = float(df15["ema200"].iloc[-2])
        ema200_past = float(df15["ema200"].iloc[-2 - EMA200_SLOPE_LOOKBACK])
        ema200_slope = ema200_now - ema200_past

    dist_pct = (price - e200) / e200 if e200 > 0 else 0.0

    if dist_pct >= TREND_DEADBAND_PCT and ema200_slope > 0:
        bias = "LONG"
    elif dist_pct <= -TREND_DEADBAND_PCT and ema200_slope < 0:
        bias = "SHORT"
    elif dist_pct >= TREND_DEADBAND_PCT or dist_pct <= -TREND_DEADBAND_PCT:
        bias = "WEAK" 
    else:
        bias = "NONE"

    mode = "TREND" if ax >= ADX_TREND_ON else "RANGE" if ax <= ADX_RANGE_ON else (prev_mode if prev_mode in ("TREND", "RANGE") else "RANGE")
    return mode, bias, ax, df15, ema200_slope

def get_vol_regime_15m(df15):
    df15["atr_15"] = atr(df15, VOL_ATR_LEN_15M)
    last = df15.iloc[-2] if len(df15) > 1 else df15.iloc[-1]
    
    price = float(last["close"])
    a = float(last["atr_15"]) if pd.notna(last["atr_15"]) else 0.0
    if price <= 0 or a <= 0: return "UNKNOWN", 0.0, 0.0, 0.0

    atr_pct = a / price
    hist_atr_pct = (df15["atr_15"].iloc[-VOL_LOOKBACK_15M:] / df15["close"].iloc[-VOL_LOOKBACK_15M:]).dropna()
    med = float(hist_atr_pct.median()) if len(hist_atr_pct) > 0 else atr_pct

    low_th = med * VOL_LOW_MULT
    high_th = med * VOL_HIGH_MULT

    if atr_pct < low_th: regime = "LOW_VOL"
    elif atr_pct > high_th: regime = "HIGH_VOL"
    else: regime = "NORMAL"

    return regime, atr_pct, low_th, high_th

def get_risk_pct_by_vol_regime(vol_regime: str) -> float:
    if vol_regime == "LOW_VOL": return RISK_PCT * LOW_VOL_RISK_MULT
    elif vol_regime == "HIGH_VOL": return RISK_PCT * HIGH_VOL_RISK_MULT
    return RISK_PCT * NORMAL_VOL_RISK_MULT

def compute_entry_indicators_5m():
    df5 = klines_df(SYMBOL, TF_ENTRY, limit=120)
    df5["ema9"], df5["ema21"] = ema(df5["close"], EMA_FAST), ema(df5["close"], EMA_SLOW)
    df5["rsi"], df5["atr"] = rsi(df5["close"], RSI_LEN), atr(df5, 14)
    df5["vol_sma"] = df5["volume"].rolling(20).mean()
    df5["don_hi"] = df5["high"].rolling(DONCHIAN_LEN).max().shift(1)
    df5["don_lo"] = df5["low"].rolling(DONCHIAN_LEN).min().shift(1)
    return df5

def signal_trend_mode(df5, bias, adx15):
    last = df5.iloc[-2] if len(df5) >= 3 else df5.iloc[-1]
    prev = df5.iloc[-3] if len(df5) >= 3 else last

    if not pd.notna(last["atr"]) or float(last["atr"]) <= 0: return None, "atr_bad", bias
    vs = float(last["vol_sma"]) if pd.notna(last["vol_sma"]) else 0.0
    v = float(last["volume"])
    if vs <= 0: return None, "vol_sma_bad", bias

    # [V9.8] Bias WEAK check
    active_bias = bias
    if bias == "NONE":
        if float(last["ema9"]) > float(last["ema21"]): active_bias = "LONG"
        elif float(last["ema9"]) < float(last["ema21"]): active_bias = "SHORT"
    elif bias == "WEAK":
        return None, "weak_bias_no_entry", bias

    if active_bias not in ("LONG", "SHORT"): return None, "no_clear_bias", active_bias

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])
    atr_tol = PULLBACK_ATR_TOL * float(last["atr"])
    confirm_tol = TREND_CONFIRM_ATR_TOL * float(last["atr"])

    if active_bias == "LONG":
        if float(last["ema9"]) <= float(last["ema21"]): return None, "ema_not_aligned_long", active_bias
        if float(last["rsi"]) < RSI_TREND_LONG_MIN: return None, "rsi_low_long", active_bias

        is_pullback = (float(last["low"]) <= (float(last["ema21"]) + atr_tol) and
                       candle_bull and float(last["close"]) >= (float(last["ema9"]) - confirm_tol))
        is_breakout = (candle_bull and float(last["close"]) > float(prev["high"]) and
                       float(last["close"]) >= (float(last["ema9"]) - confirm_tol))

        if is_pullback:
            if v < (VOL_PULLBACK_MULT * vs): return None, "pullback_vol_low", active_bias
            return "BUY", "trend_pullback_entry", active_bias
        elif is_breakout:
            if adx15 < ADX_TREND_ON: return None, "breakout_blocked_adx", active_bias
            if v < (VOL_SPIKE_MULT * vs): return None, "breakout_blocked_vol_spike", active_bias
            return "BUY", "trend_breakout_entry", active_bias
        return None, "no_setup_long", active_bias

    # SHORT Logic
    if float(last["ema9"]) >= float(last["ema21"]): return None, "ema_not_aligned_short", active_bias
    if float(last["rsi"]) > RSI_TREND_SHORT_MAX: return None, "rsi_high_short", active_bias

    is_pullback = (float(last["high"]) >= (float(last["ema21"]) - atr_tol) and
                   candle_bear and float(last["close"]) <= (float(last["ema9"]) + confirm_tol))
    is_breakout = (candle_bear and float(last["close"]) < float(prev["low"]) and
                   float(last["close"]) <= (float(last["ema9"]) + confirm_tol))

    if is_pullback:
        if v < (VOL_PULLBACK_MULT * vs): return None, "pullback_vol_low", active_bias
        return "SELL", "trend_pullback_entry", active_bias
    elif is_breakout:
        if adx15 < ADX_TREND_ON: return None, "breakout_blocked_adx", active_bias
        if v < (VOL_SPIKE_MULT * vs): return None, "breakout_blocked_vol_spike", active_bias
        return "SELL", "trend_breakout_entry", active_bias
    return None, "no_setup_short", active_bias

def signal_range_mode(df5):
    last = df5.iloc[-2] if len(df5) >= 3 else df5.iloc[-1]
    if pd.isna(last["don_hi"]) or pd.isna(last["don_lo"]): return None, "donchian_not_ready", "NONE"
    if not pd.notna(last["atr"]) or float(last["atr"]) <= 0: return None, "atr_bad", "NONE"
    if pd.isna(last["vol_sma"]) or float(last["vol_sma"]) <= 0: return None, "vol_sma_bad", "NONE"
    
    if float(last["volume"]) < (VOL_OK_RATIO_RANGE * float(last["vol_sma"])):
        return None, "range_low_volume", "NONE"

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])
    atr_tol = DONCHIAN_ATR_TOL * float(last["atr"])

    if (float(last["low"]) <= (float(last["don_lo"]) + atr_tol) and float(last["close"]) > float(last["don_lo"]) and 
        candle_bull and float(last["rsi"]) <= RSI_RANGE_LONG_MIN):
        return "BUY", "range_reject_low", "NONE"
        
    if (float(last["high"]) >= (float(last["don_hi"]) - atr_tol) and float(last["close"]) < float(last["don_hi"]) and 
        candle_bear and float(last["rsi"]) >= RSI_RANGE_SHORT_MAX):
        return "SELL", "range_reject_high", "NONE"

    return None, "no_setup", "NONE"

# =========================
# EKSEKUSI & MANAJEMEN ORDER
# =========================
def place_order_with_actual_bracket(side: str, qty_q: float, atr_val: float, mode: str, mark_price: float, sl_mult: float = 1.0, tp_mult: float = 1.0):
    filters = _get_symbol_filters(SYMBOL)
    tick = float(filters["PRICE_FILTER"]["tickSize"])

    cancel_all_open_orders()
    call_with_retry(client.futures_create_order, kwargs={"symbol": SYMBOL, "side": side, "type": "MARKET", "quantity": qty_q, "recvWindow": RECV_WINDOW})

    actual_entry = 0.0
    for _ in range(12):
        time.sleep(0.5)
        try:
            pos = call_with_retry(client.futures_position_information, kwargs={"symbol": SYMBOL, "recvWindow": RECV_WINDOW})
            for p in pos or []:
                if p.get("symbol") == SYMBOL and float(p.get("positionAmt", 0.0)) != 0.0:
                    actual_entry = float(p.get("entryPrice", 0.0))
                    break
        except:
            pass
        if actual_entry > 0: break

    if actual_entry <= 0.0:
        actual_entry = float(mark_price)

    raw_sl_dist = (atr_val * TREND_SL_ATR_MULT) if mode == "TREND" else (atr_val * RANGE_SL_ATR_MULT)
    sl_min_pct = TREND_SL_MIN_PCT if mode == "TREND" else RANGE_SL_MIN_PCT
    sl_max_pct = TREND_SL_MAX_PCT if mode == "TREND" else RANGE_SL_MAX_PCT
    
    sl_dist = sl_mult * max(actual_entry * sl_min_pct, min(raw_sl_dist, actual_entry * sl_max_pct))
    rr = (TREND_RR if mode == "TREND" else RANGE_RR) * tp_mult

    if side == "BUY":
        sl_price, tp_price = actual_entry - sl_dist, actual_entry + (sl_dist * rr)
        op_side = "SELL"
    else:
        sl_price, tp_price = actual_entry + sl_dist, actual_entry - (sl_dist * rr)
        op_side = "BUY"

    sl_q, tp_q = _round_tick(sl_price, tick), _round_tick(tp_price, tick)

    try:
        call_with_retry(client.futures_create_order, kwargs={"symbol": SYMBOL, "side": op_side, "type": "STOP_MARKET", "stopPrice": sl_q, "closePosition": True, "recvWindow": RECV_WINDOW})
        call_with_retry(client.futures_create_order, kwargs={"symbol": SYMBOL, "side": op_side, "type": "TAKE_PROFIT_MARKET", "stopPrice": tp_q, "closePosition": True, "recvWindow": RECV_WINDOW})
    except Exception as e:
        print(f"Failed to place brackets: {e}")

    return actual_entry, sl_dist, sl_q, tp_q

# =========================
# MAIN LOOP
# =========================
def main():
    print(f"Memulai {TG_PREFIX}...")
    send_telegram(f"🤖 BOT STARTED: {TG_PREFIX}\nLeverage Base: {LEVERAGE}x | Max Risk: {MAX_FORCED_RISK_PCT*100}%")

    st = load_state({
        "mode": "UNKNOWN", "bias": "NONE", "active_bias": "NONE",
        "equity_high": 0.0, "trades_today": 0, "cooldown_until": None, "last_trade_day": None,
        "entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": None, "qty_q": 0.0,
        "be_activated": False, "be_failed_once": False, "trail_activated": False,
        "seen_tran_ids": set()
    })

    while True:
        try:
            now = datetime.now(timezone.utc)
            current_day = now.strftime("%Y-%m-%d")

            if st["last_trade_day"] != current_day:
                st.update({"trades_today": 0, "equity_high": 0.0, "last_trade_day": current_day})
                save_state(st)

            pos_amt = get_position_amt()
            has_pos = abs(pos_amt) > 0.0001
            mark_price = get_mark_price()

            # --- MANAJEMEN POSISI TERBUKA (BE & TRAILING SL) ---
            if has_pos and st["entry_price"] > 0 and st["sl_dist_actual"] > 0:
                pnl_usd = get_unrealized_pnl()
                qty = st.get("qty_q", abs(pos_amt))
                rr_achieved = pnl_usd / (qty * st["sl_dist_actual"]) if qty > 0 else 0.0
                
                # Trailing SL [V9.8]
                if ENABLE_TRAILING_SL and not st.get("trail_activated") and rr_achieved >= TRAIL_ACTIVATION_RR:
                    try:
                        tick = float(_get_symbol_filters(SYMBOL)["PRICE_FILTER"]["tickSize"])
                        cancel_all_open_orders()
                        
                        ep, dist = st["entry_price"], st["sl_dist_actual"]
                        op_side = "SELL" if st["pos_side"] == "LONG" else "BUY"
                        new_sl = ep + (dist * TRAIL_LOCK_RR) if st["pos_side"] == "LONG" else ep - (dist * TRAIL_LOCK_RR)
                            
                        new_sl_q = _round_tick(new_sl, tick)
                        call_with_retry(client.futures_create_order, kwargs={"symbol": SYMBOL, "side": op_side, "type": "STOP_MARKET", "stopPrice": new_sl_q, "closePosition": True, "recvWindow": RECV_WINDOW})
                        
                        st["trail_activated"] = True
                        save_state(st)
                        send_telegram(f"🛡️ TRAILING SL ACTIVATED!\nLocked RR: {TRAIL_LOCK_RR}\nNew SL: {new_sl_q}")
                    except Exception as e:
                        print(f"Failed to trail SL: {e}")

                # Break Even [V9.8]
                elif ENABLE_BREAK_EVEN and not st.get("trail_activated") and not st["be_activated"] and not st["be_failed_once"] and rr_achieved >= BE_ACTIVATION_RR:
                    try:
                        tick = float(_get_symbol_filters(SYMBOL)["PRICE_FILTER"]["tickSize"])
                        cancel_all_open_orders()
                        
                        ep = st["entry_price"]
                        op_side = "SELL" if st["pos_side"] == "LONG" else "BUY"
                        be_price = ep * (1.0 + BE_BUFFER_PCT) if st["pos_side"] == "LONG" else ep * (1.0 - BE_BUFFER_PCT)
                            
                        be_price_q = _round_tick(be_price, tick)
                        call_with_retry(client.futures_create_order, kwargs={"symbol": SYMBOL, "side": op_side, "type": "STOP_MARKET", "stopPrice": be_price_q, "closePosition": True, "recvWindow": RECV_WINDOW})
                        
                        st["be_activated"] = True
                        save_state(st)
                        send_telegram(f"🛡️ BREAK-EVEN ACTIVATED!\nNew SL: {be_price_q}")
                    except Exception as e:
                        st["be_failed_once"] = True
                        save_state(st)
                        print(f"Failed to set BE: {e}")

                time.sleep(SLEEP_FAST)
                continue
            
            # --- RESET STATE JIKA TIDAK ADA POSISI ---
            elif not has_pos:
                if st["entry_price"] != 0.0:
                    st.update({"entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": None, "qty_q": 0.0, "be_activated": False, "be_failed_once": False, "trail_activated": False})
                    save_state(st)

            # --- PRE-ENTRY CHECKS ---
            if st["cooldown_until"]:
                cd_dt = _iso_to_dt(st["cooldown_until"])
                if cd_dt and now < cd_dt:
                    time.sleep(SLEEP_SLOW)
                    continue
                else:
                    st["cooldown_until"] = None
                    save_state(st)

            wb = get_wallet_balance_usdc()
            if wb < 10.0:
                time.sleep(SLEEP_SLOW)
                continue

            if wb > st["equity_high"]:
                st["equity_high"] = wb
                save_state(st)

            # [V9.8] 15m Candle Guard Check
            current_minute = now.minute % 15
            if current_minute >= CANDLE_15M_GUARD_MINUTES:
                time.sleep(SLEEP_SLOW)
                continue

            # --- ANALISA MARKET ---
            mode, bias, adx15, df15, ema200_slope = compute_regime_and_bias(st["mode"])
            st["mode"], st["bias"] = mode, bias
            vol_regime, atr_pct15, low_th, high_th = get_vol_regime_15m(df15)
            
            # [V9.8] Dynamic Leverage
            effective_lev = set_leverage_for_regime(vol_regime)
            
            df5 = compute_entry_indicators_5m()
            last5 = df5.iloc[-2] if len(df5) >= 3 else df5.iloc[-1]
            price = float(last5["close"])

            side, reason, active_bias = None, "no_signal", bias
            if FORCE_TEST_ENTRY:
                side, reason, active_bias = FORCE_TEST_SIDE, "forced_test", FORCE_TEST_SIDE
            else:
                if mode == "TREND":
                    side, reason, active_bias = signal_trend_mode(df5, bias, adx15)
                else:
                    side, reason, active_bias = signal_range_mode(df5)

            st["active_bias"] = active_bias
            
            # Log CSV
            dbg_data = {
                "price": price, "adx15": adx15, "ema9": float(last5["ema9"]), "ema21": float(last5["ema21"]),
                "rsi5": float(last5["rsi"]), "atr5": float(last5["atr"]), "vol": float(last5["volume"]),
                "vol_sma": float(last5["vol_sma"]) if pd.notna(last5["vol_sma"]) else 0.0,
                "don_hi": float(last5["don_hi"]) if pd.notna(last5["don_hi"]) else 0.0,
                "don_lo": float(last5["don_lo"]) if pd.notna(last5["don_lo"]) else 0.0,
                "vol_regime": vol_regime, "atr_pct15": atr_pct15, "atrp_low": low_th, "atrp_high": high_th,
                "ema200_slope": ema200_slope
            }
            log_loop(now, wb, mode, bias, active_bias, reason, dbg_data)

            # --- EKSEKUSI ENTRY ---
            if side in ("BUY", "SELL"):
                risk_pct_used = get_risk_pct_by_vol_regime(vol_regime)
                risk_pct_used = min(risk_pct_used, MAX_FORCED_RISK_PCT)

                atr_val = float(last5["atr"])
                raw_sl_dist = (atr_val * TREND_SL_ATR_MULT) if mode == "TREND" else (atr_val * RANGE_SL_ATR_MULT)
                
                risk_amount = wb * risk_pct_used
                qty = risk_amount / raw_sl_dist if raw_sl_dist > 0 else 0
                
                max_notional = wb * effective_lev * MAX_MARGIN_FRACTION
                max_qty = max_notional / price if price > 0 else 0
                qty = min(qty, max_qty)

                filters = _get_symbol_filters(SYMBOL)
                step = float(filters["LOT_SIZE"]["stepSize"])
                min_qty = float(filters["LOT_SIZE"]["minQty"])
                qty_q = _quantize_step(qty, step)

                if qty_q < min_qty:
                    print(f"Skip entry: Qty {qty_q} < Min {min_qty}")
                    time.sleep(SLEEP_SLOW)
                    continue

                # Modifier TP/SL untuk kondisi High Volatility
                sl_mult = HIGH_VOL_WIDEN_SL_MULT if vol_regime == "HIGH_VOL" else 1.0
                tp_mult = HIGH_VOL_TP_MULT if vol_regime == "HIGH_VOL" else 1.0

                actual_price, sl_dist_actual, sl_final, tp_final = place_order_with_actual_bracket(
                    side, qty_q, atr_val, st["mode"], price, sl_mult, tp_mult
                )

                st.update({
                    "trades_today": int(st.get("trades_today", 0)) + 1,
                    "cooldown_until": _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES)),
                    "entry_price": actual_price, "sl_dist_actual": sl_dist_actual,
                    "pos_side": "LONG" if side == "BUY" else "SHORT",
                    "qty_q": qty_q,
                    "be_activated": False, "be_failed_once": False, "trail_activated": False
                })
                save_state(st)

                send_telegram(
                    f"🚀 ENTRY {SYMBOL} {side}\n"
                    f"Mode: {st['mode']} ({vol_regime})\n"
                    f"Lev: {effective_lev}x | Risk: {risk_pct_used*100:.2f}%\n"
                    f"Qty: {qty_q} | Entry: {actual_price:.2f}\n"
                    f"SL: {sl_final} | TP: {tp_final}\n"
                    f"Risk~: ${(qty_q * sl_dist_actual):.2f}\n"
                    f"EMA200 Slope: {ema200_slope:.4f}\n"
                    f"Reason: {reason}"
                )

            time.sleep(SLEEP_SLOW)

        except Exception as e:
            msg = str(e)
            print(f"Loop Error: {msg}")
            traceback.print_exc()
            time.sleep(SLEEP_SLOW)

if __name__ == "__main__":
    main()