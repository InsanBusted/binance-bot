#!/usr/bin/env python3
# bot-ethusdc-v9.7.1-ultimate.py
# ETHUSDC USD-M Futures - HYBRID Trend & Range
# V9.7.1: FIX BUG Break-Even, FIX Death Loop API, & TESTNET READY

import os
import time
import json
import csv
import random
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN

import requests
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from requests.exceptions import ReadTimeout, ConnectionError

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
MAX_FORCED_RISK_PCT = 0.035
MAX_DAILY_DRAWDOWN_PCT = 0.06
MAX_MARGIN_FRACTION = 0.75

# TRADE MANAGEMENT
BE_ACTIVATION_RR = 0.35     
BE_BUFFER_PCT = 0.00015     

# VOLATILITY REGIME FILTER
VOL_ATR_LEN_15M = 14
VOL_LOOKBACK_15M = 64        
VOL_LOW_MULT = 0.75          
VOL_HIGH_MULT = 1.60         
HIGH_VOL_WIDEN_SL_MULT = 1.15
HIGH_VOL_TP_MULT = 0.90

# SNIPER PARAMETERS
ADX_LEN = 14
ADX_TREND_ON = 24.0          
ADX_RANGE_ON = 18.0
EMA_TREND_LEN = 200
TREND_DEADBAND_PCT = 0.0008

VOL_SPIKE_MULT = 1.20      
VOL_PULLBACK_MULT = 0.50   

# TREND (Disesuaikan untuk ETH)
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

# RANGE (Disesuaikan untuk ETH)
DONCHIAN_LEN = 20
RSI_RANGE_LONG_MIN = 45
RSI_RANGE_SHORT_MAX = 55
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
RECV_WINDOW = 10_000

TG_PREFIX = "ETHUSDC V9.7.1 TESTNET"
STATE_FILE = Path(".state_ethusdc_v9_testnet.json")  
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOOP_LOG = LOG_DIR / "loop_eth_testnet.csv"          

# =========================
# FUNGSI DASAR & STATE
# =========================
def call_with_retry(fn, *args, retries=5, base_sleep=1.0, **kwargs):
    last_err = None
    for i in range(retries):
        try: return fn(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i) + random.uniform(0, 0.5))
    raise last_err

def _dt_to_iso(dt): return dt.isoformat() if dt else None
def _iso_to_dt(s):
    try: return datetime.fromisoformat(s) if s else None
    except: return None

def load_state(defaults):
    try:
        if STATE_FILE.exists(): defaults.update(json.loads(STATE_FILE.read_text()))
    except: pass
    if "seen_tran_ids" in defaults and isinstance(defaults["seen_tran_ids"], list):
        defaults["seen_tran_ids"] = set(defaults["seen_tran_ids"])
    return defaults

def save_state(state):
    try:
        st_copy = state.copy()
        if "seen_tran_ids" in st_copy: st_copy["seen_tran_ids"] = list(st_copy["seen_tran_ids"])
        STATE_FILE.write_text(json.dumps(st_copy, indent=2, sort_keys=True))
    except: pass

def log_loop(now, equity, mode, bias, active_bias, reason, dbg):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not LOOP_LOG.exists()
    row = {"ts": now.isoformat(), "equity": round(equity, 2), "mode": mode, "bias": bias, "active_bias": active_bias, "reason": reason}
    for k, v in dbg.items(): row[k] = round(v, 6) if isinstance(v, float) else v
    header = ["ts","equity","mode","bias","active_bias","reason","price","adx15","ema9","ema21","rsi5","atr5","vol","vol_sma","don_hi","don_lo","vol_regime","atr_pct15","atrp_low","atrp_high"]
    with LOOP_LOG.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if new_file: w.writeheader()
        w.writerow(row)

# =========================
# SETUP BINANCE TESTNET
# =========================
load_dotenv(dotenv_path=".env")
# MENGGUNAKAN API TESTNET DAN TESTNET=TRUE
client = Client(os.getenv("BINANCE_TESTNET_API_KEY"), os.getenv("BINANCE_TESTNET_API_SECRET"), testnet=True)
client.REQUEST_TIMEOUT = 60

def sync_time_offset():
    try:
        client.timestamp_offset = call_with_retry(client.futures_time)["serverTime"] - int(time.time() * 1000)
        return True
    except: return False
sync_time_offset()

def send_telegram(msg: str):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try: requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg}, timeout=8)
        except: pass

# =========================
# HELPERS API BINANCE
# =========================
_exchange_cache = {}
def _get_symbol_filters(symbol):
    if not _exchange_cache.get("info"): _exchange_cache["info"] = call_with_retry(client.futures_exchange_info)
    for s in _exchange_cache["info"]["symbols"]:
        if s["symbol"] == symbol: return {f["filterType"]: f for f in s["filters"]}
    raise RuntimeError("Symbol not found")

def get_min_notional(symbol):
    try:
        f = _get_symbol_filters(symbol)
        if "MIN_NOTIONAL" in f: return float(f["MIN_NOTIONAL"].get("notional", 100))
    except: pass
    return 100.0

def _quantize_step(value, step):
    if step == 0: return float(value)
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))

def _round_tick(value, tick):
    if tick == 0: return float(value)
    return float((Decimal(str(value)) / Decimal(str(tick))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(tick)))

def get_wallet_balance_usdc():
    try: return next((float(b.get("availableBalance", 0.0)) for b in call_with_retry(client.futures_account_balance, recvWindow=RECV_WINDOW) if b.get("asset") == "USDC"), 0.0)
    except: return 0.0

def get_position_amt():
    try: pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except: return 0.0
    return float(pos[0].get("positionAmt", 0.0)) if pos else 0.0

def get_unrealized_pnl():
    try: pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except: return 0.0
    return float(pos[0].get("unRealizedProfit", 0.0)) if pos else 0.0

def realized_pnl_since(start_ms, seen_tran_ids):
    try:
        incomes = call_with_retry(client.futures_income_history, symbol=SYMBOL, incomeType="REALIZED_PNL", startTime=start_ms, limit=1000, recvWindow=RECV_WINDOW)
        total = 0.0
        for it in incomes or []:
            tid = it.get("tranId")
            if tid and tid not in seen_tran_ids:
                total += float(it.get("income", 0.0))
                seen_tran_ids.add(tid)
        return total
    except: return 0.0

def cancel_all_open_orders():
    try: call_with_retry(client.futures_cancel_all_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except: pass

def get_mark_price(): return float(call_with_retry(client.futures_mark_price, symbol=SYMBOL)["markPrice"])

def klines_df(symbol, interval, limit):
    raw = call_with_retry(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(raw, columns=["open_time","open","high","low","close","volume","close_time","qav","nt","tbb","tbq","ign"])
    for c in ["open","high","low","close","volume"]: df[c] = df[c].astype(float)
    return df

# =========================
# INDIKATOR & SINYAL
# =========================
def ema(series, length): return series.ewm(span=length, adjust=False).mean()
def rsi(series, length=14):
    delta = series.diff()
    rs = (delta.clip(lower=0).ewm(alpha=1/length, adjust=False).mean()) / (-delta.clip(upper=0).ewm(alpha=1/length, adjust=False).mean().replace(0, 1e-12))
    return 100 - (100 / (1 + rs))
def atr(df, length=14):
    tr = pd.concat([(df["high"] - df["low"]), (df["high"] - df["close"].shift(1)).abs(), (df["low"] - df["close"].shift(1)).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()
def adx(df, length=14):
    up, down = df["high"].diff(), -df["low"].diff()
    plus_dm, minus_dm = pd.Series(0.0, index=df.index), pd.Series(0.0, index=df.index)
    plus_dm[(up > down) & (up > 0)] = up[(up > down) & (up > 0)]
    minus_dm[(down > up) & (down > 0)] = down[(down > up) & (down > 0)]
    tr = pd.concat([(df["high"] - df["low"]), (df["high"] - df["close"].shift(1)).abs(), (df["low"] - df["close"].shift(1)).abs()], axis=1).max(axis=1)
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

    dist_pct = (price - e200) / e200 if e200 > 0 else 0.0
    if dist_pct >= TREND_DEADBAND_PCT: bias = "LONG"
    elif dist_pct <= -TREND_DEADBAND_PCT: bias = "SHORT"
    else: bias = "NONE"

    mode = "TREND" if ax >= ADX_TREND_ON else "RANGE" if ax <= ADX_RANGE_ON else (prev_mode if prev_mode in ("TREND", "RANGE") else "RANGE")
    return mode, bias, ax, df15

def get_vol_regime_15m(df15):
    df15["atr_15"] = atr(df15, VOL_ATR_LEN_15M)
    last = df15.iloc[-2] if len(df15) > 1 else df15.iloc[-1]
    
    price = float(last["close"])
    a = float(last["atr_15"]) if pd.notna(last["atr_15"]) else 0.0
    if price <= 0 or a <= 0: return "UNKNOWN", 0.0, 0.0, 0.0

    atr_pct = a / price
    hist = df15["atr_15"].iloc[-VOL_LOOKBACK_15M:]
    hist_price = df15["close"].iloc[-VOL_LOOKBACK_15M:]
    hist_atr_pct = (hist / hist_price).replace([float("inf"), -float("inf")], 0).dropna()
    med = float(hist_atr_pct.median()) if len(hist_atr_pct) > 0 else atr_pct

    low_th = med * VOL_LOW_MULT
    high_th = med * VOL_HIGH_MULT

    if atr_pct < low_th: regime = "LOW_VOL"
    elif atr_pct > high_th: regime = "HIGH_VOL"
    else: regime = "NORMAL"

    return regime, atr_pct, low_th, high_th

def compute_entry_indicators_5m():
    df5 = klines_df(SYMBOL, TF_ENTRY, limit=120)
    df5["ema9"], df5["ema21"] = ema(df5["close"], EMA_FAST), ema(df5["close"], EMA_SLOW)
    df5["rsi"], df5["atr"] = rsi(df5["close"], RSI_LEN), atr(df5, 14)
    df5["vol_sma"] = df5["volume"].rolling(20).mean()
    df5["don_hi"] = df5["high"].rolling(DONCHIAN_LEN).max().shift(1)
    df5["don_lo"] = df5["low"].rolling(DONCHIAN_LEN).min().shift(1)
    return df5

def _last_closed(df: pd.DataFrame): return df.iloc[-2] if len(df) >= 3 else df.iloc[-1]

def signal_trend_mode(df5, bias, adx15):
    last = _last_closed(df5)
    prev = df5.iloc[-3] if len(df5) >= 3 else last

    if not pd.notna(last["atr"]) or float(last["atr"]) <= 0: return None, "atr_bad", bias
    
    vs = float(last["vol_sma"]) if pd.notna(last["vol_sma"]) else 0.0
    v = float(last["volume"])
    if vs <= 0: return None, "vol_sma_bad", bias

    active_bias = bias
    if bias == "NONE":
        if float(last["ema9"]) > float(last["ema21"]): active_bias = "LONG"
        elif float(last["ema9"]) < float(last["ema21"]): active_bias = "SHORT"

    if active_bias not in ("LONG", "SHORT"): return None, "no_clear_bias", active_bias

    candle_bull, candle_bear = float(last["close"]) > float(last["open"]), float(last["close"]) < float(last["open"])
    atr_tol, confirm_tol = PULLBACK_ATR_TOL * float(last["atr"]), TREND_CONFIRM_ATR_TOL * float(last["atr"])

    if active_bias == "LONG":
        if float(last["ema9"]) <= float(last["ema21"]): return None, "ema_not_aligned_long", active_bias
        if float(last["rsi"]) < RSI_TREND_LONG_MIN: return None, "rsi_low_long", active_bias
        
        is_pullback = float(last["low"]) <= (float(last["ema21"]) + atr_tol) and candle_bull and (float(last["close"]) >= (float(last["ema9"]) - confirm_tol))
        is_breakout = candle_bull and (float(last["close"]) > float(prev["high"])) and (float(last["close"]) >= (float(last["ema9"]) - confirm_tol))

        if is_pullback:
            if v < (VOL_PULLBACK_MULT * vs): return None, "pullback_vol_low", active_bias
            return "BUY", "trend_pullback_entry", active_bias
        elif is_breakout:
            if adx15 < ADX_TREND_ON: return None, "breakout_blocked_adx", active_bias
            if v < (VOL_SPIKE_MULT * vs): return None, "breakout_blocked_vol_spike", active_bias
            return "BUY", "trend_breakout_entry", active_bias
        return None, "no_setup_long", active_bias

    if float(last["ema9"]) >= float(last["ema21"]): return None, "ema_not_aligned_short", active_bias
    if float(last["rsi"]) > RSI_TREND_SHORT_MAX: return None, "rsi_high_short", active_bias

    is_pullback = float(last["high"]) >= (float(last["ema21"]) - atr_tol) and candle_bear and (float(last["close"]) <= (float(last["ema9"]) + confirm_tol))
    is_breakout = candle_bear and (float(last["close"]) < float(prev["low"])) and (float(last["close"]) <= (float(last["ema9"]) + confirm_tol))

    if is_pullback:
        if v < (VOL_PULLBACK_MULT * vs): return None, "pullback_vol_low", active_bias
        return "SELL", "trend_pullback_entry", active_bias
    elif is_breakout:
        if adx15 < ADX_TREND_ON: return None, "breakout_blocked_adx", active_bias
        if v < (VOL_SPIKE_MULT * vs): return None, "breakout_blocked_vol_spike", active_bias
        return "SELL", "trend_breakout_entry", active_bias
    return None, "no_setup_short", active_bias

def signal_range_mode(df5):
    last = _last_closed(df5)
    if pd.isna(last["don_hi"]) or pd.isna(last["don_lo"]): return None, "donchian_not_ready", "NONE"
    if not pd.notna(last["atr"]) or float(last["atr"]) <= 0: return None, "atr_bad", "NONE"
    if pd.isna(last["vol_sma"]) or float(last["vol_sma"]) <= 0: return None, "vol_sma_bad", "NONE"
    if float(last["volume"]) < (VOL_OK_RATIO_RANGE * float(last["vol_sma"])): return None, "range_low_volume", "NONE"

    candle_bull, candle_bear = float(last["close"]) > float(last["open"]), float(last["close"]) < float(last["open"])
    atr_tol = DONCHIAN_ATR_TOL * float(last["atr"])

    if float(last["low"]) <= (float(last["don_lo"]) + atr_tol) and float(last["close"]) > float(last["don_lo"]) and candle_bull and float(last["rsi"]) >= RSI_RANGE_LONG_MIN:
        return "BUY", "range_reject_low", "NONE"
    if float(last["high"]) >= (float(last["don_hi"]) - atr_tol) and float(last["close"]) < float(last["don_hi"]) and candle_bear and float(last["rsi"]) <= RSI_RANGE_SHORT_MAX:
        return "SELL", "range_reject_high", "NONE"
    return None, "no_setup", "NONE"

# =========================
# EKSEKUSI & MANAJEMEN ORDER
# =========================
def _ensure_brackets_exist():
    try:
        oo = call_with_retry(client.futures_get_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        types = {o.get("type") for o in (oo or [])}
        return ("STOP_MARKET" in types) and ("TAKE_PROFIT_MARKET" in types)
    except: return False

def place_order_with_actual_bracket(side: str, qty_q: float, atr_val: float, mode: str, mark_price: float, sl_mult: float=1.0, tp_mult: float=1.0):
    filters = _get_symbol_filters(SYMBOL)
    tick = float(filters["PRICE_FILTER"]["tickSize"])
    cancel_all_open_orders()

    call_with_retry(client.futures_create_order, symbol=SYMBOL, side=side, type="MARKET", quantity=qty_q, recvWindow=RECV_WINDOW)

    actual_entry = 0.0
    for _ in range(12):
        time.sleep(0.5)
        try:
            pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
            for p in pos or []:
                if p.get("symbol") == SYMBOL and float(p.get("positionAmt", 0.0)) != 0.0:
                    actual_entry = float(p.get("entryPrice", 0.0))
                    break
        except: pass
        if actual_entry > 0: break

    if actual_entry <= 0.0: actual_entry = float(mark_price)

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
        call_with_retry(client.futures_create_order, symbol=SYMBOL, side=op_side, type="STOP_MARKET", stopPrice=sl_q, closePosition=True, workingType="MARK_PRICE", recvWindow=RECV_WINDOW)
        call_with_retry(client.futures_create_order, symbol=SYMBOL, side=op_side, type="TAKE_PROFIT_MARKET", stopPrice=tp_q, closePosition=True, workingType="MARK_PRICE", recvWindow=RECV_WINDOW)
        
        # TEST MODE: skip strict bracket verification
        time.sleep(2.0)
        brackets_ok = True
        
        # FORCE TEST: bypass semua filter sizing
        step_size = float(_get_symbol_filters(SYMBOL)["LOT_SIZE"]["stepSize"])
        qty_q = _quantize_step(0.05, step_size)   # kecil saja untuk test
        atr_val = float(last_closed["atr"])
        sl_mult = 1.0
        tp_mult = 1.0

        actual_price, sl_final, tp_final, sl_dist_actual = place_order_with_actual_bracket(
            side, qty_q, atr_val, st["mode"], price, sl_mult, tp_mult
        )

        st.update({
            "trades_today": int(st.get("trades_today", 0)) + 1,
            "cooldown_until": _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES)),
            "entry_price": actual_price,
            "sl_dist_actual": sl_dist_actual,
            "pos_side": "LONG",
            "qty_q": qty_q,
            "be_activated": False
        })
        save_state(st)

        send_telegram(
            f"🧪 FORCE ENTRY {SYMBOL} {side}\n"
            f"Qty: {qty_q}\n"
            f"Entry: {actual_price:.2f}\n"
            f"SL: {sl_final} | TP: {tp_final}\n"
            f"Reason: {reason}"
        )

        time.sleep(SLEEP_SLOW)
        send_telegram(
            f"🧪 FORCE ENTRY {SYMBOL} {side}"
        )

        time.sleep(SLEEP_SLOW)
                
    except Exception as e:
        print(f"CRITICAL ERROR: Failed Bracket. Emergency close. Error: {e}")
        try:
            call_with_retry(client.futures_create_order, symbol=SYMBOL, side=op_side, type="MARKET", quantity=qty_q, reduceOnly=True, recvWindow=RECV_WINDOW)
            send_telegram(f"🚨 EMERGENCY: Gagal pasang SL/TP. Posisi DITUTUP OTOMATIS! Err: {e}")
            
            # PERBAIKAN: PAKSA SIMPAN COOLDOWN AGAR TIDAK DEATH LOOP
            st_temp = load_state({})
            st_temp["cooldown_until"] = _dt_to_iso(datetime.now(timezone.utc) + pd.Timedelta(minutes=COOLDOWN_MINUTES))
            save_state(st_temp)
            
        except Exception as ex:
            send_telegram(f"💀 FATAL: Gagal pasang SL/TP dan gagal close. CEK BINANCE MANUAL. Err: {repr(ex)}")
        raise

    return actual_entry, sl_q, tp_q, sl_dist

def manage_break_even(st, mark_price, tick_size, qty_q):
    if st.get("be_activated", False): return
    
    entry_price = float(st.get("entry_price", 0.0))
    sl_dist = float(st.get("sl_dist_actual", 0.0))
    side = st.get("pos_side", "")
    
    if entry_price <= 0 or sl_dist <= 0: return

    if side == "LONG": profit_r = (mark_price - entry_price) / sl_dist
    elif side == "SHORT": profit_r = (entry_price - mark_price) / sl_dist
    else: return

    if profit_r >= BE_ACTIVATION_RR:
        try:
            open_orders = call_with_retry(client.futures_get_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
            for o in open_orders:
                if o["type"] == "STOP_MARKET":
                    call_with_retry(client.futures_cancel_order, symbol=SYMBOL, orderId=o["orderId"], recvWindow=RECV_WINDOW)
            
            op_side = "SELL" if side == "LONG" else "BUY"
            if side == "LONG": be_price = entry_price * (1 + BE_BUFFER_PCT)
            else: be_price = entry_price * (1 - BE_BUFFER_PCT)
            
            be_price_q = _round_tick(be_price, tick_size)
            call_with_retry(client.futures_create_order, symbol=SYMBOL, side=op_side, type="STOP_MARKET", stopPrice=be_price_q, closePosition=True, workingType="MARK_PRICE", recvWindow=RECV_WINDOW)
            
            st["be_activated"] = True
            save_state(st)
            send_telegram(f"🛡️ ETH Break-Even Activated!\nProfit capai {BE_ACTIVATION_RR}R.\nSL aman di titik impas: {be_price_q}")
        except Exception as e:
            print(f"Error activating BE: {e}")

# =========================
# MAIN LOOP
# =========================
def main():
    try: call_with_retry(client.futures_change_leverage, symbol=SYMBOL, leverage=LEVERAGE, recvWindow=RECV_WINDOW)
    except: pass

    min_notional = get_min_notional(SYMBOL)
    tick_size = float(_get_symbol_filters(SYMBOL)["PRICE_FILTER"]["tickSize"])
    
    send_telegram(f"🟢 {TG_PREFIX} started\nEq: ${get_wallet_balance_usdc():.2f}\nMode: Ultimate Sniper ETH + Vol Regime")

    st = load_state({
        "day_key": datetime.now(timezone.utc).date().isoformat(),
        "start_equity_today": get_wallet_balance_usdc(),
        "daily_realized_pnl": 0.0,
        "trades_today": 0, "loss_streak": 0, 
        "daily_locked": False, "cooldown_until": None,
        "mode": "RANGE", "prev_in_position": False,
        "last_pnl_check_ms": int(time.time() * 1000) - 60_000,
        "seen_tran_ids": set(),
        "entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": "", "be_activated": False, "qty_q": 0.0
    })

    last_time_sync = time.time()
    TIME_SYNC_EVERY_S = 30 * 60

    while True:
        try:
            now = datetime.now(timezone.utc)
            if (time.time() - last_time_sync) >= TIME_SYNC_EVERY_S:
                if sync_time_offset(): last_time_sync = time.time()

            cur_day = now.date().isoformat()
            if cur_day != st.get("day_key"):
                st.update({"day_key": cur_day, "start_equity_today": get_wallet_balance_usdc(), "daily_realized_pnl": 0.0, "trades_today": 0, "loss_streak": 0, "daily_locked": False, "cooldown_until": None})
                send_telegram(f"🗓 ETH Day reset. Start equity: ${st['start_equity_today']:.2f}")
                save_state(st)

            equity_now = get_wallet_balance_usdc()
            pos_amt = get_position_amt()
            in_pos = abs(pos_amt) > 0

            if st.get("prev_in_position") and not in_pos:
                pnl = realized_pnl_since(int(st.get("last_pnl_check_ms", 0)), st["seen_tran_ids"])
                st["last_pnl_check_ms"] = int(time.time() * 1000)
                st["daily_realized_pnl"] = float(st.get("daily_realized_pnl", 0.0)) + pnl
                st["loss_streak"] = int(st.get("loss_streak", 0)) + 1 if pnl < 0 else 0
                
                st.update({"entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": "", "be_activated": False, "qty_q": 0.0})

                send_telegram(f"✅ ETH Trade Closed | PnL: ${pnl:.4f} | Streak: {st['loss_streak']}")
                if st["loss_streak"] >= LOSS_STREAK_LIMIT:
                    st["daily_locked"] = True
                    send_telegram("🧯 ETH LOSS STREAK LIMIT! Locked until tomorrow.")
                else:
                    st["cooldown_until"] = _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES))
                
                st["prev_in_position"] = False
                save_state(st)
                time.sleep(SLEEP_SLOW)
                continue

            if in_pos:
                st["prev_in_position"] = True
                mark_price = get_mark_price()
                manage_break_even(st, mark_price, tick_size, st.get("qty_q", 0.0))
                time.sleep(SLEEP_SLOW)
                continue

            st["prev_in_position"] = False

            total_daily_pnl = float(st.get("daily_realized_pnl", 0.0))
            start_eq = float(st.get("start_equity_today", 0.0)) or 0.0
            daily_dd = (total_daily_pnl / start_eq) if start_eq > 0 else 0.0

            if daily_dd <= -abs(MAX_DAILY_DRAWDOWN_PCT):
                if not st.get("daily_locked"):
                    st["daily_locked"] = True
                    send_telegram(f"🛑 ETH DAILY STOP! DD: {daily_dd*100:.2f}%")
                    save_state(st)
                time.sleep(SLEEP_SLOW)
                continue

            if int(st.get("trades_today", 0)) >= MAX_TRADES_PER_DAY:
                if not st.get("daily_locked"):
                    st["daily_locked"] = True
                    send_telegram("⚠️ ETH Trade Limit Reached. Locked until tomorrow.")
                    save_state(st)
                time.sleep(SLEEP_SLOW)
                continue

            cdt = _iso_to_dt(st.get("cooldown_until"))
            if st.get("daily_locked") or (cdt and now < cdt):
                time.sleep(SLEEP_SLOW)
                continue

            st["mode"], bias, adx15, df15 = compute_regime_and_bias(st.get("mode", "RANGE"))
            vol_regime, atrp, low_th, high_th = get_vol_regime_15m(df15)
            
            df5 = compute_entry_indicators_5m()
            last_closed = _last_closed(df5)
            price = float(last_closed["close"])

            active_bias = "NONE"
            if st["mode"] == "TREND": side, reason, active_bias = signal_trend_mode(df5, bias, float(adx15))
            else: side, reason, active_bias = signal_range_mode(df5)

            if vol_regime == "LOW_VOL" and reason in ("trend_breakout_entry",):
                side = None
                reason = "blocked_low_vol_breakout"

            dbg = {
                "price": price, "adx15": float(adx15), "ema9": float(last_closed.get("ema9", 0.0)), "ema21": float(last_closed.get("ema21", 0.0)), 
                "rsi5": float(last_closed.get("rsi", 0.0)), "atr5": float(last_closed.get("atr", 0.0)), "vol": float(last_closed.get("volume", 0.0)), 
                "vol_sma": float(last_closed.get("vol_sma", 0.0)) if pd.notna(last_closed.get("vol_sma", 0.0)) else 0.0,
                "vol_regime": vol_regime, "atr_pct15": atrp, "atrp_low": low_th, "atrp_high": high_th
            }
            log_loop(now, equity_now, st["mode"], bias, active_bias, reason, dbg)

            if side is None:
                side = "BUY"
                reason = "FORCE_TEST_ENTRY"
                active_bias = "LONG"

            sl_mult = 1.0
            tp_mult = 1.0
            if vol_regime == "HIGH_VOL":
                sl_mult = HIGH_VOL_WIDEN_SL_MULT
                tp_mult = HIGH_VOL_TP_MULT

            atr_val = float(last_closed["atr"])
            raw_sl_dist = (atr_val * TREND_SL_ATR_MULT) if st["mode"] == "TREND" else (atr_val * RANGE_SL_ATR_MULT)
            sl_min_pct = TREND_SL_MIN_PCT if st["mode"] == "TREND" else RANGE_SL_MIN_PCT
            sl_max_pct = TREND_SL_MAX_PCT if st["mode"] == "TREND" else RANGE_SL_MAX_PCT
            
            sl_dist_est = sl_mult * max(price * sl_min_pct, min(raw_sl_dist, price * sl_max_pct))

            risk_usd = equity_now * RISK_PCT
            qty = (risk_usd / sl_dist_est) if sl_dist_est > 0 else 0.0
            step_size = float(_get_symbol_filters(SYMBOL)["LOT_SIZE"]["stepSize"])
            qty_q = _quantize_step(qty, step_size)
            notional_q = qty_q * price

            if notional_q < min_notional:
                forced_notional = min_notional * 1.02
                qty_q = _quantize_step(forced_notional / price, step_size)
                notional_q = qty_q * price
                if ((qty_q * sl_dist_est) / max(equity_now, 1e-9)) > MAX_FORCED_RISK_PCT:
                    print(f"Skip: Risk melebihi MAX_FORCED_RISK ({MAX_FORCED_RISK_PCT*100}%)")
                    time.sleep(SLEEP_SLOW); continue

            if (notional_q / LEVERAGE) > (equity_now * MAX_MARGIN_FRACTION): time.sleep(SLEEP_SLOW); continue
            
            est_fee = (notional_q * TAKER_FEE_PCT) * 2.0
            rr = (TREND_RR if st["mode"] == "TREND" else RANGE_RR) * tp_mult
            
            if (qty_q * (sl_dist_est * rr)) <= est_fee * 1.5: time.sleep(SLEEP_SLOW); continue

            if notional_q >= min_notional and qty_q > 0:
                actual_price, sl_final, tp_final, sl_dist_actual = place_order_with_actual_bracket(side, qty_q, atr_val, st["mode"], price, sl_mult, tp_mult)
                
                # FIX V9.7.1: pos_side konversi
                st.update({
                    "trades_today": int(st.get("trades_today", 0)) + 1,
                    "cooldown_until": _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES)),
                    "entry_price": actual_price,
                    "sl_dist_actual": sl_dist_actual,
                    "pos_side": "LONG" if side == "BUY" else "SHORT",
                    "qty_q": qty_q,
                    "be_activated": False
                })
                save_state(st)

                send_telegram(f"🚀 ENTRY {SYMBOL} {side}\nMode: {st['mode']} ({vol_regime})\nQty: {qty_q}\nEntry: {actual_price:.2f}\nSL: {sl_final} | TP: {tp_final}\nRisk~ ${(qty_q * sl_dist_actual):.2f}\nReason: {reason}")

            time.sleep(SLEEP_SLOW)

        except Exception as e:
            msg = str(e)
            if ("Timestamp for this request" in msg) or ("recvWindow" in msg) or ("-1021" in msg):
                sync_time_offset()
                last_time_sync = time.time()
            time.sleep(SLEEP_SLOW)

if __name__ == "__main__": main()