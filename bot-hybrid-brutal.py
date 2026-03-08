#!/usr/bin/env python3
# bot-hybrid-brutal-v10.py
# MULTIPAIR USD-M Futures - SCALPING TRIPLE EMA + CHOP + WEBSOCKET + BATCH ORDERS
# 100% TESTNET READY

import os
import time
import json
import csv
import random
import traceback
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Tuple, Optional

import requests
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from requests.exceptions import ReadTimeout, ConnectionError
from binance import ThreadedWebsocketManager

# =========================
# WEBSOCKET STREAMER (REAL-TIME PUSH)
# =========================
class BinanceStreamer:
    def __init__(self, api_key, api_secret, symbol, tf_entry):
        self.symbol = symbol
        self.tf_entry = tf_entry
        self.twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret, testnet=True)
        self.mark_price = 0.0
        self.candle_closed = False

    def start(self):
        self.twm.start()
        # Stream Mark Price (Update hitungan milidetik)
        self.twm.start_symbol_mark_price_socket(callback=self.handle_mark_price, symbol=self.symbol)
        # Stream Klines (Update pergerakan candle)
        self.twm.start_kline_socket(callback=self.handle_kline, symbol=self.symbol, interval=self.tf_entry)
        print(f"🟢 WebSocket Stream Started for {self.symbol}...")

    def handle_mark_price(self, msg):
        # Tambahkan pengecekan agar tidak error jika 'e' tidak ada
        if msg and isinstance(msg, dict) and 'e' in msg:
            if msg['e'] == 'markPriceUpdate':
                self.mark_price = float(msg['p'])
        else:
            # Ini biasanya pesan koneksi (ping/pong), abaikan saja
            pass

    def handle_kline(self, msg):
        # Tambahkan pengecekan yang sama di sini
        if msg and isinstance(msg, dict) and 'e' in msg:
            if msg['e'] == 'kline':
                if msg['k']['x']:  # Jika 'x' True, artinya candle baru saja tutup
                    self.candle_closed = True

    def stop(self):
        self.twm.stop()


# =========================
# CONFIGURASI BOT & MODAL
# =========================
FORCE_TEST_ENTRY = False
FORCE_TEST_SIDE = "BUY"

SYMBOL = "BTCUSDT"
LEVERAGE = 25
TF_REGIME = "15m"
TF_ENTRY = "5m"
TAKER_FEE_PCT = 0.0005
USE_CLOSED_CANDLE_ONLY = True
ENABLE_RANGE_MODE = True  # Ubah ke False jika hanya ingin bot fokus pada Trend/Scalping saja

def get_quote_asset(symbol: str) -> str:
    for q in ("USDT", "USDC", "BUSD"):
        if symbol.endswith(q):
            return q
    raise ValueError(f"Quote asset tidak dikenali dari symbol: {symbol}")

QUOTE_ASSET = get_quote_asset(SYMBOL)
SYMBOL_TAG = SYMBOL.lower()

# Parameter Aggressive Small Account
RISK_PCT = 0.05
MAX_FORCED_RISK_PCT = 0.15
MAX_DAILY_DRAWDOWN_PCT = 0.40
MAX_MARGIN_FRACTION = 0.95

# VOLATILITY ADAPTIVE RISK
LOW_VOL_RISK_MULT = 1.00
NORMAL_VOL_RISK_MULT = 1.25
HIGH_VOL_RISK_MULT = 0.90

# TRADE MANAGEMENT (BREAK EVEN)
ENABLE_BREAK_EVEN = True
BE_ACTIVATION_RR = 1.0  # Diturunkan untuk Scalping (Aman cepat impas)
BE_BUFFER_PCT = 0.0003

# VOLATILITY REGIME FILTER
VOL_ATR_LEN_15M = 14
VOL_LOOKBACK_15M = 48
VOL_LOW_MULT = 0.70
VOL_HIGH_MULT = 1.45
HIGH_VOL_WIDEN_SL_MULT = 1.40
HIGH_VOL_TP_MULT = 1.50

# SCALPING PARAMETERS (TRIPLE EMA + CHOP)
CHOP_LEN = 14
CHOP_TREND_ON = 38.2
CHOP_RANGE_ON = 61.8
EMA_TREND_LEN = 200
TREND_DEADBAND_PCT = 0.0005

# TREND SCALPING
EMA_FAST = 20
EMA_SLOW = 50
EMA_200 = 200
RSI_LEN = 14
TREND_RR = 1.5  # Diubah agar realistis untuk Scalping
TREND_SL_ATR_MULT = 2.5
TREND_SL_MIN_PCT = 0.0050
TREND_SL_MAX_PCT = 0.0150


# RANGE
DONCHIAN_LEN = 14
RSI_RANGE_LONG_MIN = 42
RSI_RANGE_SHORT_MAX = 58
RANGE_RR = 2.0
RANGE_SL_ATR_MULT = 2.0
RANGE_SL_MIN_PCT = 0.0040
RANGE_SL_MAX_PCT = 0.0120
VOL_OK_RATIO_RANGE = 1.10 # Butuh minimal 10% volume di atas rata-rata untuk validasi Donchian
DONCHIAN_ATR_TOL = 0.28

MAX_TRADES_PER_DAY = 100
COOLDOWN_MINUTES = 1
LOSS_STREAK_LIMIT = 15

SLEEP_SLOW = 6
SLEEP_FAST = 2
RECV_WINDOW = 10_000

TG_PREFIX = f"{SYMBOL} V10 SCALPER TESTNET"
STATE_FILE = Path(f".state_{SYMBOL_TAG}_v10_testnet.json")
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOOP_LOG = LOG_DIR / f"loop_{SYMBOL_TAG}_testnet.csv"


# =========================
# FUNGSI DASAR & STATE
# =========================
def call_with_retry(fn, *args, retries=5, base_sleep=1.0, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            time.sleep(base_sleep * (2 ** i) + random.uniform(0, 0.5))
    raise last_err

def _dt_to_iso(dt):
    return dt.isoformat() if dt else None

def _iso_to_dt(s):
    try:
        return datetime.fromisoformat(s) if s else None
    except:
        return None

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

def log_loop(now, equity, mode, bias, reason, dbg):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    new_file = not LOOP_LOG.exists()
    row = {
        "ts": now.isoformat(),
        "equity": round(equity, 2),
        "mode": mode,
        "bias": bias,
        "reason": reason
    }
    for k, v in dbg.items():
        row[k] = round(v, 6) if isinstance(v, float) else v

    header = [
        "ts", "equity", "mode", "bias", "reason",
        "price", "chop15", "ema20", "ema50", "ema200", "rsi5", "atr5",
        "vol", "vol_sma", "don_hi", "don_lo", "vol_regime",
        "atr_pct15", "atrp_low", "atrp_high"
    ]

    with LOOP_LOG.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        if new_file:
            w.writeheader()
        w.writerow(row)


# =========================
# SETUP BINANCE TESTNET
# =========================
load_dotenv(dotenv_path=".env")
client = Client(
    os.getenv("BINANCE_TESTNET_API_KEY"),
    os.getenv("BINANCE_TESTNET_API_SECRET"),
    testnet=True
)
client.REQUEST_TIMEOUT = 60

def sync_time_offset():
    try:
        client.timestamp_offset = call_with_retry(client.futures_time)["serverTime"] - int(time.time() * 1000)
        return True
    except:
        return False

sync_time_offset()

def send_telegram(msg: str):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=8
            )
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

def _quantize_step(value, step):
    if step == 0:
        return float(value)
    return float((Decimal(str(value)) / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step)))

def _round_tick(value, tick):
    if tick == 0:
        return float(value)
    return float((Decimal(str(value)) / Decimal(str(tick))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(tick)))

def get_wallet_balance_quote():
    try:
        balances = call_with_retry(client.futures_account_balance, recvWindow=RECV_WINDOW)
        return next((float(b.get("availableBalance", 0.0)) for b in balances if b.get("asset") == QUOTE_ASSET), 0.0)
    except:
        return 0.0

def get_position_amt():
    try:
        pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except:
        return 0.0
    return float(pos[0].get("positionAmt", 0.0)) if pos else 0.0

def realized_pnl_since(start_ms, seen_tran_ids):
    try:
        incomes = call_with_retry(
            client.futures_income_history,
            symbol=SYMBOL, startTime=start_ms, limit=1000, recvWindow=RECV_WINDOW
        )
        total = 0.0
        new_seen = []
        for it in incomes or []:
            tid = it.get("tranId")
            income_type = it.get("incomeType")
            if not tid or tid in seen_tran_ids:
                continue
            if income_type in ("REALIZED_PNL", "COMMISSION"):
                total += float(it.get("income", 0.0))
                new_seen.append(tid)

        for tid in new_seen:
            seen_tran_ids.add(tid)
        return total, len(new_seen) > 0
    except:
        return 0.0, False

def get_closed_trade_pnl_with_retry(start_ms, seen_tran_ids, retries=5, wait_sec=2):
    total_pnl = 0.0
    got_any = False
    for _ in range(retries):
        pnl, found = realized_pnl_since(start_ms, seen_tran_ids)
        if found:
            total_pnl += pnl
            got_any = True
            time.sleep(1.5)
            extra_pnl, _ = realized_pnl_since(start_ms, seen_tran_ids)
            total_pnl += extra_pnl
            break
        time.sleep(wait_sec)
    return total_pnl, got_any

def cancel_all_open_orders():
    try:
        call_with_retry(client.futures_cancel_all_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
    except:
        pass

def klines_df(symbol, interval, limit):
    raw = call_with_retry(client.futures_klines, symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(raw, columns=["open_time", "open", "high", "low", "close", "volume", "close_time", "qav", "nt", "tbb", "tbq", "ign"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    return df

def get_mark_price():
    try:
        # Mengambil harga mark price terbaru dari Binance Futures
        mp = call_with_retry(client.futures_mark_price, symbol=SYMBOL)
        return float(mp["markPrice"])
    except Exception as e:
        print(f"Error di get_mark_price: {e}")
        return 0.0
    
def check_telegram_commands(st):
    """Mengecek pesan masuk dari Telegram (Polling singkat)"""
    try:
        # Kita gunakan limit 1 dan timeout pendek agar tidak menghambat loop trading
        url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates?offset=-1&limit=1"
        resp = requests.get(url, timeout=2).json()
        
        if resp.get("ok") and resp.get("result"):
            last_msg = resp["result"][0]
            text = last_msg.get("message", {}).get("text", "")
            update_id = last_msg.get("update_id")
            
            # Gunakan seen_update_id agar bot tidak menjawab pesan yang sama berulang kali
            if update_id != st.get("last_tg_update_id"):
                st["last_tg_update_id"] = update_id
                
                if text == "/status":
                    eq = get_wallet_balance_quote()
                    msg = (
                        f"📊 *STATUS REPORT*\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"💰 Equity: ${eq:.2f}\n"
                        f"📈 PnL Today: ${st['daily_realized_pnl']:.2f}\n"
                        f"🔄 Trades: {st['trades_today']}\n"
                        f"🤖 Mode: {st['mode']}\n"
                        f"📍 Status: {'🟢 IN POSITION' if st['prev_in_position'] else '⚪ IDLE'}"
                    )
                    send_telegram(msg)
                
                elif text == "/stop":
                    send_telegram("⚠️ Emergency Stop received! Bot shutting down...")
                    os._exit(0) # Mematikan bot secara paksa
                    
    except Exception as e:
        # Abaikan error telegram agar loop trading tetap jalan
        pass

# =========================
# INDIKATOR (CHOP & TRIPLE EMA)
# =========================
def ema(series, length):
    return series.ewm(span=length, adjust=False).mean()

def rsi(series, length=14):
    delta = series.diff()
    rs = (delta.clip(lower=0).ewm(alpha=1/length, adjust=False).mean() / (-delta.clip(upper=0).ewm(alpha=1/length, adjust=False).mean().replace(0, 1e-12)))
    return 100 - (100 / (1 + rs))

def atr(df, length=14):
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def chop(df: pd.DataFrame, length: int = 14) -> pd.Series:
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    atr_sum = tr.rolling(length).sum()
    highest_high = df["high"].rolling(length).max()
    lowest_low = df["low"].rolling(length).min()
    range_hl = (highest_high - lowest_low).replace(0, 1e-12)
    return 100 * np.log10(atr_sum / range_hl) / np.log10(length)


def compute_regime_and_bias(prev_mode):
    df15 = klines_df(SYMBOL, TF_REGIME, limit=max(220, VOL_LOOKBACK_15M + 20))
    df15["ema200"] = ema(df15["close"], EMA_TREND_LEN)
    df15["chop"] = chop(df15, CHOP_LEN)

    last = df15.iloc[-2] if len(df15) > 1 else df15.iloc[-1]
    price = float(last["close"])
    e200 = float(last["ema200"])
    chop_val = float(last["chop"])

    dist_pct = (price - e200) / e200 if e200 > 0 else 0.0
    if dist_pct >= TREND_DEADBAND_PCT:
        bias = "LONG"
    elif dist_pct <= -TREND_DEADBAND_PCT:
        bias = "SHORT"
    else:
        bias = "NONE"

    if chop_val <= CHOP_TREND_ON:
        mode = "TREND"
    elif chop_val >= CHOP_RANGE_ON:
        mode = "RANGE"
    else:
        mode = prev_mode if prev_mode in ("TREND", "RANGE") else "RANGE"

    return mode, bias, chop_val, df15

def get_vol_regime_15m(df15):
    df15["atr_15"] = atr(df15, VOL_ATR_LEN_15M)
    last = df15.iloc[-2] if len(df15) > 1 else df15.iloc[-1]
    price = float(last["close"])
    a = float(last["atr_15"]) if pd.notna(last["atr_15"]) else 0.0
    
    if price <= 0 or a <= 0:
        return "UNKNOWN", 0.0, 0.0, 0.0

    atr_pct = a / price
    hist = df15["atr_15"].iloc[-VOL_LOOKBACK_15M:]
    hist_price = df15["close"].iloc[-VOL_LOOKBACK_15M:]
    hist_atr_pct = (hist / hist_price).replace([float("inf"), -float("inf")], 0).dropna()
    med = float(hist_atr_pct.median()) if len(hist_atr_pct) > 0 else atr_pct

    low_th = med * VOL_LOW_MULT
    high_th = med * VOL_HIGH_MULT

    if atr_pct < low_th:
        regime = "LOW_VOL"
    elif atr_pct > high_th:
        regime = "HIGH_VOL"
    else:
        regime = "NORMAL"

    return regime, atr_pct, low_th, high_th

def get_risk_pct_by_vol_regime(vol_regime: str) -> float:
    if vol_regime == "LOW_VOL": return RISK_PCT * LOW_VOL_RISK_MULT
    elif vol_regime == "HIGH_VOL": return RISK_PCT * HIGH_VOL_RISK_MULT
    return RISK_PCT * NORMAL_VOL_RISK_MULT

def compute_entry_indicators_5m() -> Tuple[pd.DataFrame, dict]:
    df5 = klines_df(SYMBOL, TF_ENTRY, limit=260)
    df5["ema_fast"] = ema(df5["close"], EMA_FAST)     # EMA 20
    df5["ema_slow"] = ema(df5["close"], EMA_SLOW)     # EMA 50
    df5["ema_200"] = ema(df5["close"], EMA_200)       # EMA 200
    df5["rsi"] = rsi(df5["close"], RSI_LEN)
    df5["atr"] = atr(df5, 14)
    df5["don_hi"] = df5["high"].rolling(DONCHIAN_LEN).max().shift(1)
    df5["don_lo"] = df5["low"].rolling(DONCHIAN_LEN).min().shift(1)
    df5["vol_sma"] = df5["volume"].rolling(20).mean()

    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    dbg = {
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
        "ema_200": float(last["ema_200"]) if pd.notna(last["ema_200"]) else 0.0,
        "rsi5": float(last["rsi"]),
        "atr5": float(last["atr"]),
        "don_hi": float(last["don_hi"]) if pd.notna(last["don_hi"]) else None,
        "don_lo": float(last["don_lo"]) if pd.notna(last["don_lo"]) else None,
        "vol_sma": float(last["vol_sma"]) if pd.notna(last["vol_sma"]) else 0.0,
    }
    return df5, dbg

def _last_closed(df: pd.DataFrame):
    return df.iloc[-2] if len(df) >= 3 else df.iloc[-1]

def signal_trend_mode(df5: pd.DataFrame, bias: str) -> Tuple[Optional[str], str, dict]:
    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    ema20 = float(last["ema_fast"])
    ema50 = float(last["ema_slow"])
    ema200 = float(last["ema_200"])
    rsiv = float(last["rsi"])
    atrv = float(last["atr"])
    price = float(last["close"])
    vol = float(last["volume"])
    vol_sma = float(last["vol_sma"])

    dbg = {"touch": False, "rejection": False, "confirm": False}

    if atrv <= 0 or pd.isna(ema200):
        return None, "warming_up_or_bad_atr", dbg

    # Filter Volume dinaikkan jadi 1.30 (30% di atas rata-rata) agar terhindar dari market sepi
    if vol < (vol_sma * 1.30):
        return None, "low_momentum_volume", dbg

    candle_bull = price > float(last["open"])
    candle_bear = price < float(last["open"])

    lookback = 3
    window = df5.iloc[-(lookback+1):-1] if USE_CLOSED_CANDLE_ONLY else df5.iloc[-lookback:]
    pullback_touch_long = any(float(row["low"]) <= float(row["ema_fast"]) for _, row in window.iterrows())
    pullback_touch_short = any(float(row["high"]) >= float(row["ema_fast"]) for _, row in window.iterrows())

    # Hitung panjang bodi dan ekor candle
    candle_body = abs(price - float(last["open"]))
    upper_wick = float(last["high"]) - max(price, float(last["open"]))
    lower_wick = min(price, float(last["open"])) - float(last["low"])

    # ==================================
    # LOGIKA LONG
    # ==================================
    if bias == "LONG":
        if price <= ema200 or ema20 <= ema50: return None, "ema_not_aligned_long", dbg
        if rsiv < 45: return None, "rsi_too_weak_long", dbg
        if not pullback_touch_long: return None, "no_pullback_touch_ema20", dbg
        
        # Cek Fake Out Ekor Atas (Wick Rejection) SEBELUM konfirmasi BUY
        if upper_wick > candle_body:
            return None, "fakeout_long_wick_rejected", dbg

        if (not candle_bull) or (price < ema20):
            dbg["touch"] = True
            return None, "no_bull_confirm", dbg

        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return "BUY", "scalp_pullback_long", dbg

    # ==================================
    # LOGIKA SHORT
    # ==================================
    elif bias == "SHORT":
        if price >= ema200 or ema20 >= ema50: return None, "ema_not_aligned_short", dbg
        if rsiv > 55: return None, "rsi_too_weak_short", dbg
        if not pullback_touch_short: return None, "no_pullback_touch_ema20", dbg
        
        # Cek Fake Out Ekor Bawah (Wick Rejection) SEBELUM konfirmasi SELL
        # Jika ekor bawah lebih panjang dari bodi, artinya ada dorongan beli kuat dari bawah (bahaya untuk di-Short)
        if lower_wick > candle_body:
            return None, "fakeout_short_wick_rejected", dbg

        if (not candle_bear) or (price > ema20):
            dbg["touch"] = True
            return None, "no_bear_confirm", dbg

        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return "SELL", "scalp_pullback_short", dbg

    return None, "invalid_bias", dbg

def signal_range_mode(df5: pd.DataFrame) -> Tuple[Optional[str], str, dict]:
    idx = -2 if USE_CLOSED_CANDLE_ONLY else -1
    last = df5.iloc[idx]

    don_hi = last.get("don_hi")
    don_lo = last.get("don_lo")
    atr_val = float(last.get("atr", 0.0))
    vol_sma = float(last.get("vol_sma", 0.0))
    vol = float(last.get("volume", 0.0))
    rsi_val = float(last.get("rsi", 0.0))

    dbg = {"touch": False, "rejection": False, "confirm": False}

    if pd.isna(don_hi) or pd.isna(don_lo) or atr_val <= 0 or vol_sma <= 0:
        return None, "indicators_not_ready", dbg

    if vol < (VOL_OK_RATIO_RANGE * vol_sma):
        return None, "range_low_volume", dbg

    candle_bull = float(last["close"]) > float(last["open"])
    candle_bear = float(last["close"]) < float(last["open"])
    atr_tol = DONCHIAN_ATR_TOL * atr_val

    touch_low = float(last["low"]) <= (float(don_lo) + atr_tol)
    close_inside_low = float(last["close"]) > float(don_lo)

    if touch_low and close_inside_low and candle_bull and rsi_val >= RSI_RANGE_LONG_MIN:
        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return "BUY", "range_reject_low", dbg

    touch_high = float(last["high"]) >= (float(don_hi) - atr_tol)
    close_inside_high = float(last["close"]) < float(don_hi)

    if touch_high and close_inside_high and candle_bear and rsi_val <= RSI_RANGE_SHORT_MAX:
        dbg.update({"touch": True, "rejection": True, "confirm": True})
        return "SELL", "range_reject_high", dbg

    return None, "no_range_setup", dbg


# =========================
# EKSEKUSI & MANAJEMEN ORDER
# =========================
def place_order_with_actual_bracket(side: str, qty_q: float, atr_val: float, mode: str, mark_price: float, sl_mult: float = 1.0, tp_mult: float = 1.0):
    filters = _get_symbol_filters(SYMBOL)
    tick = float(filters["PRICE_FILTER"]["tickSize"])
    cancel_all_open_orders()

    raw_sl_dist = (atr_val * TREND_SL_ATR_MULT) if mode == "TREND" else (atr_val * RANGE_SL_ATR_MULT)
    sl_min_pct = TREND_SL_MIN_PCT if mode == "TREND" else RANGE_SL_MIN_PCT
    sl_max_pct = TREND_SL_MAX_PCT if mode == "TREND" else RANGE_SL_MAX_PCT

    sl_dist = sl_mult * max(mark_price * sl_min_pct, min(raw_sl_dist, mark_price * sl_max_pct))
    rr = (TREND_RR if mode == "TREND" else RANGE_RR) * tp_mult

    if side == "BUY":
        sl_price = mark_price - sl_dist
        tp_price = mark_price + (sl_dist * rr)
        op_side = "SELL"
    else:
        sl_price = mark_price + sl_dist
        tp_price = mark_price - (sl_dist * rr)
        op_side = "BUY"

    sl_q = _round_tick(sl_price, tick)
    tp_q = _round_tick(tp_price, tick)

    # BATCH ORDER ATOMIC PAYLOAD
    batch_payload = [
        {"symbol": SYMBOL, "side": side, "type": "MARKET", "quantity": str(qty_q)},
        {"symbol": SYMBOL, "side": op_side, "type": "STOP_MARKET", "stopPrice": str(sl_q), "closePosition": "true", "workingType": "MARK_PRICE"},
        {"symbol": SYMBOL, "side": op_side, "type": "TAKE_PROFIT_MARKET", "stopPrice": str(tp_q), "closePosition": "true", "workingType": "MARK_PRICE"}
    ]

    actual_entry = 0.0

    try:
        responses = call_with_retry(
            client.futures_place_batch_order,
            batchOrders=json.dumps(batch_payload),
            recvWindow=RECV_WINDOW
        )
        if responses and isinstance(responses, list):
            entry_order = responses[0]
            if "code" in entry_order and entry_order["code"] < 0:
                raise Exception(f"Batch Order Entry Failed: {entry_order['msg']}")
            actual_entry = float(entry_order.get("avgPrice", 0.0))
            
        if actual_entry <= 0.0: actual_entry = float(mark_price)

    except Exception as e:
        print(f"CRITICAL ERROR: Failed Batch Order. Emergency close. Error: {e}")
        try:
            cancel_all_open_orders()
            current_pos_amt = 0.0
            pos = call_with_retry(client.futures_position_information, symbol=SYMBOL, recvWindow=RECV_WINDOW)
            for p in pos or []:
                if p.get("symbol") == SYMBOL:
                    current_pos_amt = abs(float(p.get("positionAmt", 0.0)))
                    break

            if current_pos_amt > 0:
                call_with_retry(
                    client.futures_create_order,
                    symbol=SYMBOL, side=op_side, type="MARKET", quantity=current_pos_amt, reduceOnly=True, recvWindow=RECV_WINDOW
                )
            send_telegram(f"🚨 EMERGENCY: Gagal eksekusi Batch Order. Posisi DITUTUP OTOMATIS! Err: {e}")
            st_temp = load_state({})
            st_temp["cooldown_until"] = _dt_to_iso(datetime.now(timezone.utc) + pd.Timedelta(minutes=COOLDOWN_MINUTES))
            save_state(st_temp)
        except Exception as ex:
            send_telegram(f"💀 FATAL: Gagal eksekusi Batch dan gagal close. CEK BINANCE MANUAL. Err: {repr(ex)}")
        raise

    return actual_entry, sl_q, tp_q, sl_dist

def manage_break_even(st, mark_price, tick_size, qty_q):
    if not ENABLE_BREAK_EVEN or st.get("be_activated", False) or st.get("be_failed_once", False): return

    entry_price = float(st.get("entry_price", 0.0))
    sl_dist = float(st.get("sl_dist_actual", 0.0))
    side = st.get("pos_side", "")

    if entry_price <= 0 or sl_dist <= 0: return

    if side == "LONG": profit_r = (mark_price - entry_price) / sl_dist
    elif side == "SHORT": profit_r = (entry_price - mark_price) / sl_dist
    else: return

    if profit_r < BE_ACTIVATION_RR: return

    try:
        open_orders = call_with_retry(client.futures_get_open_orders, symbol=SYMBOL, recvWindow=RECV_WINDOW)
        stop_order_id = next((o.get("orderId") for o in open_orders if o.get("type") == "STOP_MARKET"), None)
        old_stop_price = next((float(o.get("stopPrice", 0.0)) for o in open_orders if o.get("type") == "STOP_MARKET"), None)

        op_side = "SELL" if side == "LONG" else "BUY"
        be_price = entry_price * (1 + BE_BUFFER_PCT) if side == "LONG" else entry_price * (1 - BE_BUFFER_PCT)
        be_price_q = _round_tick(be_price, tick_size)

        if stop_order_id:
            call_with_retry(client.futures_cancel_order, symbol=SYMBOL, orderId=stop_order_id, recvWindow=RECV_WINDOW)

        try:
            new_be_order = call_with_retry(
                client.futures_create_order,
                symbol=SYMBOL, side=op_side, type="STOP_MARKET", stopPrice=be_price_q, closePosition=True, workingType="MARK_PRICE", recvWindow=RECV_WINDOW
            )
            if new_be_order and "orderId" in new_be_order:
                st["be_activated"] = True
                save_state(st)
                send_telegram(f"🛡️ {SYMBOL} Break-Even Activated!\nProfit capai {BE_ACTIVATION_RR}R.\nSL aman di: {be_price_q}")
                return
        except Exception as be_err:
            if old_stop_price and old_stop_price > 0:
                try:
                    call_with_retry(
                        client.futures_create_order,
                        symbol=SYMBOL, side=op_side, type="STOP_MARKET", stopPrice=_round_tick(old_stop_price, tick_size), closePosition=True, workingType="MARK_PRICE", recvWindow=RECV_WINDOW
                    )
                except Exception as restore_err:
                    send_telegram(f"🚨 FATAL BE: gagal pasang BE dan gagal restore SL lama.\nBE Err: {be_err}\nRestore Err: {restore_err}")
            st["be_failed_once"] = True; save_state(st)
    except Exception as e:
        st["be_failed_once"] = True; save_state(st)


# =========================
# MAIN LOOP
# =========================
def main():
    try:
        call_with_retry(client.futures_change_leverage, symbol=SYMBOL, leverage=LEVERAGE, recvWindow=RECV_WINDOW)
    except:
        pass

    min_notional = get_min_notional(SYMBOL)
    tick_size = float(_get_symbol_filters(SYMBOL)["PRICE_FILTER"]["tickSize"])

    # --- INISIALISASI AWAL ---
    send_telegram(
        f"🟢 {TG_PREFIX} started\n"
        f"Eq: ${get_wallet_balance_quote():.2f}\n"
        f"Mode: Ultimate Sniper {SYMBOL} + Vol Regime + WebSocket"
    )

    st = load_state({
        "day_key": datetime.now(timezone.utc).date().isoformat(),
        "start_equity_today": get_wallet_balance_quote(),
        "daily_realized_pnl": 0.0, "trades_today": 0, "loss_streak": 0,
        "daily_locked": False, "cooldown_until": None, "mode": "RANGE",
        "prev_in_position": False, "last_pnl_check_ms": int(time.time() * 1000) - 60_000,
        "position_open_ms": 0, "seen_tran_ids": set(),
        "entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": "",
        "be_activated": False, "be_failed_once": False, "qty_q": 0.0,
        "force_test_done": False, "margin_trap_alert_sent": False,
    })

    # Variabel Kontrol Loop
    last_time_sync = time.time()
    last_rest_check = 0
    last_health_check = time.time()
    TIME_SYNC_EVERY_S = 30 * 60
    REST_INTERVAL = 3.0  
    HEALTH_CHECK_S = 1800 # 30 Menit

    # NYALAKAN WEBSOCKET
    streamer = BinanceStreamer(
        api_key=os.getenv("BINANCE_TESTNET_API_KEY"),
        api_secret=os.getenv("BINANCE_TESTNET_API_SECRET"),
        symbol=SYMBOL, tf_entry=TF_ENTRY
    )
    streamer.start()
    time.sleep(2)

    try:
        while True:
            now = datetime.now(timezone.utc)
            
            # --- FAILSAFE HARGA ---
            # Kita coba ambil dari WebSocket dulu (paling cepat)
            current_mark_price = streamer.mark_price
            
            # Jika WebSocket macet (0.0), kita paksa ambil lewat REST API
            if current_mark_price <= 0:
                try:
                    current_mark_price = get_mark_price()
                    # Optional: print agar kamu tahu di log kalau sedang pakai REST
                    # print(f"[{datetime.now()}] ⚠️ WebSocket Delay, using REST: {current_mark_price}")
                except Exception as e:
                    print(f"[{datetime.now()}] 🚨 Fatal: API Error saat ambil harga: {e}")
                    time.sleep(2)
                    continue

            # Debugging agar kamu bisa pantau di PM2 Logs
            if int(time.time()) % 10 == 0: # Print setiap 10 detik biar gak spam
                print(f"[{datetime.now()}] Mode: {st['mode']} | Price: {current_mark_price} | Pos: {st['prev_in_position']}")

            # PENTING: Untuk logika di bawah ini, SELALU gunakan current_mark_price
            # ==========================================
            # 1. LOOP SUPER CEPAT (Real-time Break-Even)
            # ==========================================
            if st.get("prev_in_position") and current_mark_price > 0:
                # Sinkronisasi entry_price jika zombie state (harga 0 di state)
                if float(st.get("entry_price", 0.0)) <= 0:
                     pos_amt = get_position_amt()
                     if abs(pos_amt) > 0:
                         pos_info = call_with_retry(client.futures_position_information, symbol=SYMBOL)
                         st["entry_price"] = float(pos_info[0].get("entryPrice", current_mark_price))
                         save_state(st)
                
                # Gunakan current_mark_price di sini
                manage_break_even(st, current_mark_price, tick_size, st.get("qty_q", 0.0))
            
            # ==========================================
            # 1. LOOP SUPER CEPAT (Real-time Break-Even)
            # ==========================================
            if st.get("prev_in_position") and current_mark_price > 0:
                # Failsafe: Jika state mencatat in_position tapi entry_price 0, sinkronkan ulang
                if float(st.get("entry_price", 0.0)) <= 0:
                     pos_amt = get_position_amt()
                     if abs(pos_amt) > 0:
                         # Ambil entry price langsung dari API Binance (REST)
                         pos_info = call_with_retry(client.futures_position_information, symbol=SYMBOL)
                         st["entry_price"] = float(pos_info[0].get("entryPrice", current_mark_price))
                         save_state(st)
                
                manage_break_even(st, current_mark_price, tick_size, st.get("qty_q", 0.0))

            # ==========================================
            # 2. LOOP MENENGAH (Cek Status Posisi & Saldo)
            # ==========================================
            if time.time() - last_rest_check >= REST_INTERVAL:
                last_rest_check = time.time()
                
                if (time.time() - last_time_sync) >= TIME_SYNC_EVERY_S:
                    if sync_time_offset(): last_time_sync = time.time()
                    
                    check_telegram_commands(st)

                # --- HEALTH CHECK TELEGRAM ---
                if time.time() - last_health_check >= HEALTH_CHECK_S:
                    last_health_check = time.time()
                    eq_check = get_wallet_balance_quote()
                    send_telegram(
                        f"🤖 {SYMBOL} Health Check\n"
                        f"Equity: ${eq_check:.2f} | PnL Day: ${st['daily_realized_pnl']:.2f}\n"
                        f"Trades: {st['trades_today']} | Status: {'🟢 In Pos' if st['prev_in_position'] else '⚪ Idle'}"
                    )

                cur_day = now.date().isoformat()
                if cur_day != st.get("day_key"):
                    st.update({
                        "day_key": cur_day, "start_equity_today": get_wallet_balance_quote(),
                        "daily_realized_pnl": 0.0, "trades_today": 0, "loss_streak": 0,
                        "daily_locked": False, "cooldown_until": None, "force_test_done": False,
                        "margin_trap_alert_sent": False
                    })
                    send_telegram(f"🗓 {SYMBOL} Day reset. Start equity: ${st['start_equity_today']:.2f}")
                    save_state(st)

                equity_now = get_wallet_balance_quote()
                pos_amt = get_position_amt()
                in_pos = abs(pos_amt) > 0

                # Logika: Sinkronisasi State vs Realita Binance (Anti-Zombie)
                if in_pos and not st.get("prev_in_position"):
                    st["prev_in_position"] = True
                    st["position_open_ms"] = int(time.time() * 1000)
                    save_state(st)
                
                elif not in_pos and st.get("prev_in_position"):
                    # Jika realita tidak ada posisi tapi state mencatat ada, anggap trade sudah tutup
                    pnl_start_ms = int(st.get("position_open_ms", 0)) or int(st.get("last_pnl_check_ms", 0))
                    pnl, got_any = get_closed_trade_pnl_with_retry(pnl_start_ms, st["seen_tran_ids"], retries=3, wait_sec=2)
                    
                    st["last_pnl_check_ms"] = int(time.time() * 1000)
                    if got_any:
                        st["daily_realized_pnl"] = float(st.get("daily_realized_pnl", 0.0)) + pnl
                        st["loss_streak"] = int(st.get("loss_streak", 0)) + 1 if pnl < 0 else 0
                        send_telegram(f"✅ {SYMBOL} Trade Closed | PnL: ${pnl:.4f} | Streak: {st['loss_streak']}")
                    
                    st.update({
                        "entry_price": 0.0, "sl_dist_actual": 0.0, "pos_side": "",
                        "be_activated": False, "be_failed_once": False, "qty_q": 0.0,
                        "position_open_ms": 0, "prev_in_position": False
                    })
                    
                    if st["loss_streak"] >= LOSS_STREAK_LIMIT:
                        st["daily_locked"] = True
                        send_telegram(f"🧯 {SYMBOL} LOSS STREAK LIMIT! Locked until tomorrow.")
                    else:
                        st["cooldown_until"] = _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES))
                    save_state(st)

            # ==========================================
            # 3. LOOP ENTRY (HANYA SAAT CANDLE TUTUP)
            # ==========================================
            if streamer.candle_closed:
                streamer.candle_closed = False
                
                if st.get("prev_in_position") or st.get("daily_locked"): continue

                total_daily_pnl = float(st.get("daily_realized_pnl", 0.0))
                start_eq = float(st.get("start_equity_today", 0.0)) or 0.0
                daily_dd = (total_daily_pnl / start_eq) if start_eq > 0 else 0.0

                if daily_dd <= -abs(MAX_DAILY_DRAWDOWN_PCT):
                    st["daily_locked"] = True
                    send_telegram(f"🛑 {SYMBOL} DAILY STOP! DD: {daily_dd*100:.2f}%")
                    save_state(st)
                    continue

                if int(st.get("trades_today", 0)) >= MAX_TRADES_PER_DAY:
                    st["daily_locked"] = True
                    send_telegram(f"⚠️ {SYMBOL} Trade Limit Reached. Locked.")
                    save_state(st)
                    continue

                cdt = _iso_to_dt(st.get("cooldown_until"))
                if cdt and now < cdt: continue

                # ANALISA PASAR
                st["mode"], bias, chop15, df15 = compute_regime_and_bias(st.get("mode", "RANGE"))
                vol_regime, atrp, low_th, high_th = get_vol_regime_15m(df15)
                df5, dbg5 = compute_entry_indicators_5m()
                
                if df5 is None or len(df5) < 3: continue

                last_closed = _last_closed(df5)
                price = current_mark_price if current_mark_price > 0 else float(last_closed["close"])

                # Logika Force Test Entry
                if FORCE_TEST_ENTRY and not st.get("force_test_done", False):
                    step_size = float(_get_symbol_filters(SYMBOL)["LOT_SIZE"]["stepSize"])
                    qty_q = _quantize_step((min_notional * 1.1) / price, step_size)
                    try:
                        actual_price, sl_final, tp_final, sl_dist_actual = place_order_with_actual_bracket(FORCE_TEST_SIDE, qty_q, float(last_closed["atr"]), st["mode"], price, 1.0, 1.0)
                        st.update({"trades_today": int(st.get("trades_today", 0)) + 1, "force_test_done": True})
                        save_state(st)
                    except Exception as e: print(f"Force test entry gagal: {e}")
                    continue

                # DETEKSI SINYAL
                side = None
                reason = "no_setup"
                
                if st["mode"] == "TREND":
                    side, reason, sig_dbg = signal_trend_mode(df5, bias)
                elif st["mode"] == "RANGE" and ENABLE_RANGE_MODE:
                    side, reason, sig_dbg = signal_range_mode(df5)

                if vol_regime == "LOW_VOL" and reason in ("scalp_pullback_long", "scalp_pullback_short"):
                    side, reason = None, "blocked_low_vol"

                dbg = {
                    "price": price, "chop15": float(chop15), "ema20": float(dbg5.get("ema_fast", 0.0)),
                    "ema50": float(dbg5.get("ema_slow", 0.0)), "ema200": float(dbg5.get("ema_200", 0.0)),
                    "rsi5": float(dbg5.get("rsi5", 0.0)), "atr5": float(dbg5.get("atr5", 0.0)),
                    "vol": float(last_closed.get("volume", 0.0)), "vol_sma": float(dbg5.get("vol_sma", 0.0)),
                    "don_hi": float(dbg5.get("don_hi", 0.0)) if dbg5.get("don_hi") else 0.0,
                    "don_lo": float(dbg5.get("don_lo", 0.0)) if dbg5.get("don_lo") else 0.0,
                    "vol_regime": vol_regime, "atr_pct15": atrp, "atrp_low": low_th, "atrp_high": high_th
                }
                log_loop(now, equity_now, st["mode"], bias, reason, dbg)

                if side is not None:
                    sl_mult = HIGH_VOL_WIDEN_SL_MULT if vol_regime == "HIGH_VOL" else 1.0
                    tp_mult = HIGH_VOL_TP_MULT if vol_regime == "HIGH_VOL" else 1.0

                    atr_val = float(last_closed["atr"])
                    raw_sl_dist = (atr_val * TREND_SL_ATR_MULT) if st["mode"] == "TREND" else (atr_val * RANGE_SL_ATR_MULT)
                    sl_min_pct = TREND_SL_MIN_PCT if st["mode"] == "TREND" else RANGE_SL_MIN_PCT
                    sl_max_pct = TREND_SL_MAX_PCT if st["mode"] == "TREND" else RANGE_SL_MAX_PCT

                    sl_dist_est = sl_mult * max(price * sl_min_pct, min(raw_sl_dist, price * sl_max_pct))
                    risk_pct_used = get_risk_pct_by_vol_regime(vol_regime)
                    risk_usd = equity_now * risk_pct_used

                    qty = (risk_usd / sl_dist_est) if sl_dist_est > 0 else 0.0
                    step_size = float(_get_symbol_filters(SYMBOL)["LOT_SIZE"]["stepSize"])
                    qty_q = _quantize_step(qty, step_size)
                    notional_q = qty_q * price

                    if notional_q < min_notional:
                        qty_q = _quantize_step((min_notional * 1.02) / price, step_size)
                        notional_q = qty_q * price
                        if ((qty_q * sl_dist_est) / max(equity_now, 1e-9)) > MAX_FORCED_RISK_PCT:
                            if not st.get("margin_trap_alert_sent", False):
                                send_telegram(f"🪤 JEBAKAN MODAL: Risk capai {MAX_FORCED_RISK_PCT*100}%. Skip entry.")
                                st["margin_trap_alert_sent"] = True; save_state(st)
                            continue
                        else:
                            st["margin_trap_alert_sent"] = False

                    est_fee = (notional_q * TAKER_FEE_PCT) * 2.0
                    rr = (TREND_RR if st["mode"] == "TREND" else RANGE_RR) * tp_mult

                    if notional_q >= min_notional and qty_q > 0 and (qty_q * (sl_dist_est * rr)) > est_fee * 3.0:
                        actual_price, sl_final, tp_final, sl_dist_actual = place_order_with_actual_bracket(
                            side, qty_q, atr_val, st["mode"], price, sl_mult, tp_mult
                        )

                        st.update({
                            "trades_today": int(st.get("trades_today", 0)) + 1,
                            "cooldown_until": _dt_to_iso(now + pd.Timedelta(minutes=COOLDOWN_MINUTES)),
                            "entry_price": actual_price, "sl_dist_actual": sl_dist_actual,
                            "pos_side": "LONG" if side == "BUY" else "SHORT", "qty_q": qty_q,
                            "be_activated": False, "be_failed_once": False,
                            "position_open_ms": int(time.time() * 1000), "margin_trap_alert_sent": False
                        })
                        save_state(st)

                        send_telegram(
                            f"🚀 ENTRY {SYMBOL} {side}\n"
                            f"Mode: {st['mode']} ({vol_regime})\n"
                            f"Risk: {risk_pct_used*100:.2f}%\n"
                            f"Qty: {qty_q} | Entry: {actual_price:.2f}\n"
                            f"SL: {sl_final} | TP: {tp_final}\n"
                            f"Reason: {reason}"
                        )

            time.sleep(0.1)

    except Exception as e:
        print(f"Loop Error: {e}")
        traceback.print_exc()
    finally:
        streamer.stop()
        
if __name__ == "__main__":
    main()