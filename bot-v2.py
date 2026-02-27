import os
import time
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from decimal import Decimal, ROUND_DOWN, ROUND_CEILING
from datetime import datetime, timezone

# ======================
# CONFIG (BOT V2 FULL)
# ======================
SYMBOL = "BTCUSDT"

# Timeframes
TF_TREND = "4h"   # bias + structure + fib swing
TF_ENTRY = "1h"   # entry confirmation + execution timing

# Trend filter
EMA_TREND_LEN = 200  # EMA200 on 4H

# RSI confirmation (1H)
RSI_LEN = 14
RSI_LONG_MIN = 45   # long confirm when RSI rising and >= this
RSI_SHORT_MAX = 55  # short confirm when RSI falling and <= this

# Risk & Bracket
LEVERAGE = 5
RISK_PCT = 0.02        # 2% equity risk per trade
SL_PCT = 0.05          # 5% SL from entry price
TP_MULT = 3.0          # RR 1:3 => TP = 15% if SL=5%

# Global Kill Switch (equity-based, from start_equity)
TARGET_EQUITY_PCT = 0.20  # +20%
MAX_DRAWDOWN_PCT = 0.10   # -10%

# Fibonacci entry zone
FIB_MIN = 0.50
FIB_MAX = 0.705

# Pivot detection for structure / swing range on 4H
PIVOT_LEFT = 3
PIVOT_RIGHT = 3

# Exchange constraints
MIN_NOTIONAL_USD = 100.0

# Anti-spam
SLEEP_SECONDS = 60

# Safety add-ons (Option B)
COOLDOWN_HOURS = 12
MAX_NOTIONAL_CAP_RATIO = 0.25  # cap notional at 25% of (equity * leverage)

# Testnet endpoint (UM Futures)
FUTURES_TESTNET_URL = "https://testnet.binancefuture.com"

# ======================
# SETUP CLIENT
# ======================
load_dotenv()
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise SystemExit("API key/secret belum kebaca. Pastikan file .env sudah benar.")

client = Client(API_KEY, API_SECRET, testnet=True)
client.FUTURES_URL = FUTURES_TESTNET_URL

# ======================
# EXCHANGE FILTERS (cache)
# ======================
_symbol_filters_cache = None

def get_symbol_filters(symbol: str):
    global _symbol_filters_cache
    if _symbol_filters_cache is not None:
        return _symbol_filters_cache

    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            filters = {f["filterType"]: f for f in s["filters"]}
            _symbol_filters_cache = filters

            print("Symbol:", symbol)
            print("LOT_SIZE stepSize:", filters["LOT_SIZE"]["stepSize"], "minQty:", filters["LOT_SIZE"]["minQty"])
            print("PRICE_FILTER tickSize:", filters["PRICE_FILTER"]["tickSize"])
            return filters

    raise RuntimeError("Symbol not found in futures_exchange_info()")

def quantize_down(value: float, step: str) -> float:
    v = Decimal(str(value))
    s = Decimal(step)
    q = (v / s).to_integral_value(rounding=ROUND_DOWN) * s
    return float(q)

def quantize_up(value: float, step: str) -> float:
    v = Decimal(str(value))
    s = Decimal(step)
    q = (v / s).to_integral_value(rounding=ROUND_CEILING) * s
    return float(q)

# ======================
# HELPERS: EXCHANGE & DATA
# ======================
def set_leverage() -> None:
    client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)

def get_mark_price() -> float:
    mp = client.futures_mark_price(symbol=SYMBOL)
    return float(mp["markPrice"])

def get_wallet_balance() -> float:
    acc = client.futures_account()
    return float(acc.get("totalWalletBalance", 0.0))

def get_klines(symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
    klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base", "taker_quote", "ignore"
    ])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype(float)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# ======================
# POSITION / ORDERS
# ======================
def get_position_amt() -> float:
    positions = client.futures_position_information(symbol=SYMBOL)
    if not positions:
        return 0.0
    for p in positions:
        return float(p.get("positionAmt", 0.0))
    return 0.0

def has_open_position() -> bool:
    return get_position_amt() != 0.0

def cancel_all_open_orders() -> None:
    try:
        orders = client.futures_get_open_orders(symbol=SYMBOL)
        if orders:
            client.futures_cancel_all_open_orders(symbol=SYMBOL)
            print("Cancel: open orders cleared.")
    except Exception as e:
        print("WARN cancel_all_open_orders:", e)

def close_position_market() -> None:
    """
    Close any open position at MARKET (reduceOnly).
    Useful for global stop/target hit.
    """
    amt = get_position_amt()
    if amt == 0.0:
        return

    side = "SELL" if amt > 0 else "BUY"
    qty = abs(amt)

    filters = get_symbol_filters(SYMBOL)
    step_size = filters["LOT_SIZE"]["stepSize"]
    qty = quantize_down(qty, step_size)

    if qty <= 0:
        return

    try:
        client.futures_create_order(
            symbol=SYMBOL,
            side=side,
            type="MARKET",
            quantity=qty,
            reduceOnly=True
        )
        print("Position closed at MARKET. qty =", qty)
    except Exception as e:
        print("ERROR close_position_market:", e)

# ======================
# STRATEGY: PIVOTS, STRUCTURE, FIB
# ======================
def detect_pivots(df: pd.DataFrame, left: int, right: int):
    """
    Simple pivot detection:
    pivot_high at i if high[i] is max in [i-left, i+right]
    pivot_low  at i if low[i]  is min in [i-left, i+right]
    """
    pivots = []
    highs = df["high"].values
    lows = df["low"].values

    for i in range(left, len(df) - right):
        window_high = highs[i - left: i + right + 1]
        window_low = lows[i - left: i + right + 1]

        if highs[i] == window_high.max():
            pivots.append({
                "i": i,
                "type": "H",
                "price": float(highs[i]),
                "time": df.iloc[i]["close_time"]
            })
        if lows[i] == window_low.min():
            pivots.append({
                "i": i,
                "type": "L",
                "price": float(lows[i]),
                "time": df.iloc[i]["close_time"]
            })

    pivots.sort(key=lambda x: x["i"])
    return pivots

def last_two(pivots, t):
    xs = [p for p in pivots if p["type"] == t]
    if len(xs) < 2:
        return None, None
    return xs[-2], xs[-1]

def structure_bias_4h(df4h: pd.DataFrame):
    """
    Returns bias: "LONG", "SHORT", or "NONE"
    using EMA200 + HH/HL or LH/LL on recent pivots.
    """
    df = df4h.copy()
    df["ema200"] = ema(df["close"], EMA_TREND_LEN)

    last_closed = df.iloc[-2]  # 4H candle last closed
    close_4h = float(last_closed["close"])
    ema200 = float(last_closed["ema200"])

    ema_bias = "LONG" if close_4h > ema200 else "SHORT" if close_4h < ema200 else "NONE"
    if ema_bias == "NONE":
        return "NONE", {"close_4h": close_4h, "ema200_4h": ema200, "ema_bias": ema_bias}

    pivots = detect_pivots(df.iloc[:-1], PIVOT_LEFT, PIVOT_RIGHT)  # exclude forming candle
    h1, h2 = last_two(pivots, "H")
    l1, l2 = last_two(pivots, "L")

    info = {
        "close_4h": close_4h,
        "ema200_4h": ema200,
        "ema_bias": ema_bias,
        "pivot_h_prev": h1,
        "pivot_h_last": h2,
        "pivot_l_prev": l1,
        "pivot_l_last": l2
    }

    if not h1 or not h2 or not l1 or not l2:
        return "NONE", info

    is_up = (h2["price"] > h1["price"]) and (l2["price"] > l1["price"])
    is_down = (h2["price"] < h1["price"]) and (l2["price"] < l1["price"])

    if ema_bias == "LONG" and is_up:
        return "LONG", info
    if ema_bias == "SHORT" and is_down:
        return "SHORT", info

    return "NONE", info

def get_last_swing_for_fib(df4h: pd.DataFrame, direction: str):
    """
    For LONG: last swing low -> swing high (L then H after it)
    For SHORT: last swing high -> swing low (H then L after it)
    Returns (A, B) prices for fib draw A->B:
      LONG: A=low, B=high
      SHORT: A=high, B=low
    """
    pivots = detect_pivots(df4h.iloc[:-1], PIVOT_LEFT, PIVOT_RIGHT)

    if direction == "LONG":
        lows = [p for p in pivots if p["type"] == "L"]
        if not lows:
            return None
        last_low = lows[-1]
        highs_after = [p for p in pivots if p["type"] == "H" and p["i"] > last_low["i"]]
        if not highs_after:
            return None
        last_high = highs_after[-1]
        return (last_low["price"], last_high["price"])

    if direction == "SHORT":
        highs = [p for p in pivots if p["type"] == "H"]
        if not highs:
            return None
        last_high = highs[-1]
        lows_after = [p for p in pivots if p["type"] == "L" and p["i"] > last_high["i"]]
        if not lows_after:
            return None
        last_low = lows_after[-1]
        return (last_high["price"], last_low["price"])

    return None

def in_fib_zone(direction: str, swing_a: float, swing_b: float, price: float) -> bool:
    """
    Check whether current price is inside fib zone 0.5–0.705.
    LONG: A=low, B=high. Retracement from B downward.
    SHORT: A=high, B=low. Retracement from B upward.
    """
    if direction == "LONG":
        low = swing_a
        high = swing_b
        if high <= low:
            return False
        lvl_50 = high - (high - low) * FIB_MIN
        lvl_705 = high - (high - low) * FIB_MAX
        zone_low = min(lvl_50, lvl_705)
        zone_high = max(lvl_50, lvl_705)
        return zone_low <= price <= zone_high

    if direction == "SHORT":
        high = swing_a
        low = swing_b
        if high <= low:
            return False
        lvl_50 = low + (high - low) * FIB_MIN
        lvl_705 = low + (high - low) * FIB_MAX
        zone_low = min(lvl_50, lvl_705)
        zone_high = max(lvl_50, lvl_705)
        return zone_low <= price <= zone_high

    return False

def rejection_confirm_1h(df1h: pd.DataFrame, direction: str) -> bool:
    """
    1H confirmation (simple, robust):
    - Rejection wick on last closed 1H candle
    - RSI turning in the right direction (confirmation, not main trigger)
    """
    df = df1h.copy()
    df["rsi"] = rsi(df["close"], RSI_LEN)

    c = df.iloc[-2]  # last closed
    o = float(c["open"])
    h = float(c["high"])
    l = float(c["low"])
    cl = float(c["close"])
    r = float(c["rsi"]) if pd.notna(c["rsi"]) else None

    body = abs(cl - o)
    upper_wick = h - max(o, cl)
    lower_wick = min(o, cl) - l

    r_prev = float(df.iloc[-3]["rsi"]) if pd.notna(df.iloc[-3]["rsi"]) else None
    if r is None or r_prev is None:
        return False

    # Avoid division by zero weirdness: require some body
    if body <= 0:
        body = 1e-9

    if direction == "LONG":
        wick_ok = lower_wick > body * 1.2
        close_ok = cl >= o
        rsi_ok = (r >= r_prev) and (r >= RSI_LONG_MIN)
        return wick_ok and close_ok and rsi_ok

    if direction == "SHORT":
        wick_ok = upper_wick > body * 1.2
        close_ok = cl <= o
        rsi_ok = (r <= r_prev) and (r <= RSI_SHORT_MAX)
        return wick_ok and close_ok and rsi_ok

    return False

# ======================
# SIZING & BRACKET
# ======================
def calc_qty_risk_based(entry_price: float) -> float:
    """
    Risk-based sizing with safety cap:
      risk_usd = equity * 2%
      SL = 5% => notional = risk_usd / 0.05
      cap notional at equity * leverage * MAX_NOTIONAL_CAP_RATIO
    """
    equity = get_wallet_balance()
    risk_usd = equity * RISK_PCT
    notional = risk_usd / SL_PCT

    # Safety cap (prevents oversized positions due to glitches)
    max_notional = equity * LEVERAGE * MAX_NOTIONAL_CAP_RATIO
    if notional > max_notional:
        print("WARN notional capped:", round(notional, 2), "->", round(max_notional, 2))
        notional = max_notional

    # Enforce minimum notional
    if notional < MIN_NOTIONAL_USD:
        notional = MIN_NOTIONAL_USD

    filters = get_symbol_filters(SYMBOL)
    step_size = filters["LOT_SIZE"]["stepSize"]
    min_qty = float(filters["LOT_SIZE"]["minQty"])

    raw_qty = notional / entry_price
    qty = quantize_up(raw_qty, step_size)

    if qty < min_qty:
        qty = min_qty

    # Ensure approx min notional
    if qty * entry_price < MIN_NOTIONAL_USD:
        qty = quantize_up(MIN_NOTIONAL_USD / entry_price, step_size)

    return qty

def place_entry_and_bracket(direction: str, qty: float, entry_price: float) -> None:
    """
    Entry MARKET + SL/TP with closePosition=True.
    SL = 5%, TP = 15% (RR 1:3).
    """
    if direction == "LONG":
        entry_side = "BUY"
        exit_side = "SELL"
        sl_price = entry_price * (1 - SL_PCT)
        tp_price = entry_price * (1 + SL_PCT * TP_MULT)
    else:
        entry_side = "SELL"
        exit_side = "BUY"
        sl_price = entry_price * (1 + SL_PCT)
        tp_price = entry_price * (1 - SL_PCT * TP_MULT)

    filters = get_symbol_filters(SYMBOL)
    tick = filters["PRICE_FILTER"]["tickSize"]

    sl_price = quantize_down(sl_price, tick)
    tp_price = quantize_down(tp_price, tick)

    # Clear leftovers before entry
    cancel_all_open_orders()

    # Entry
    client.futures_create_order(
        symbol=SYMBOL,
        side=entry_side,
        type="MARKET",
        quantity=qty
    )

    # Stop Loss
    client.futures_create_order(
        symbol=SYMBOL,
        side=exit_side,
        type="STOP_MARKET",
        stopPrice=sl_price,
        closePosition=True
    )

    # Take Profit
    client.futures_create_order(
        symbol=SYMBOL,
        side=exit_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=tp_price,
        closePosition=True
    )

    print("BRACKET SET | SL:", sl_price, "| TP:", tp_price)

# ======================
# MAIN LOOP
# ======================
def main() -> None:
    set_leverage()
    start_equity = get_wallet_balance()

    target_equity = start_equity * (1 + TARGET_EQUITY_PCT)
    stop_equity = start_equity * (1 - MAX_DRAWDOWN_PCT)

    print("BOT V2 START (TESTNET)", SYMBOL)
    print("Start equity:", round(start_equity, 2))
    print("Global target equity:", round(target_equity, 2), "| Global stop equity:", round(stop_equity, 2))
    print("Risk/trade:", int(RISK_PCT * 100), "% | SL:", int(SL_PCT * 100), "% | TP:", int(SL_PCT * TP_MULT * 100), "%")
    print("Cooldown:", COOLDOWN_HOURS, "hours | Notional cap ratio:", MAX_NOTIONAL_CAP_RATIO)

    last_1h_close_time = None
    cooldown_until = None
    prev_in_position = has_open_position()

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            equity_now = get_wallet_balance()

            # 1) Global kill switch (equity based)
            if equity_now >= target_equity:
                print(now_utc, "GLOBAL TARGET HIT. equity_now =", round(equity_now, 2))
                cancel_all_open_orders()
                close_position_market()
                cancel_all_open_orders()
                break

            if equity_now <= stop_equity:
                print(now_utc, "GLOBAL STOP HIT. equity_now =", round(equity_now, 2))
                cancel_all_open_orders()
                close_position_market()
                cancel_all_open_orders()
                break

            # 2) Cooldown tracking (start cooldown when a position just closed)
            in_pos = has_open_position()
            if prev_in_position and not in_pos:
                cooldown_until = now_utc + pd.Timedelta(hours=COOLDOWN_HOURS)
                print(now_utc, "COOLDOWN START until", cooldown_until)
            prev_in_position = in_pos

            # 3) Fetch 1H and only run once per closed 1H candle
            df1h = get_klines(SYMBOL, TF_ENTRY, limit=300)
            last_closed_1h = df1h.iloc[-2]
            close_time_1h = last_closed_1h["close_time"]

            if last_1h_close_time == close_time_1h:
                time.sleep(SLEEP_SECONDS)
                continue
            last_1h_close_time = close_time_1h

            # If still in cooldown, skip entries
            if cooldown_until is not None and now_utc < cooldown_until:
                print(close_time_1h, "In cooldown. Skip entry. equity =", round(equity_now, 2))
                time.sleep(SLEEP_SECONDS)
                continue

            # If in position, do nothing (bracket handles exit)
            if in_pos:
                amt = get_position_amt()
                print(close_time_1h, "In position:", amt, "| equity:", round(equity_now, 2), "Skip.")
                time.sleep(SLEEP_SECONDS)
                continue

            # Optional: keep book clean when flat
            cancel_all_open_orders()

            # 4) Determine bias + structure on 4H
            df4h = get_klines(SYMBOL, TF_TREND, limit=500)
            bias, info = structure_bias_4h(df4h)

            print(
                close_time_1h,
                "equity:", round(equity_now, 2),
                "| 4H bias:", bias,
                "| 4H close:", round(info.get("close_4h", 0.0), 2),
                "| EMA200:", round(info.get("ema200_4h", 0.0), 2),
            )

            if bias == "NONE":
                time.sleep(SLEEP_SECONDS)
                continue

            # 5) Build fib from last swing
            swing = get_last_swing_for_fib(df4h, bias)
            if not swing:
                print("No valid swing for fib. Skip.")
                time.sleep(SLEEP_SECONDS)
                continue
            swing_a, swing_b = swing

            # 6) Must be inside fib zone
            price = get_mark_price()
            if not in_fib_zone(bias, swing_a, swing_b, price):
                print(
                    "Price not in fib zone. mark =", round(price, 2),
                    "| swingA:", round(swing_a, 2),
                    "swingB:", round(swing_b, 2)
                )
                time.sleep(SLEEP_SECONDS)
                continue

            # 7) 1H confirmation
            if not rejection_confirm_1h(df1h, bias):
                print("1H confirmation not met. Skip.")
                time.sleep(SLEEP_SECONDS)
                continue

            # 8) Entry with risk-based sizing (with cap)
            qty = calc_qty_risk_based(price)
            approx_notional = qty * price
            print(close_time_1h, "ENTRY", bias, "| qty:", qty, "| mark:", round(price, 2), "| approx_notional:", round(approx_notional, 2))

            place_entry_and_bracket(bias, qty, price)
            print("ENTRY OK. Waiting next candle...")

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print("ERROR:", e)
            time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()