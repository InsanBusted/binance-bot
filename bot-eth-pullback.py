import os
import time
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from decimal import Decimal, ROUND_DOWN, ROUND_CEILING
from datetime import datetime, timezone

# ======================
# CONFIG (ETH PULLBACK SCALP - HIGH WINRATE)
# ======================
SYMBOL = "ETHUSDT"

# Timeframes
TF_TREND = "1h"
TF_ENTRY = "15m"

# Trend filter
EMA_TREND_LEN = 200  # EMA200 on 1H

# Entry indicators (15m)
EMA_FAST = 20
EMA_SLOW = 50
RSI_LEN = 14

# Pullback rules
RSI_PULL_MIN = 45
RSI_PULL_MAX = 55
PULLBACK_MAX_DIST = 0.0018  # 0.18% max distance from EMA20 to qualify as "pullback near EMA20"

# Leverage
LEVERAGE = 5

# Risk-based sizing (lebih aman untuk scalping)
RISK_PCT = 0.0075  # 0.75% equity risk per trade

# ATR-based SL/TP (15m) - clamp biar scalping
ATR_LEN = 14
MIN_SL_PCT = 0.006   # 0.6%
MAX_SL_PCT = 0.012   # 1.2%
TP_R_MULT = 1.5      # TP = 1.5R (enak untuk winrate 55–60%)

# Exchange constraints
MIN_NOTIONAL_USD = 100.0

# Safety
MAX_TRADES_PER_DAY = 12
COOLDOWN_MINUTES = 45
MAX_DAILY_DRAWDOWN_PCT = 0.03  # stop harian -3% equity
MAX_NOTIONAL_CAP_RATIO = 0.20  # cap notional at 20% of (equity * leverage)

SLEEP_SECONDS = 30

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
# EXCHANGE FILTERS (cache per symbol)
# ======================
_symbol_filters_cache = {}

def get_symbol_filters(symbol: str):
    if symbol in _symbol_filters_cache:
        return _symbol_filters_cache[symbol]

    info = client.futures_exchange_info()
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            filters = {f["filterType"]: f for f in s["filters"]}
            _symbol_filters_cache[symbol] = filters
            print("Symbol:", symbol)
            print("LOT_SIZE stepSize:", filters["LOT_SIZE"]["stepSize"], "minQty:", filters["LOT_SIZE"]["minQty"])
            print("PRICE_FILTER tickSize:", filters["PRICE_FILTER"]["tickSize"])
            return filters

    raise RuntimeError(f"Symbol {symbol} not found in futures_exchange_info()")

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
# HELPERS
# ======================
def set_leverage() -> None:
    client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)

def get_mark_price() -> float:
    mp = client.futures_mark_price(symbol=SYMBOL)
    return float(mp["markPrice"])

def get_wallet_balance() -> float:
    acc = client.futures_account()
    return float(acc.get("totalWalletBalance", 0.0))

def get_klines(symbol: str, interval: str, limit: int = 300) -> pd.DataFrame:
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

def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()

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

def place_entry_and_bracket(side: str, qty: float, sl_price: float, tp_price: float) -> None:
    filters = get_symbol_filters(SYMBOL)
    tick = filters["PRICE_FILTER"]["tickSize"]

    sl_price = quantize_down(sl_price, tick)
    tp_price = quantize_down(tp_price, tick)

    cancel_all_open_orders()

    client.futures_create_order(
        symbol=SYMBOL,
        side=side,
        type="MARKET",
        quantity=qty
    )

    exit_side = "SELL" if side == "BUY" else "BUY"

    client.futures_create_order(
        symbol=SYMBOL,
        side=exit_side,
        type="STOP_MARKET",
        stopPrice=sl_price,
        closePosition=True
    )

    client.futures_create_order(
        symbol=SYMBOL,
        side=exit_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=tp_price,
        closePosition=True
    )

    print("BRACKET SET | SL:", sl_price, "| TP:", tp_price)

# ======================
# SIZING
# ======================
def calc_qty_risk_based(entry_price: float, sl_pct: float) -> float:
    equity = get_wallet_balance()
    risk_usd = equity * RISK_PCT
    notional = risk_usd / sl_pct

    max_notional = equity * LEVERAGE * MAX_NOTIONAL_CAP_RATIO
    if notional > max_notional:
        print("WARN notional capped:", round(notional, 2), "->", round(max_notional, 2))
        notional = max_notional

    if notional < MIN_NOTIONAL_USD:
        notional = MIN_NOTIONAL_USD

    filters = get_symbol_filters(SYMBOL)
    step_size = filters["LOT_SIZE"]["stepSize"]
    min_qty = float(filters["LOT_SIZE"]["minQty"])

    raw_qty = notional / entry_price
    qty = quantize_up(raw_qty, step_size)

    if qty < min_qty:
        qty = min_qty

    if qty * entry_price < MIN_NOTIONAL_USD:
        qty = quantize_up(MIN_NOTIONAL_USD / entry_price, step_size)

    return qty

# ======================
# SIGNALS
# ======================
def trend_bias_1h(df1h: pd.DataFrame) -> str:
    df = df1h.copy()
    df["ema200"] = ema(df["close"], EMA_TREND_LEN)
    last_closed = df.iloc[-2]
    close_ = float(last_closed["close"])
    ema200 = float(last_closed["ema200"])
    if close_ > ema200:
        return "LONG"
    if close_ < ema200:
        return "SHORT"
    return "NONE"

def pullback_entry_15m(df15: pd.DataFrame, bias: str):
    """
    High-winrate pullback entry:
    - Trend 15m: EMA20 vs EMA50 must align with bias
    - Pullback: candle must be near/touch EMA20 (not far from it)
    - RSI must be in pullback zone 45–55 and turning back with direction
    - Candle close confirms direction
    """
    df = df15.copy()
    df["ema20"] = ema(df["close"], EMA_FAST)
    df["ema50"] = ema(df["close"], EMA_SLOW)
    df["rsi"] = rsi(df["close"], RSI_LEN)
    df["atr"] = atr(df, ATR_LEN)

    c = df.iloc[-2]     # last closed
    p = df.iloc[-3]     # prev closed

    o = float(c["open"])
    h = float(c["high"])
    l = float(c["low"])
    cl = float(c["close"])

    ema20 = float(c["ema20"])
    ema50 = float(c["ema50"])
    r = float(c["rsi"]) if pd.notna(c["rsi"]) else None
    r_prev = float(p["rsi"]) if pd.notna(p["rsi"]) else None
    atr_val = float(c["atr"]) if pd.notna(c["atr"]) else None

    if r is None or r_prev is None or atr_val is None:
        return None

    # ATR -> SL% clamp
    sl_pct = atr_val / cl
    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, sl_pct))
    tp_pct = sl_pct * TP_R_MULT

    # pullback distance to EMA20 (use close as reference)
    dist = abs(cl - ema20) / cl
    near_ema20 = dist <= PULLBACK_MAX_DIST

    # "touch" logic: wick touches EMA20 area
    touched_ema20 = (l <= ema20 <= h)

    # RSI in pullback zone
    rsi_in_pull_zone = (RSI_PULL_MIN <= r <= RSI_PULL_MAX)

    bullish_close = cl > o
    bearish_close = cl < o

    # Trend alignment on 15m
    up_trend_15m = ema20 > ema50
    down_trend_15m = ema20 < ema50

    if bias == "LONG":
        if not up_trend_15m:
            return None

        # Pullback requirement: price must be near/touch EMA20
        if not (near_ema20 or touched_ema20):
            return None

        # RSI pullback then start rising
        if not (rsi_in_pull_zone and r >= r_prev):
            return None

        # Candle confirmation
        if not bullish_close:
            return None

        return {"side": "BUY", "sl_pct": sl_pct, "tp_pct": tp_pct, "ema20": ema20, "ema50": ema50, "rsi": r, "dist": dist}

    if bias == "SHORT":
        if not down_trend_15m:
            return None

        if not (near_ema20 or touched_ema20):
            return None

        if not (rsi_in_pull_zone and r <= r_prev):
            return None

        if not bearish_close:
            return None

        return {"side": "SELL", "sl_pct": sl_pct, "tp_pct": tp_pct, "ema20": ema20, "ema50": ema50, "rsi": r, "dist": dist}

    return None

# ======================
# MAIN LOOP
# ======================
def main() -> None:
    set_leverage()
    print("ETH PULLBACK SCALP START (TESTNET)", SYMBOL, "| TF:", TF_TREND, "+", TF_ENTRY, "| Lev:", LEVERAGE)

    last_15m_close_time = None
    trades_today = 0
    today = datetime.now(timezone.utc).date()

    start_equity_today = get_wallet_balance()
    cooldown_until = None
    prev_in_position = has_open_position()

    while True:
        try:
            now = datetime.now(timezone.utc)

            # reset daily (UTC)
            if now.date() != today:
                today = now.date()
                trades_today = 0
                start_equity_today = get_wallet_balance()
                cooldown_until = None
                print("== New day (UTC). Reset counters ==")

            equity_now = get_wallet_balance()
            daily_dd = (equity_now - start_equity_today) / max(start_equity_today, 1e-9)

            if daily_dd <= -abs(MAX_DAILY_DRAWDOWN_PCT):
                print(now, "DAILY STOP. DD:", round(daily_dd * 100, 2), "%")
                time.sleep(SLEEP_SECONDS)
                continue

            if trades_today >= MAX_TRADES_PER_DAY:
                print(now, "Trade limit reached. trades_today =", trades_today)
                time.sleep(SLEEP_SECONDS)
                continue

            # cooldown tracking after close
            in_pos = has_open_position()
            if prev_in_position and not in_pos:
                cooldown_until = now + pd.Timedelta(minutes=COOLDOWN_MINUTES)
                print(now, "COOLDOWN START until", cooldown_until)
            prev_in_position = in_pos

            if cooldown_until is not None and now < cooldown_until:
                time.sleep(SLEEP_SECONDS)
                continue

            # fetch 15m candles, run once per close
            df15 = get_klines(SYMBOL, TF_ENTRY, limit=300)
            last_closed_15 = df15.iloc[-2]
            close_time_15 = last_closed_15["close_time"]

            if last_15m_close_time == close_time_15:
                time.sleep(SLEEP_SECONDS)
                continue
            last_15m_close_time = close_time_15

            # if in position: do nothing
            if in_pos:
                amt = get_position_amt()
                print(close_time_15, "In position:", amt, "Skip.")
                time.sleep(SLEEP_SECONDS)
                continue

            cancel_all_open_orders()

            # bias 1h
            df1h = get_klines(SYMBOL, TF_TREND, limit=400)
            bias = trend_bias_1h(df1h)

            # pullback entry 15m
            sig = pullback_entry_15m(df15, bias)

            print(
                close_time_15,
                "equity:", round(equity_now, 2),
                "| bias:", bias,
                "| signal:", "YES" if sig else "NO"
            )

            if bias == "NONE" or not sig:
                time.sleep(SLEEP_SECONDS)
                continue

            price = get_mark_price()
            sl_pct = sig["sl_pct"]
            tp_pct = sig["tp_pct"]
            side = sig["side"]

            qty = calc_qty_risk_based(price, sl_pct)
            approx_notional = qty * price

            if side == "BUY":
                sl_price = price * (1 - sl_pct)
                tp_price = price * (1 + tp_pct)
                direction = "LONG"
            else:
                sl_price = price * (1 + sl_pct)
                tp_price = price * (1 - tp_pct)
                direction = "SHORT"

            print(
                close_time_15,
                "ENTRY", direction,
                "| qty:", qty,
                "| mark:", round(price, 2),
                "| SL%:", round(sl_pct * 100, 3),
                "| TP%:", round(tp_pct * 100, 3),
                "| dist_to_EMA20%:", round(sig["dist"] * 100, 3),
                "| RSI:", round(sig["rsi"], 2),
                "| notional~:", round(approx_notional, 2)
            )

            place_entry_and_bracket(side, qty, sl_price, tp_price)
            trades_today += 1
            print("ENTRY OK. trades_today =", trades_today)

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print("ERROR:", e)
            time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()