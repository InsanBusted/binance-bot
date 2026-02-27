import os
import time
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client
from decimal import Decimal, ROUND_DOWN, ROUND_CEILING
from datetime import datetime


# ======================
# CONFIG
# ======================
SYMBOL = "BTCUSDT"
INTERVAL = "15m"

EMA_FAST = 20
EMA_SLOW = 50
RSI_LEN = 14

LEVERAGE = 5

# Strategy thresholds (simple & jelas)
RSI_LONG = 55
RSI_SHORT = 45

# Risk / bracket
STOP_PCT = 0.005    # 0.5%
TAKE_PCT = 0.008    # 0.8%

# Notional per trade (TESTNET kamu minta $200)
TARGET_NOTIONAL_USD = 200.0
MIN_NOTIONAL_USD = 100.0     # constraint dari exchange (seen in testnet)

# Basic safety limits (optional)
MAX_TRADES_PER_DAY = 20
MAX_DAILY_LOSS_USD = 999999  # set besar dulu untuk testnet (boleh kamu kecilin nanti)

# Anti spam / rate limit
SLEEP_SECONDS = 60

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
# HELPERS
# ======================
def set_leverage() -> None:
    client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)


def get_mark_price() -> float:
    mp = client.futures_mark_price(symbol=SYMBOL)
    return float(mp["markPrice"])


def get_klines(limit: int = 200) -> pd.DataFrame:
    klines = client.futures_klines(symbol=SYMBOL, interval=INTERVAL, limit=limit)
    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base", "taker_quote", "ignore"
    ])
    df["close"] = df["close"].astype(float)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(RSI_LEN).mean()
    avg_loss = loss.rolling(RSI_LEN).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def get_position_amt() -> float:
    positions = client.futures_position_information(symbol=SYMBOL)
    if not positions:
        return 0.0
    for p in positions:
        # untuk symbol yang sama, biasanya 1 item
        return float(p.get("positionAmt", 0.0))
    return 0.0


def has_open_position() -> bool:
    return get_position_amt() != 0.0


def cancel_all_open_orders() -> None:
    try:
        orders = client.futures_get_open_orders(symbol=SYMBOL)
        if not orders:
            return
        client.futures_cancel_all_open_orders(symbol=SYMBOL)
        print("Cancel: open orders cleared.")
    except Exception as e:
        print("WARN cancel_all_open_orders:", e)


def calc_qty_for_notional(notional_usd: float, price: float) -> float:
    """
    Qty dihitung dari notional target, lalu dibulatkan KE ATAS sesuai stepSize.
    Pastikan minimal juga memenuhi minQty dan min notional.
    """
    filters = get_symbol_filters(SYMBOL)
    step_size = filters["LOT_SIZE"]["stepSize"]
    min_qty = float(filters["LOT_SIZE"]["minQty"])

    # pastikan notional minimal memenuhi min notional exchange
    if notional_usd < MIN_NOTIONAL_USD:
        notional_usd = MIN_NOTIONAL_USD

    raw_qty = notional_usd / price
    qty = quantize_up(raw_qty, step_size)

    if qty < min_qty:
        qty = min_qty

    return qty


def place_entry_and_bracket(side: str, qty: float, entry_price: float) -> None:
    """
    Entry MARKET + pasang SL/TP pakai closePosition=True.
    stopPrice harus sesuai tickSize.
    """
    # Entry
    client.futures_create_order(
        symbol=SYMBOL,
        side=side,
        type="MARKET",
        quantity=qty
    )

    # SL/TP base
    if side == "BUY":
        sl_price = entry_price * (1 - STOP_PCT)
        tp_price = entry_price * (1 + TAKE_PCT)
        exit_side = "SELL"
    else:
        sl_price = entry_price * (1 + STOP_PCT)
        tp_price = entry_price * (1 - TAKE_PCT)
        exit_side = "BUY"

    filters = get_symbol_filters(SYMBOL)
    tick = filters["PRICE_FILTER"]["tickSize"]

    sl_price = quantize_down(sl_price, tick)
    tp_price = quantize_down(tp_price, tick)

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


def get_wallet_balance() -> float:
    acc = client.futures_account()
    return float(acc.get("totalWalletBalance", 0.0))


# ======================
# MAIN LOOP
# ======================
def main() -> None:
    set_leverage()
    print("BOT START (TESTNET)", SYMBOL, "TF", INTERVAL, "| Notional:", TARGET_NOTIONAL_USD, "| Lev:", LEVERAGE)

    last_candle_close = None
    trades_today = 0
    today = datetime.utcnow().date()
    start_balance_today = None

    while True:
        try:
            # reset harian (UTC)
            now_day = datetime.utcnow().date()
            if now_day != today:
                today = now_day
                trades_today = 0
                start_balance_today = None
                print("== New day (UTC). Reset counters ==")

            if start_balance_today is None:
                start_balance_today = get_wallet_balance()

            # fetch candle
            df = compute_indicators(get_klines())
            last = df.iloc[-2]  # candle terakhir yang sudah close
            close_time = last["close_time"]

            # run once per candle close
            if last_candle_close == close_time:
                time.sleep(SLEEP_SECONDS)
                continue
            last_candle_close = close_time

            # daily stop (basic)
            bal_now = get_wallet_balance()
            daily_pnl = bal_now - start_balance_today
            if daily_pnl <= -abs(MAX_DAILY_LOSS_USD):
                print(close_time, "DAILY STOP. PnL:", round(daily_pnl, 2))
                time.sleep(SLEEP_SECONDS)
                continue

            if trades_today >= MAX_TRADES_PER_DAY:
                print(close_time, "Trade limit reached today. trades_today =", trades_today)
                time.sleep(SLEEP_SECONDS)
                continue

            # if no position but leftover orders exist, clear them
            if not has_open_position():
                cancel_all_open_orders()

            # if already in position, skip entry
            if has_open_position():
                amt = get_position_amt()
                print(close_time, "In position:", amt, "Skip entry.")
                time.sleep(SLEEP_SECONDS)
                continue

            # indicators
            ema_fast = float(last["ema_fast"])
            ema_slow = float(last["ema_slow"])
            rsi = float(last["rsi"])
            close_price = float(last["close"])

            # decide signal
            signal = "NONE"
            side = None

            if ema_fast > ema_slow and rsi > RSI_LONG:
                signal = "LONG"
                side = "BUY"
            elif ema_fast < ema_slow and rsi < RSI_SHORT:
                signal = "SHORT"
                side = "SELL"

            print(
                close_time,
                "close:", round(close_price, 2),
                "ema20:", round(ema_fast, 2),
                "ema50:", round(ema_slow, 2),
                "rsi:", round(rsi, 2),
                "signal:", signal
            )

            if side is None:
                time.sleep(SLEEP_SECONDS)
                continue

            # entry
            price = get_mark_price()
            qty = calc_qty_for_notional(TARGET_NOTIONAL_USD, price)

            print(close_time, "ENTRY:", signal, "| qty:", qty, "| mark:", round(price, 2))
            place_entry_and_bracket(side, qty, price)
            trades_today += 1
            print("ENTRY OK. trades_today =", trades_today)

            time.sleep(SLEEP_SECONDS)

        except Exception as e:
            print("ERROR:", e)
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    main()