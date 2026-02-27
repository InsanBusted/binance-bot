import os
import pandas as pd
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

client = Client(api_key, api_secret, testnet=True)
client.FUTURES_URL = "https://testnet.binancefuture.com"

# Ambil 200 candle 15 menit
klines = client.futures_klines(symbol="BTCUSDT", interval="15m", limit=200)

# Masukkan ke DataFrame
df = pd.DataFrame(klines, columns=[
    "open_time","open","high","low","close","volume",
    "close_time","qav","num_trades","taker_base","taker_quote","ignore"
])

df["close"] = df["close"].astype(float)

# Hitung EMA
df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

# Hitung RSI
delta = df["close"].diff()
gain = delta.clip(lower=0)
loss = -delta.clip(upper=0)

avg_gain = gain.rolling(14).mean()
avg_loss = loss.rolling(14).mean()

rs = avg_gain / avg_loss
df["rsi"] = 100 - (100 / (1 + rs))

# Ambil candle terakhir yang sudah close
last = df.iloc[-2]

print("Harga Close:", round(last["close"],2))
print("EMA20:", round(last["ema20"],2))
print("EMA50:", round(last["ema50"],2))
print("RSI:", round(last["rsi"],2))

if last["ema20"] > last["ema50"] and last["rsi"] > 55:
    print("Signal: LONG")
elif last["ema20"] < last["ema50"] and last["rsi"] < 45:
    print("Signal: SHORT")
else:
    print("Signal: NO TRADE")