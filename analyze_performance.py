#!/usr/bin/env python3
# analyze_performance.py
# Script untuk menganalisis performa bot HYPEUSDT V5 Hybrid

import csv
import pandas as pd
from pathlib import Path

# Path menuju file log trades
LOG_FILE = Path("logs/trades_hypeusdt_v5.csv")

def analyze_trades():
    if not LOG_FILE.exists():
        print(f"❌ File log tidak ditemukan di: {LOG_FILE}")
        print("Pastikan bot sudah berjalan dan melakukan minimal 1 trade penuh (OPEN & CLOSE).")
        return

    trades = []
    current_trade = {}

    # Parsing manual baris per baris untuk menangani perbedaan kolom OPEN vs CLOSE
    with LOG_FILE.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None) # Skip header utama
        
        for row in reader:
            if len(row) < 2:
                continue
            
            ts = row[0]
            event = row[1]
            
            if event == "OPEN":
                # Ambil data pembukaan posisi
                current_trade = {
                    "open_ts": ts,
                    "side": row[2] if len(row) > 2 else "UNKNOWN",
                    "mode": row[3] if len(row) > 3 else "UNKNOWN",
                    "bias": row[4] if len(row) > 4 else "UNKNOWN",
                    "entry_price": float(row[5]) if len(row) > 5 and row[5] else 0.0,
                }
            elif event == "CLOSE":
                # Karena format CSV menimpa posisi kolom, realized_pnl ada di index ke-2
                try:
                    pnl = float(row[2])
                except (ValueError, IndexError):
                    pnl = 0.0
                    
                if current_trade:
                    current_trade["close_ts"] = ts
                    current_trade["pnl"] = pnl
                    trades.append(current_trade)
                    current_trade = {} # Reset untuk trade berikutnya

    if not trades:
        print("⚠️ Belum ada trade yang selesai (CLOSE) untuk dianalisis.")
        return

    # Konversi ke Pandas DataFrame untuk kalkulasi statistik
    df = pd.DataFrame(trades)
    
    # Kalkulasi Metrik Utama
    total_trades = len(df)
    wins = df[df['pnl'] > 0]
    losses = df[df['pnl'] < 0]
    evens = df[df['pnl'] == 0]
    
    win_rate = (len(wins) / total_trades) * 100 if total_trades > 0 else 0
    total_pnl = df['pnl'].sum()
    gross_profit = wins['pnl'].sum()
    gross_loss = abs(losses['pnl'].sum())
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')
    
    avg_win = wins['pnl'].mean() if len(wins) > 0 else 0.0
    avg_loss = losses['pnl'].mean() if len(losses) > 0 else 0.0
    
    # Kalkulasi Max Drawdown (berdasarkan PnL akumulatif)
    df['cumulative_pnl'] = df['pnl'].cumsum()
    df['peak_pnl'] = df['cumulative_pnl'].cummax()
    df['drawdown'] = df['cumulative_pnl'] - df['peak_pnl']
    max_drawdown = df['drawdown'].min()
    
    # Menampilkan Laporan
    print("="*55)
    print("📊 LAPORAN PERFORMA BOT: HYPEUSDT V5 HYBRID")
    print("="*55)
    print(f"Total Trades Selesai : {total_trades}")
    print(f"Win Rate             : {win_rate:.2f}% ({len(wins)} Win / {len(losses)} Loss / {len(evens)} BEP)")
    print(f"Total Realized PnL   : ${total_pnl:.4f}")
    print(f"Profit Factor        : {profit_factor:.2f}")
    print(f"Max Drawdown (PnL)   : ${max_drawdown:.4f}")
    print(f"Rata-rata Win        : ${avg_win:.4f}")
    print(f"Rata-rata Loss       : ${avg_loss:.4f}")
    
    print("\n📈 KINERJA BERDASARKAN MODE STRATEGI:")
    if 'mode' in df.columns:
        mode_stats = df.groupby('mode')['pnl'].agg(
            Total_Trade='count', 
            Total_PnL='sum', 
            Win_Rate_Pct=lambda x: (x > 0).mean() * 100
        )
        print(mode_stats.to_string(formatters={'Total_PnL': '${:.4f}'.format, 'Win_Rate_Pct': '{:.2f}%'.format}))
        
    print("\n📉 KINERJA BERDASARKAN ARAH TRADING:")
    if 'side' in df.columns:
        side_stats = df.groupby('side')['pnl'].agg(
            Total_Trade='count', 
            Total_PnL='sum', 
            Win_Rate_Pct=lambda x: (x > 0).mean() * 100
        )
        print(side_stats.to_string(formatters={'Total_PnL': '${:.4f}'.format, 'Win_Rate_Pct': '{:.2f}%'.format}))
    print("="*55)

if __name__ == "__main__":
    analyze_trades()