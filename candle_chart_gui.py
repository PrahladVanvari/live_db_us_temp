import streamlit as st
import pickle
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import direct_redis

# Redis connection
r = direct_redis.DirectRedis(host='localhost', port=6379)

def try_decode(val):
    return val.decode() if isinstance(val, bytes) else val

# Get symbols
def get_symbols():
    symbols = []
    for key in r.scan_iter("l.*"):
        k = try_decode(key)
        if not k.startswith("l.tick_"):
            symbols.append(k)
    return sorted(symbols)

# Get all timestamps and OHLCV data for a symbol
def get_data(symbol):
    raw = r.hgetall(symbol)
    data = {}
    for ts_b, val_b in raw.items():
        ts = try_decode(ts_b)
        try:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except:
            continue
        # ohlc = pickle.loads(val_b)
        ohlc = val_b if isinstance(val_b, dict) else pickle.loads(val_b)
        data[dt] = ohlc
    return data

def main():
    st.title("📈 1-Minute Candle Chart Viewer")

    symbols = get_symbols()
    selected_symbol = st.selectbox("Select a Symbol", symbols)

    if selected_symbol:
        all_data = get_data(selected_symbol)
        date_options = sorted({dt.date().isoformat() for dt in all_data})
        selected_date = st.selectbox("Select a Date", date_options)

        if selected_date:
            # Filter by date
            rows = []
            for dt, ohlc in all_data.items():
                if dt.date().isoformat() == selected_date:
                    rows.append({
                        "timestamp": dt,
                        **ohlc
                    })

            df = pd.DataFrame(rows).sort_values("timestamp")

            if not df.empty:
                st.subheader(f"Candles for {selected_symbol} on {selected_date}")
                fig, ax = plt.subplots(figsize=(10, 4))

                for _, row in df.iterrows():
                    color = 'green' if row['c'] >= row['o'] else 'red'
                    wick_x = row['timestamp'] + timedelta(minutes=0.5)
                    ax.plot([wick_x, wick_x], [row['l'], row['h']], color=color)
                    ax.add_patch(
                        plt.Rectangle(
                            (row['timestamp'], min(row['o'], row['c'])),
                            pd.Timedelta(minutes=1),
                            abs(row['c'] - row['o']),
                            color=color
                        )
                    )

                ax.set_ylabel("Price")
                ax.grid(True)
                fig.autofmt_xdate()
                fig.tight_layout()
                # fig.patch.set_visible(False)
                plt.box(False)
                st.pyplot(fig)

if __name__ == "__main__":
    main()
