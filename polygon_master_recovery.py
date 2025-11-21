import requests
import threading
import csv
import os
from datetime import datetime, timedelta
import time
import direct_redis
import pandas as pd
from io import StringIO
import concurrent.futures
import queue
from polygon import RESTClient
from polygon.rest.aggs import Agg
from config import POLYGON_API_KEY
from config import REVERSED_INDEX_QUEUE_DICT, REVERSED_OPTIONS_QUEUE_DICT
from config import get_our_index_ticker, get_our_options_ticker

r = direct_redis.DirectRedis(host='localhost', port=6379, db=0)
client = RESTClient(POLYGON_API_KEY)

# aggs = []
# for a in client.list_aggs(
#     ticker="O:SPXW250818C06430000",
#     multiplier=1,
#     timespan="minute",
#     from_="2025-08-18",
#     to="2025-08-18",
#     adjusted=True,
#     sort="asc",
#     limit=120,
# ):
#     # aggs.append(a)
#     print(a)

def store_in_redis(symbol: str, data: Agg):
    reference_timestamp = data.timestamp
    int_timestamp_ms = int(reference_timestamp or 0)
    timestamp_sec = int(int_timestamp_ms / 1000)
    # timestamp_sec = reference_timestamp
    timestamp = datetime.fromtimestamp(timestamp_sec)
    ltt_min_1 = timestamp.replace(second=0, microsecond=0)
    print(ltt_min_1)
    values = {
        'o': data.open,
        'h': data.high,
        'l': data.low,
        'c': data.close,
        'v': data.volume,
        'oi': 0,
    }
    # Store in Redis hashset
    r.hset(f"l.tick_{str(ltt_min_1)}", symbol, values)                 
    r.hset(f'l.{symbol}', str(ltt_min_1), values)

# Shared queue
data_queue = queue.Queue()

# Sentinel value to signal shutdown
SENTINEL = object()

def fetch_data(symbol, date_start, date_end):
    print(f"[FETCH] Fetching {symbol}")
    for data in client.list_aggs(
        ticker=symbol,
        multiplier=1,
        timespan="minute",
        from_=date_start,
        to=date_end,
        adjusted=True,
        sort="asc",
        limit=120,
    ):
        # aggs.append(a)
        data_queue.put((symbol, data))
    print(f"[FETCH] Done {symbol}")

# Consumer that processes data
def process_data():
    while True:
        item = data_queue.get()
        if item is SENTINEL:
            data_queue.task_done()
            print("[PROCESS] Received sentinel. Exiting.")
            break

        symbol, data = item
        print(f"[PROCESS] Processing {symbol}: {data}")
        if str(symbol).startswith("I:"):
            internal_symbol = get_our_index_ticker(symbol)
            store_in_redis(internal_symbol, data)
        elif str(symbol).startswith("O:"):
            internal_symbol = get_our_options_ticker(symbol)
            store_in_redis(internal_symbol, data)
        print(f"[PROCESS] Done {symbol}")
        data_queue.task_done()

# Orchestrator
def fetch_and_process(symbols, date_start, date_end, max_fetchers=5, max_processors=5):
    # Start processor threads first
    processor_threads = []
    for _ in range(max_processors):
        t = threading.Thread(target=process_data)
        t.start()
        processor_threads.append(t)

    # Start fetchers
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_fetchers) as fetch_executor:
        for symbol in symbols:
            fetch_executor.submit(fetch_data, symbol, date_start, date_end)

    # Wait for all data to be processed
    data_queue.join()

    # Send sentinel signals to stop processors
    for _ in range(max_processors):
        data_queue.put(SENTINEL)

    # Wait for all processors to finish
    for t in processor_threads:
        t.join()




if __name__ == "__main__":
    dt_start = datetime.now().replace(hour=9, minute=30)
    dt_end = datetime.now().replace(hour=12, minute=55)

    index_tickers   = list(REVERSED_INDEX_QUEUE_DICT.keys())
    options_tickers = list(REVERSED_OPTIONS_QUEUE_DICT.keys())

    # tickers_to_fetch_data_for = index_tickers + options_tickers
    # tickers_to_fetch_data_for = index_tickers
    tickers_to_fetch_data_for = options_tickers
    fetch_and_process(tickers_to_fetch_data_for, dt_start, dt_end)