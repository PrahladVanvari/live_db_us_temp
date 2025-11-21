# polygon_master.py
from typing import List
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from polygon.websocket.models import IndexValue, EquityTrade, EquityQuote
import polygon
import direct_redis
import multiprocessing as mp
from datetime import datetime, time,timedelta
import time
import queue
import asyncio

from config import POLYGON_API_KEY, MARKET_CLOSE_TIME
from config import MASTER_QUEUE_DICT, REVERSED_MASTER_QUEUE_DICT, INDEX_QUEUE_DICT, REVERSED_INDEX_QUEUE_DICT, OPTIONS_QUEUE_DICT, REVERSED_OPTIONS_QUEUE_DICT
from config import get_our_index_ticker, get_our_options_ticker
from loggers import polygon_logger, queue_logger
from data_consumers import consume_index_value, consume_options_trade_msg, consume_options_quote_msg
from data_producers import create_index_websocket_client, create_options_websocket_client, indices_data_producer, options_data_producer
from data_producers import MAIN_QUEUE # MP_QUEUE

# KEYDB DataBase Connection
redis = direct_redis.DirectRedis(host='localhost', port=6379, db=0)
polygon_logger.info("Connection to KeyDB Complete.")
running = True

options_last_msg_time = datetime.now()
def process_queue_data(queue_name: str, queue1: mp.Queue):
    global options_last_msg_time
    last_minute = None
        
    queue_logger.info(f"process_queue_data() started for {queue_name}")
    while running:
        try:
            market_data = queue1.get(timeout=0.01)
            queue_logger.debug(f"Popped index data from queue: {market_data}")
            if isinstance(market_data, IndexValue):
                last_minute = consume_index_value(market_data, last_minute, queue1.qsize(), redis, queue_name)
            elif isinstance(market_data, EquityTrade):
                options_last_msg_time = datetime.now()
                consume_options_trade_msg(market_data, last_minute, queue1.qsize(), redis, queue_name) # last_minute = 
            elif isinstance(market_data, EquityQuote):
                options_last_msg_time = datetime.now()
                consume_options_quote_msg(market_data, last_minute, queue1.qsize(), redis, queue_name)
            else:
                queue_logger.warning(f"Unknown Message received: {market_data}")

        except queue.Empty:
            if datetime.now() > MARKET_CLOSE_TIME:
                queue_logger.info(f"{datetime.now()} > {MARKET_CLOSE_TIME}. Stopping Consumers.")
                break
            # options_logger.info("Waiting for data")
            continue  # Keep waiting for new data
    print("Running is false. Exited processor")

if __name__ == "__main__":
    
    # Starting Producer
    polygon_logger.info("Starting Polygon Producer")
    index_client = create_index_websocket_client()
    options_client = create_options_websocket_client()
    index_producer_process = mp.Process(target=indices_data_producer, args=[index_client])
    index_producer_process.start()
    options_producer_process = mp.Process(target=options_data_producer, args=[options_client])
    options_producer_process.start()

    # Starting Consumers
    consumer_processes: List[mp.Process] = []
    for queue_name, queue_tmp in OPTIONS_QUEUE_DICT.items():
    # if True:
        # queue_name = "main"
        queue_1 = MAIN_QUEUE
        # print(f'Inside for : {queues[prefix]}')
        polygon_logger.info(f"Starting mp.Queue for {queue_name}")
        process = mp.Process(target=process_queue_data, args=(queue_name, queue_1))
        consumer_processes.append(process)
        process.start()

    while True:
        time.sleep(10)

        # 🔐 Detect silent disconnection
        if datetime.now() - options_last_msg_time > timedelta(seconds=50):
            polygon_logger.warning("No data received for 50s — reconnecting...")
            try:
                options_client.unsubscribe_all()
                asyncio.run(options_client.close())
                options_producer_process.join()     #TODO: check if the previous .close() actually stops this function
            except Exception as e:
                polygon_logger.warning(f"Error closing socket: {e}")

            # ⬅️ Recreate and resubscribe right here
            options_client = create_options_websocket_client()
            
            # We will start options_data_producer() again as the client.close() call is assumed to have killed the function.
            options_producer_process = mp.Process(target=options_data_producer, args=[options_client])
            options_producer_process.start()
            options_last_msg_time = datetime.now()

        # HANDLE market Close
        if datetime.now() > MARKET_CLOSE_TIME:
            running = False
            # polygon.unsubscribe_all()
            polygon_logger.info("Market Closed. Unsubscribing to all ticker updates")
            print("Market Closed. Unsubscribing to all ticker updates")
            index_client.unsubscribe_all()
            polygon_logger.info("Unsubscribing to all index updates successful.")
            print("Unsubscribing to all index updates successful.")
            options_client.unsubscribe_all()
            polygon_logger.info("Unsubscribing to all options updates successful.")
            print("Unsubscribing to all options updates successful.")
            index_client.close()
            options_client.close()
            polygon_logger.info("Successfully Closed All WebSockets.")

            # async def shutdown():
            #     polygon_logger.info("Market Closed. Closing all socket connections")
            #     print("Market Closed. Closing all socket connections")
            #     await index_client.close()
            #     polygon_logger.info("Index client closed.")
            #     print("Index client closed.")
            #     await options_client.close()
            #     polygon_logger.info("Options client closed.")
            #     print("Options client closed.")
            # asyncio.run(shutdown())

            # Wait for producers to finish
            index_producer_process.join()
            options_producer_process.join()

            # Wait for consumers to finish
            for p in consumer_processes:
                p.join()            

            # status = polygon.close()
            print("Market Closed. Process Shutdown.")
            break
 