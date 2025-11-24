from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from polygon.websocket.models import IndexValue, EquityTrade, EquityQuote
from typing import List
import multiprocessing as mp # for mp.Queue
import traceback
from datetime import datetime, timedelta
from config import POLYGON_API_KEY, MARKET_CLOSE_TIME
from config import MASTER_QUEUE_DICT, REVERSED_MASTER_QUEUE_DICT, INDEX_QUEUE_DICT, REVERSED_INDEX_QUEUE_DICT, OPTIONS_QUEUE_DICT, REVERSED_OPTIONS_QUEUE_DICT
from loggers import polygon_logger, options_logger, index_logger
import time

MP_QUEUES = {queue_name: mp.Queue() for queue_name, symbols in MASTER_QUEUE_DICT.items()}
# MAIN_QUEUE = mp.Queue()

def _create_polygon_websocket_client(api_key: str, feed: Feed, market: Market) -> WebSocketClient:
    return WebSocketClient(api_key=api_key, feed=feed, market=market)

def create_index_websocket_client():
    # Polygon Websocket Connection
    polygon = WebSocketClient(
        api_key=POLYGON_API_KEY,
        feed=Feed.RealTime,
        market=Market.Indices
    )
    polygon_logger.info("Connection to Polygon for Indexes Complete.")
    
    return polygon

def create_options_websocket_client():
    # Polygon Websocket Connection
    polygon = WebSocketClient(
        api_key=POLYGON_API_KEY,
        feed=Feed.RealTime,
        market=Market.Options
    )
    polygon_logger.info("Connection to Polygon for Options Complete.")
    
    return polygon

def indices_data_producer(polygon: WebSocketClient):

    # Subscribe to receive feed for the specific indices
    for index_name in REVERSED_INDEX_QUEUE_DICT.keys():
        subscription_name =f"V.{index_name}"
        polygon_logger.info(f"Subscribed to {subscription_name}")
        polygon.subscribe(subscription_name)

    polygon_logger.info("Indices Data subscription complete")

    def handle_index_update(index_value: IndexValue):
        # print(f"IndexValue[ ev:{index_value.event_type}, val: {index_value.value}, ticker: {index_value.ticker}, Timestamp: {index_value.timestamp}]")
        # polygon_logger.debug(f"IndexValue[ ev:{index_value.event_type}, val: {index_value.value}, ticker: {index_value.ticker}, Timestamp: {index_value.timestamp}]")
        index_logger.debug(f"Received tick: {index_value}")
        try :
            if index_value.ticker in REVERSED_INDEX_QUEUE_DICT.keys() :
                queue_name = REVERSED_INDEX_QUEUE_DICT[index_value.ticker]
                index_logger.debug(f"Adding data to Queue({queue_name}): {index_value}")
                MP_QUEUES[queue_name].put(index_value)
                # MAIN_QUEUE.put(index_value)
            else:
                index_logger.warning(f"Tick for Unknown Index Received: {index_value.ticker}")

        except Exception as e:
            traceback.print_exc()


    def handle_msg(msgs: List[WebSocketMessage]):
        for msg in msgs:
            # print(msg)
            # Check if the message is of type IndexValue
            if isinstance(msg, IndexValue):
                handle_index_update(msg)
    # print messages
    polygon.run(handle_msg)

def options_data_producer(polygon: WebSocketClient):
    # # Polygon Websocket Connection
    # polygon = WebSocketClient(
    #     api_key=POLYGON_API_KEY,
    #     feed=Feed.RealTime,
    #     market=Market.Options
    # )
    # polygon_logger.info("Connection to Polygon Complete.")

    for option_name in REVERSED_OPTIONS_QUEUE_DICT.keys():
        subscription_name =f"T.{option_name}"
        polygon_logger.info(f"Subscribed to {subscription_name}")
        polygon.subscribe(subscription_name)

        subscription_name =f"Q.{option_name}"
        polygon_logger.info(f"Subscribed to {subscription_name}")
        polygon.subscribe(subscription_name)

    polygon_logger.info("Options Data subscription complete")

    def handle_options_trade_update(options_trade_data: EquityTrade):
        options_logger.debug(f"Received tick: {options_trade_data}")
        try :
            if options_trade_data.symbol in REVERSED_OPTIONS_QUEUE_DICT.keys() :
                queue_name = REVERSED_OPTIONS_QUEUE_DICT[options_trade_data.symbol]
                options_logger.debug(f"Adding data to Queue({queue_name}): {options_trade_data}")
                MP_QUEUES[queue_name].put(options_trade_data)
                # MAIN_QUEUE.put(options_trade_data)
            else:
                options_logger.warning(f"Tick for Unknown Index Received: {options_trade_data.symbol}")

        except Exception as e:
            traceback.print_exc()
            
    def handle_options_quote_update(options_quote_data: EquityQuote):
        options_logger.debug(f"Received tick: {options_quote_data}")
        try :
            if options_quote_data.symbol in REVERSED_OPTIONS_QUEUE_DICT.keys() :
                queue_name = REVERSED_OPTIONS_QUEUE_DICT[options_quote_data.symbol]
                options_logger.debug(f"Adding data to Queue({queue_name}): {options_quote_data}")
                MP_QUEUES[queue_name].put(options_quote_data)
                # MAIN_QUEUE.put(options_quote_data)
            else:
                options_logger.warning(f"Tick for Unknown Index Received: {options_quote_data.symbol}")

        except Exception as e:
            traceback.print_exc()

    last_msg_time = datetime.now()

    def handle_msg(msgs: List[WebSocketMessage]):
        nonlocal last_msg_time
        last_msg_time = datetime.now()
        for msg in msgs:
            if isinstance(msg, EquityTrade):
                handle_options_trade_update(msg)
            elif isinstance(msg, EquityQuote):
                handle_options_quote_update(msg)
            else:
                options_logger.warning(f"Unknown Message received: {msg}")
    polygon.run(handle_msg)




#  while True:
#         try:
#             polygon.run(handle_msg)

#             # 🔐 Detect silent disconnection
#             if datetime.now() - last_msg_time > timedelta(seconds=50):
#                 options_logger.warning("No data received for 50s — reconnecting...")
#                 try:
#                     polygon.unsubscribe_all()
#                     polygon.close()
#                 except Exception as e:
#                     options_logger.warning(f"Error closing socket: {e}")

#                 # ⬅️ Recreate and resubscribe right here
#                 polygon = create_options_websocket_client()
#                 for option_name in REVERSED_OPTIONS_QUEUE_DICT.keys():
#                     polygon.subscribe(f"T.{option_name}")
#                     polygon.subscribe(f"Q.{option_name}")

#                 last_msg_time = datetime.now()
#         except Exception as e:
#             options_logger.error(f"WebSocket crashed: {e}")
#             try:
#                 polygon.close()
#             except:
#                 pass
#             time.sleep(5)
#             polygon = create_options_websocket_client()






