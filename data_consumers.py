from polygon.websocket.models import IndexValue, EquityTrade, EquityQuote
import direct_redis

from config import get_our_index_ticker, get_our_options_ticker, redis
from loggers import polygon_logger, options_logger
from resample import resample_index_tick, resample_options_tick

def consume_index_value(index_tick: IndexValue, last_minute, queue_size: int, queue_name):
    internal_index_ticker = get_our_index_ticker(str(index_tick.ticker))
    redis.set(f'idxval.{internal_index_ticker}', index_tick.value)

    last_minute = resample_index_tick(index_tick, last_minute, queue_size, internal_index_ticker, redis, queue_name)
    return last_minute

def consume_options_trade_msg(options_tick: EquityTrade, last_minute, queue_size: int, queue_name):
    internal_options_ticker = get_our_options_ticker(str(options_tick.symbol))
    redis.set(f'ltp.{internal_options_ticker}', options_tick.price)

    last_minute = resample_options_tick(options_tick, last_minute, queue_size, internal_options_ticker, redis, queue_name)
    return last_minute

def consume_options_quote_msg(options_tick: EquityQuote, last_minute, queue_size: int, queue_name):
    internal_options_ticker = get_our_options_ticker(str(options_tick.symbol))
    pipe = redis.pipeline()
    pipe.set(f'bid_price.{internal_options_ticker}', options_tick.bid_price)
    pipe.set(f'bid_qty.{internal_options_ticker}', options_tick.bid_size)
    pipe.set(f'ask_price.{internal_options_ticker}', options_tick.ask_price)
    pipe.set(f'ask_qty.{internal_options_ticker}', options_tick.ask_size)
    pipe.execute()