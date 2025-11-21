import datetime
import direct_redis
from typing import Dict
from polygon.websocket.models import IndexValue, EquityTrade
from loggers import resampler_logger


def resample_tick(reference_timestamp, reference_price, symbol, qsize, redis: direct_redis.DirectRedis, queue_name, last_minute):
    try:
        system_timestamp = datetime.datetime.now()
        int_timestamp_ms = int(reference_timestamp or 0)
        timestamp_sec = int(int_timestamp_ms / 1000)
        timestamp = datetime.datetime.fromtimestamp(timestamp_sec)
        ltt_min_1 = timestamp.replace(second=0, microsecond=0)
        ltt_min_2 = ltt_min_1 - datetime.timedelta(minutes=1)

        # print(f'{queue_name} {qsize} {ltt_min_1} {symbol}:{reference_price}', sep='', end='\r', flush=True)
        print(f'{queue_name} {qsize} {ltt_min_1} {symbol}:{reference_price}')
        resampler_logger.debug(
        f"Received tick for {symbol} at {timestamp}, Queue Size: {qsize}, Tick Value: {reference_price}"
        )

        # if ltt_min_1.time() < datetime.time(9, 15) and ltt_min_1.time() != datetime.time(9, 7):
        # resampler_logger.debug(f"Ignoring tick before market open: {ltt_min_1.time()}")
        # return

        price_value = reference_price if reference_price else 0
        existing_candle_1: Dict = redis.hget(f'l.tick_{ltt_min_1}', symbol) # type: ignore

        if existing_candle_1 is not None:
            candle = {
            "o": existing_candle_1["o"],
            "h": max(existing_candle_1["h"], price_value),
            "l": min(existing_candle_1["l"], price_value),
            "c": price_value,
            "v": 0,
            "oi": 0,
            }
            resampler_logger.debug(f"Updated candle for {symbol} at {timestamp}: {candle}")
        else:
            candle = {
            "o": price_value,
            "h": price_value,
            "l": price_value,
            "c": price_value,
            "v": 0,
            "oi": 0,
            }
        resampler_logger.debug(f"Created new candle for {symbol} at {timestamp}: {candle}")

        redis.hset(f'l.tick_{str(ltt_min_1)}', symbol, candle)
        redis.hset(f'l.{symbol}', str(ltt_min_1), candle)

        resampler_logger.debug(f"Candle pushed for {symbol} at {timestamp}")

        # redis.set('CURRENT_EVENT', str(ltt_min_1))
        # PUBLISH EVENT
        if symbol == 'SPXSPOT':
            print('IF CONDITION REACHED')
    
            if last_minute is None:
                last_minute = ltt_min_1.minute
        
            elif ltt_min_1.minute == 0 and last_minute == 59:
                redis.lpush('EVENTS', {'timestamp': ltt_min_2, 'bar_complete': True})
                print(f'#####################################################BAR COMPLETE#################################################')
                # print(f'Bar Complete True : {ltt_min_2}')
                last_minute = ltt_min_1.minute
    
            elif ltt_min_1.minute > last_minute:
                redis.lpush('EVENTS', {'timestamp': ltt_min_2, 'bar_complete': True})
                print(f'###############################################################BAR COMPLETE ################################################')
                last_minute = ltt_min_1.minute
        
            else:
                redis.lpush('EVENTS', {'timestamp': ltt_min_1, 'bar_complete': False})
        lag_seconds = (system_timestamp - timestamp).total_seconds()
        redis.hset('td_candle_init_lag_timestamp', str(system_timestamp), lag_seconds)
        resampler_logger.info(
        f"[{timestamp}] {symbol} Q{queue_name}[{qsize}] Lag: {lag_seconds:.3f} seconds"
        )

    except Exception as e:
        resampler_logger.exception(f"Error in resampling tick for {symbol}: {e}")

    return last_minute

    

def resample_index_tick(index_tick: IndexValue, last_minute, qsize, index_ticker, redis: direct_redis.DirectRedis, queue_name):
    reference_timestamp = index_tick.timestamp
    reference_price = index_tick.value
    symbol = index_ticker
    last_minute = resample_tick(reference_timestamp, reference_price, symbol, qsize, redis, queue_name, last_minute)

    return last_minute

def resample_options_tick(options_tick: EquityTrade, last_minute, qsize, options_ticker, redis: direct_redis.DirectRedis, queue_name):
    reference_timestamp = options_tick.timestamp
    reference_price = options_tick.price
    symbol = options_ticker
    last_minute = resample_tick(reference_timestamp, reference_price, symbol, qsize, redis, queue_name, last_minute)

    return last_minute
