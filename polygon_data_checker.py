import time
from datetime import datetime, timedelta, date
import direct_redis
import json

from config import POLYGON_API_KEY
from config import MARKET_CLOSE_TIME, TODAY
from config import get_our_options_ticker, get_our_index_ticker

from polygon import RESTClient
from polygon.rest.models import IndicesSnapshot, OptionsContract

client = RESTClient(POLYGON_API_KEY)

# tg = Comms()
# di = DataInterface()

# Connect to Redis
r = direct_redis.DirectRedis(host='localhost', port=6379, db=0)

def round_to_minute(dt):
    return dt.replace(second=0, microsecond=0)

# def check_symbol(symbol):
#     now = datetime.now()
#     current_minute = round_to_minute(now)
#     current_key_time = current_minute.strftime('%Y-%m-%d %H:%M:%S')
#     current_key = f"l.tick_{current_key_time}"

#     current_ltp = r.hget(current_key, symbol)

#     if current_ltp is None :
#         print(f"⚠️ ALERT: [{symbol}] Missing data for {current_key}")
#         tg.ping(f"⚠️ ALERT: [{symbol}] Missing data for {current_key}")

# def run_check():
#     """
#     1) collect index names
#     2) collect expiries we are tracking
#     3) find strikes of all indexes
#     4) find appropriate atm strike for each index
#     5) construct symbol name for each index -> atm call, atm put
#     6) check if we have data for it in the db.
#     """
    
#     print(f'Checking for missing data. {datetime.now()}')
#     symbols = ['NIFTYSPOT', 'SENSEXSPOT']
#     updated_symbols = ['NIFTYSPOT', 'SENSEXSPOT']
#     for sym in symbols:
#         atm_symbol_ce = di.find_symbol_by_moneyness(datetime.now().replace(second=0, microsecond=0), sym[:-4], 0, 'CE', 0)
#         atm_symbol_pe = di.find_symbol_by_moneyness(datetime.now().replace(second=0, microsecond=0), sym[:-4], 0, 'PE', 0)
        
#         updated_symbols.append(atm_symbol_ce)
#         updated_symbols.append(atm_symbol_pe)
#         # print(f'Atm symbols generated : {atm_symbol_ce}, {atm_symbol_pe}')
        
#     for symbol in updated_symbols:
#         check_symbol(symbol)
#         time.sleep(0.1)
        
#     print(f'Check complete. {datetime.now()}')

def wait_until_30th_second():
    while True:
        now = datetime.now()
        if (now.second == 30) or (now.second == 31):
            return
        time.sleep(0.1)


def get_index_names() -> list[str]:
    # Extend this list as needed
    # return ["SPX", "NDX", "DJI"]
    return ["SPX"]

# def get_expiry_dates(num_of_dtes: int, num_expiries: int = 1) -> list[str]:
#     return [
#         (TODAY + timedelta(days=num_of_dtes + i)).strftime("%Y-%m-%d")
#         for i in range(num_expiries)
#     ]

# def get_expiry_dates(ticker) -> list[str]:
#     expiries = r.hget('list_of_expiries', ticker)
#     return expiries

def get_expiry_dates(ticker) -> list[date]:
    expiries_json = r.hget('list_of_expiries', ticker)
    if expiries_json:
        # Parse ISO strings back into date objects
        return [datetime.strptime(d, "%Y-%m-%d").date() for d in json.loads(expiries_json)]
    return []

def get_index_prices(tickers: list[str]) -> dict[str, float]:
    index_value_map = {}
    index_tickers = [f"I:{ticker}" for ticker in tickers]
    snapshots: list[IndicesSnapshot] = list(client.get_snapshot_indices(index_tickers)) # type: ignore
    for snapshot in snapshots:
        index_value_map[str(snapshot.ticker)[2:]] = snapshot.value
    return index_value_map

def get_atm_strike_for_index(index_ticker: str, index_strike: float, expiry: str | date) -> tuple[float, OptionsContract]:
    # Get the contract just above
    contract_above: OptionsContract = next(client.list_options_contracts(
        underlying_ticker=index_ticker,
        order="asc",
        limit=1,
        sort="strike_price",
        expiration_date=expiry,
        strike_price_gte=index_strike
    ), None)    # type: ignore

    # Get the contract just below
    contract_below: OptionsContract = next(client.list_options_contracts(
        underlying_ticker=index_ticker,
        order="desc",
        limit=1,
        sort="strike_price",
        expiration_date=expiry,
        strike_price_lte=index_strike
    ), None)    # type: ignore

    if not contract_above or not contract_below:
        return None, None  # Error case

    # Choose closer
    diff_above = abs(contract_above.strike_price or 0 - index_strike)
    diff_below = abs(contract_below.strike_price or 0 - index_strike)

    if diff_above < diff_below:
        atm_contract = contract_above
    else:
        atm_contract = contract_below

    return atm_contract.strike_price or 0, atm_contract

def construct_atm_option_symbols(index_ticker: str, expiry: str | date, strike: float) -> list[str]:
    # # Format strike (e.g., 6410 -> "06410000")
    # strike_str = f"{int(strike):08d}"

    # # Format expiry
    # expiry_formatted = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%m%d")

    # call_symbol = f"O:{index_ticker}W{expiry_formatted}C{strike_str}"
    # put_symbol = f"O:{index_ticker}W{expiry_formatted}P{strike_str}"
    # return call_symbol, put_symbol

    # contracts = []
    contract_symbols = []
    for contract in client.list_options_contracts(
        underlying_ticker=index_ticker,
        order="asc",
        limit=10,
        sort="strike_price",
        expiration_date=expiry,
        strike_price=strike,
        ):
        # print(contract)
        ctr: OptionsContract = contract # type: ignore
        contract_symbols.append(ctr.ticker)

    return contract_symbols

def check_symbol(symbol: str):
    now = datetime.now()
    current_minute = round_to_minute(now)
    current_key_time = current_minute
    # current_key_time = current_minute.strftime('%Y-%m-%d %H:%M:%S')
    current_key = f"l.tick_{current_key_time}"

    current_ltp = r.hget(current_key, symbol)

    if current_ltp is None :
        print(f"⚠️ ALERT: [{symbol}] Missing data for {current_key}")
        # tg.ping(f"⚠️ ALERT: [{symbol}] Missing data for {current_key}")
        return False
    return True

def run_checker():
    index_names = get_index_names()
    print(f"index_names: {index_names}")
    # expiry_dates = get_expiry_dates(num_of_dtes=0, num_expiries=1)

    index_prices = get_index_prices(index_names)
    print(f"index_prices: {index_prices}")

    for index in index_names:
        internal_ticker = get_our_index_ticker(f"I:{index}")
        expiry_dates = get_expiry_dates(ticker=internal_ticker)
        print(f"{index}=> expiry_dates: {expiry_dates}")

        for expiry in expiry_dates:
            if index not in index_prices:
                print(f"Price not found for index: {index}")
                continue

            strike_price, atm_contract = get_atm_strike_for_index(index, index_prices[index], expiry)
            if not atm_contract:
                print(f"No ATM contract found for {index} at expiry {expiry}")
                continue

            atm_contracts = construct_atm_option_symbols(index, expiry, strike_price)
            print(f"{index} ATM Contracts {atm_contracts}")

            for symbol in atm_contracts:
                internal_symbol = get_our_options_ticker(symbol)
                if check_symbol(internal_symbol):
                    print(f"Data exists in DB for: {internal_symbol}")
                else:
                    print(f"No data in DB for: {internal_symbol}")

# Main loop: runs every minute at 30th second
while True:
    wait_until_30th_second()
    run_checker()
    # Sleep the remaining time until next 30th second
    # time.sleep(60 - datetime.now().second)
    time.sleep(1)
    
    # if datetime.now() > MARKET_CLOSE_TIME.replace(minute=30):
    #     print('Market Closed')
    #     break
