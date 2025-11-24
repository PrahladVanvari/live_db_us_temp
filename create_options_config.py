from polygon import RESTClient
from typing import List
import json
from collections import defaultdict
from datetime import timedelta
from direct_redis import DirectRedis
from polygon.rest.models import IndicesSnapshot, OptionsContract
from config import POLYGON_API_KEY, TODAY
# from trader import broker_config
from datetime import datetime

client = RESTClient(POLYGON_API_KEY)
redis = DirectRedis(db=0)
# Loading config
with open("/srv/US_ResearchFramework/US_Execution/US_Live_Framework/live_db/create_options_config.json", "r") as file:
    config = json.load(file)

# --------------- Getting tickers to subscribe -----------
# tickers = ["I:SPX", "I:DJI", "I:VIX"]
# tickers = []
# with open('options_config_tickers.txt', 'r') as file:
#     for line in file:
#         print(line.strip())  # .strip() removes the newline character
#         tickers.append(line.strip())

tickers = config["index_tokens"]
strike_threshold = config["strike_threshold_percent"]
number_of_queues = config["number_of_queues"]
config_file_path = config["config_file_path"]
num_of_dtes = 3
dte_list = config["dte_list"]

print(f"Config: Ticker[{tickers}], StrikeThreshold: {strike_threshold}, number_of_queues: {number_of_queues}, config_file_path: {config_file_path}")

print(f"Tickers {tickers}")
    
#  ------------- Getting the index strikes ----------------
def get_index_price_map(tickers):
    index_value_map = {}
    
    index_tickers_to_subscribe = [ f"I:{ticker}" for ticker in tickers ]
    snapshots: List[IndicesSnapshot] = client.get_snapshot_indices(index_tickers_to_subscribe) # type: ignore
    # print raw values
    for snapshot in snapshots:
        index_value_map[str(snapshot.ticker)[2:]] = snapshot.value

    return index_value_map

index_value_map = get_index_price_map(tickers)

print(index_value_map)

def convert_symbol_to_tradingclass(ticker):
    if ticker == 'SPX':
        return 'SPXW'
    elif ticker == 'NDX':
        return 'NDXP'
    elif ticker == 'DJI':
        return 'DJI'
    else:
        return ticker

# ------------ Getting the relevant Option tickers -------------
def get_nearest_expiry(target_date, available_expiries):
    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    for expiry in available_expiries:
        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        if expiry_date >= target:
            return expiry_date
    return None  # if no future expiry

def get_option_tickers_for_index(index_ticker, index_strike, threshold_perc=strike_threshold, num_of_dtes: int = 3):
    # contracts = []
    option_tickers = []
    threshold_multiplier = threshold_perc / 100

    strike_gt = index_strike * (1-threshold_multiplier)
    strike_lt = index_strike * (1+threshold_multiplier)

    print(f"Index: {index_ticker}, Strike: {index_strike}, StrikeRange: [{strike_gt, strike_lt}]")
    expiry_list = []
    i = 0
    converted_ticker = convert_symbol_to_tradingclass(index_ticker)
    while i <= num_of_dtes:
        expiries = TODAY + timedelta(days=i)
        expiry_list.append(expiries)
        i+=1

    redis.hset('list_of_expiries', f'{converted_ticker}', expiry_list)

    for contract in client.list_options_contracts(
        underlying_ticker=index_ticker,
        order="asc",
        limit=10,
        sort="ticker",
        expiration_date_lte=(TODAY + timedelta(days=num_of_dtes)).strftime("%Y-%m-%d"),
        strike_price_gte=index_strike * (1-threshold_multiplier),
        strike_price_lte=index_strike * (1+threshold_multiplier)
        ):
        print(contract)
        
        option_contract: OptionsContract = contract #type: ignore
        option_tickers.append(option_contract.ticker)

    return option_tickers
    # print(contracts)

# ------------ Getting the relevant Option tickers -------------
def get_option_tickers_for_dtes(index_ticker, index_strike, threshold_perc=strike_threshold, dte_list=dte_list):
    threshold_multiplier = threshold_perc / 100
    strike_gt = index_strike * (1 - threshold_multiplier)
    strike_lt = index_strike * (1 + threshold_multiplier)
    converted_ticker = convert_symbol_to_tradingclass(index_ticker)

    # get available expiries for this underlying
    expiries = set()
    for contract in client.list_options_contracts(underlying_ticker=index_ticker, limit=1000):
        expiries.add(contract.expiration_date)
    expiries = sorted(expiries)

    tickers = []
    nearest_expiry_list = []

    for dte in dte_list:
        target_date = (TODAY + timedelta(days=dte)).strftime("%Y-%m-%d")
        nearest_expiry = get_nearest_expiry(target_date, expiries)

        if nearest_expiry:  # only if valid expiry found
            nearest_expiry_list.append(nearest_expiry)

            for contract in client.list_options_contracts(
                underlying_ticker=index_ticker,
                order="asc",
                limit=100,
                sort="ticker",
                expiration_date=nearest_expiry,
                strike_price_gte=strike_gt,
                strike_price_lte=strike_lt
            ):
                tickers.append(contract.ticker)

    # --- store full list of expiries in Redis ---
    if nearest_expiry_list:
        redis.hset(
            "list_of_expiries",
            converted_ticker,
            nearest_expiry_list
        )

    return tickers


all_option_tickers = []
for index_ticker in tickers:
    # option_tickers = get_option_tickers_for_index(index_ticker, index_value_map[index_ticker], strike_threshold, num_of_dtes)
    option_tickers = get_option_tickers_for_dtes(index_ticker, index_value_map[index_ticker], strike_threshold)
    all_option_tickers.extend(option_tickers)
    
# -------- Creating the config --------
print(all_option_tickers)

def update_json_file_with_tickers(json_path, tickers, num_queues):
    # Load the existing JSON file
    with open(json_path, 'r') as file:
        data = json.load(file)
    
    # Distribute tickers evenly among the specified number of queues
    new_queues = defaultdict(list)
    # for i, ticker in enumerate(tickers):
    #     key = str((i % num_queues) + 1)
    #     new_queues[key].append(ticker)

    # Initialize queues
    new_queues = {str(i + 1): [] for i in range(num_queues)}

    # Distribute in order (chunked distribution)
    chunk_size = len(tickers) // num_queues
    remainder = len(tickers) % num_queues
    start = 0

    for i in range(num_queues):
        end = start + chunk_size + (1 if i < remainder else 0)  # spread remainder evenly
        key = str(i + 1)
        new_queues[key].extend(tickers[start:end])
        start = end

    # Update the 'options_queues' section
    data["options_queues"] = dict(new_queues)

    # Save the updated JSON back to the file
    with open(json_path, 'w') as file:
        json.dump(data, file, indent=2)

    print(f"Updated {json_path} successfully.")

# import re
# OCC_REGEX = re.compile(r"O:([A-Z]+)(\d{6})([CP])(\d{8})")


# def extract_strike(ticker: str) -> float:
#     """Extract strike from OCC ticker (last 8 digits / 1000)."""
#     m = OCC_REGEX.match(ticker)
#     if not m:
#         return 0.0
#     return int(m.group(4)) / 1000.0


# def update_json_file_with_tickers(json_path, tickers, num_queues, atm_strike):
def update_json_file_with_tickers(json_path, tickers, num_queues):
    # Load the existing JSON file
    with open(json_path, 'r') as file:
        data = json.load(file)

    # -------------------------
    # 1. Sort by ATM proximity
    # -------------------------
    # tickers_sorted = sorted(tickers, key=lambda t: abs(extract_strike(t) - atm_strike))
    tickers_sorted = sorted(tickers)

    # -------------------------
    # 2. Round-robin distribute
    # -------------------------
    queues = {str(i + 1): [] for i in range(num_queues)}
    idx = 0

    for t in tickers_sorted:
        key = str((idx % num_queues) + 1)
        queues[key].append(t)
        idx += 1

    # -------------------------
    # 3. Update JSON structure
    # -------------------------
    data["options_queues"] = queues

    with open(json_path, 'w') as file:
        json.dump(data, file, indent=2)

    print(f"Updated {json_path} successfully.")


# def create_conids_json():
#     choice = input("Confirm IBKR Login (Y/N): ")
#     if choice.upper() == 'Y':
#         print(f'Creating IBKR config...')
#         broker_config.create_map_for_subscriptions()
#     elif choice.upper() == 'N':
#         print("Login Not Initiated. Exiting.")
#         return
#     else:
#         print("Typo in Login Choice. Exiting.")
#         return

if __name__ == "__main__":
    update_json_file_with_tickers(
        json_path=config_file_path,
        tickers=all_option_tickers,
        num_queues=number_of_queues,
    )
    # create_conids_json()

