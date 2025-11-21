from dotenv import load_dotenv
import os
import json
from datetime import datetime, time, timedelta
from collections import defaultdict

load_dotenv()  # load variables from .env

POLYGON_API_KEY = 'b14hrfXWHdEUrqnOxXpLJMNWAok8aE4t'  #os.getenv("POLYGON_API_KEY")

if not POLYGON_API_KEY:
    raise ValueError("POLYGON_API_KEY not found in environment variables")

# Get today's date
TODAY = datetime.today().date()
TOMORROW = TODAY + timedelta(days=1)
MARKET_CLOSE_TIME = datetime.combine(TODAY, time(hour=16, minute=5)) # 5 extra buffer minutes
# MARKET_CLOSE_TIME = datetime.combine(TODAY, time(hour=11, minute=12))

# --------------- Loading Tokens -------------------
with open("./subscriptions.json", "r") as file:
    subs_config = json.load(file)

INDEX_QUEUE_DICT = {key: tuple(value) for key, value in subs_config["index_queues"].items()}
REVERSED_INDEX_QUEUE_DICT = {value: key for key, values in INDEX_QUEUE_DICT.items() for value in values}

OPTIONS_QUEUE_DICT = {key: tuple(value) for key, value in subs_config["options_queues"].items()}
REVERSED_OPTIONS_QUEUE_DICT = {value: key for key, values in OPTIONS_QUEUE_DICT.items() for value in values}

# Merge into a combined dict with appended values
combined = defaultdict(list)

for key, value in INDEX_QUEUE_DICT.items():
    combined[key].extend(value)
for key, value in OPTIONS_QUEUE_DICT.items():
    combined[key].extend(value)

# Convert lists to tuples (optional)
MASTER_QUEUE_DICT = {key: tuple(values) for key, values in combined.items()}
REVERSED_MASTER_QUEUE_DICT = {value: key for key, values in MASTER_QUEUE_DICT.items() for value in values}


# Storing token's internal representation
with open("./symbol_repr.json", "r") as file:
    symbol_repr_config = json.load(file)
    
repr_index = symbol_repr_config['index_repr']

def get_our_index_ticker(external_ticker: str):
    return repr_index.get(external_ticker, external_ticker)

def get_our_options_ticker(external_ticker: str):
    return external_ticker[2:]