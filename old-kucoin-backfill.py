"""
1. Initiate KuCoin and load all available symbols and timeframes.
2. Load PyStore Store and get collection in question.
3. For each kucoin symbol, check database for items
4. If it doesnt exist, fetch today.
4.1 For time delta, multiple length of result time interval. Adjust since using this number. 
5. Append to dataframe until no results are received
6. Save Item, including metadata source, start, end
"""

import ccxt
import datetime
import pandas as pd
import pystore
import os
storage_dir = os.path.abspath(os.path.curdir + '/storage')
kucoin = ccxt.kucoin({'apiKey': '', 'secret': '', 'password': ''})
kucoin.rateLimit = 60
print(kucoin.rateLimit)
balance = kucoin.fetchBalance()
symbols = kucoin.symbols
all_usdt_symbols = list(filter(lambda x: x.endswith("USDT"), symbols))

current_symbols = []
data = {}
pystore.set_path(storage_dir)
store = pystore.store('capa_store')
collection = store.collection('Crypto.Candles.Min')
collections = store.list_collections()
items = collection.list_items()

for symbol in all_usdt_symbols:
    items = collection.list_items()
    if symbol[0:-5] not in items:
        print("getting", symbol)
        minute_candles = []
        since = kucoin.parse8601('2019-01-01T00:00:00Z')
        while since < kucoin.milliseconds():
            print("getting", since)
            page = kucoin.fetchOHLCV(symbol, '1m', since)
            if len(page) > 1:
                since = page[-1][0]
                minute_candles += page
            else:   
                since += 60*1000*60*24
        if len(minute_candles) > 1:
            export = pd.DataFrame(minute_candles)
            export = export.rename(columns={0:'timestamp', 1:'open', 2:'high', 3:'low', 4:'close', 5:'volume'})
            export['timestamp'] = pd.to_datetime(export["timestamp"], unit="ms")
            export = export.set_index('timestamp')
            # export['symbol'] = symbol
            # export = export.set_index(['timestamp', 'symbol'])
            print(len(export), symbol, export)
            collection.write(symbol, export, metadata={'source':'kucoin'})
