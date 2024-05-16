# Databricks notebook source
# MAGIC %pip install laktory

# COMMAND ----------

import requests
import time
import datetime
from laktory import models

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

base_asset_symbol = "BTC"
quote_asset_symbol = "USD"
pair_symbol = f"{base_asset_symbol}-{quote_asset_symbol}"


def get_btc_price():
    url = "https://min-api.cryptocompare.com/data/price"
    params = {
        "fsym": base_asset_symbol,
        "tsyms": quote_asset_symbol,
    }
    tstamp = datetime.datetime.utcnow()
    data = requests.get(url, params=params).json()
    return (tstamp, data[quote_asset_symbol])


for i in range(12 * 60):

    # ----------------------------------------------------------------------- #
    # Fetch data                                                              #
    # ----------------------------------------------------------------------- #

    tstamp, price = get_btc_price()
    print(f"BTC price @ {tstamp}: {price}")

    event = models.DataEvent(
        name="asset_price",
        producer=models.DataProducer(name="cryptocompare"),
        data={
            "created_at": tstamp.isoformat(),
            "base_asset_symbol": base_asset_symbol,
            "quote_asset_symbol": quote_asset_symbol,
            "pair_symbol": pair_symbol,
            "value": price,
        },
    )

    # ----------------------------------------------------------------------- #
    # Write data                                                              #
    # ----------------------------------------------------------------------- #

    event.to_databricks(suffix=pair_symbol, skip_if_exists=True)

    time.sleep(5)
