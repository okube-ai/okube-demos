df = spark.read.table("dev.sandbox.brz_asset_prices")
df.write.saveAsTable("dev.sandbox.brz_asset_prices_static")

df = spark.read.table("dev.sandbox.slv_asset_prices")
df.write.saveAsTable("dev.sandbox.slv_asset_prices_static")

df = spark.read.table("dev.sandbox.gld_asset_ohlc")
df.write.saveAsTable("dev.sandbox.gld_asset_ohlc_static")
