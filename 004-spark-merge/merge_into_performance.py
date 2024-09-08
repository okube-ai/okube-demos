import pyspark.sql.functions as F
import pyspark.sql.types as T
import datetime
import time
import numpy as np
import pandas as pd
from delta.tables import DeltaTable
from collections import defaultdict


# COMMAND ----------
def print_done(t0):
    dt = time.time() - t0
    print(f" done. [{dt:5.2f} sec]")
    return dt


class Stocks:
    def __init__(self, nrows=500, nstocks=500):
        self.nstocks = nstocks
        self.nrows = nrows
        self.metrics = defaultdict(lambda: [])

        # Paths
        self.path = "/Volumes/q01/sources/input/tmp/stocks_target"

        # Timestamps
        self.T0 = datetime.datetime(2024, 8, 23)

        # DataFrames
        self._df = self.build_df()

    @property
    def target(self):
        return self._df.filter(~F.col("_is_new"))

    def get_source(self, incremental=True, update_frac=0.01):
        if incremental:
            df = self._df.filter(F.col("_is_new"))
        else:
            df = self._df.select(self._df.columns)
            df = df.withColumn(
                "_is_updated",
                F.col("symbol").isin(
                    [f"S{i:03d}" for i in range(int(update_frac * self.nstocks))]
                ),
            )
        df = df.withColumn("_from", F.lit("source"))
        return df

    @property
    def keys(self):
        return ["symbol", "timestamp"]

    def build_df(self):
        ntstamps = int(self.nrows / self.nstocks)

        # Timestamps
        timestamps0 = pd.date_range(
            datetime.datetime(2020, 8, 23), self.T0, freq="min"
        )[-ntstamps:].tolist()
        timestamps1 = pd.date_range(
            self.T0, self.T0 + datetime.timedelta(days=1), freq="min"
        ).tolist()
        timestamps = timestamps0 + timestamps1
        timestamps = np.unique(timestamps)

        # Symbols
        symbols = [f"S{i:03d}" for i in range(self.nstocks)]

        # Create a schema for the DataFrame
        schema = F.StructType(
            [
                T.StructField("stock_symbol", T.StringType(), True),
                T.StructField("timestamp", T.TimestampType(), True),
                T.StructField("open", T.DoubleType(), True),
                T.StructField("close", T.DoubleType(), True),
                T.StructField("high", T.DoubleType(), True),
                T.StructField("low", T.DoubleType(), True),
            ]
        )

        # Create an empty DataFrame with the schema
        df = spark.createDataFrame([], schema)

        df_symbols = spark.createDataFrame(pd.DataFrame({"symbol": symbols}))
        df_timestamps = spark.createDataFrame(pd.DataFrame({"timestamp": timestamps}))
        df = df_timestamps.crossJoin(df_symbols)

        # Add columns with random numbers
        for c in ["open", "close", "high", "low"]:
            df = df.withColumn(c, F.rand())

        df = df.withColumn("_from", F.lit("target"))
        df = df.withColumn("_is_updated", F.lit(False))
        df = df.withColumn("_is_new", F.col("timestamp") > self.T0)

        return df

    def write_target(self):
        self.target.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(self.path)

    def append(self, incremental=True):
        _k1 = "increment" if incremental else "dump"
        self.write_target()
        source = self.get_source(incremental=incremental)
        # df = get_source(increment)
        t0 = time.time()
        print(f"Processing {_k1} as append...", end="")
        source.write.format("DELTA").mode("append").save(self.path)
        dt = print_done(t0)
        return dt

    def overwrite(self, incremental=True):
        _k1 = "increment" if incremental else "dump"
        if incremental:
            self.write_target()
        source = self.get_source(incremental=incremental)
        t0 = time.time()
        print(f"Processing {_k1} as overwrite...", end="")
        if incremental:
            df0 = spark.read.format("DELTA").load(self.path)
            df = df0.union(source)
        else:
            df = source
        df.write.format("DELTA").mode("overwrite").save(self.path)
        dt = print_done(t0)
        return dt

    def merge(self, incremental=True):
        _k1 = "increment" if incremental else "dump"
        self.write_target()
        source = self.get_source(incremental)
        t0 = time.time()
        print(f"Processing {_k1} as merge...", end="")
        table_target = DeltaTable.forPath(spark, self.path)
        (
            table_target.alias("target")
            .merge(
                source.alias("source"),
                # how="outer",
                " AND ".join([f"source.{c} = target.{c}" for c in self.keys]),
            )
            .whenMatchedUpdate(
                set={
                    f"target.{c}": f"source.{c}"
                    for c in ["open", "close", "high", "low", "_from"]
                },
                condition="source._is_updated = true",
            )
            .whenNotMatchedInsert(
                # values = {f"target.{c}": f"source.{c}" for c in df.columns if not c.startswith("_")}
                values={f"target.{c}": f"source.{c}" for c in source.columns}
            )
        ).execute()
        dt = print_done(t0)
        return dt

    def benchmark(self, sizes, nruns=3):
        self.metrics = defaultdict(lambda: [])
        for size in sizes:
            print(f"Setting target with size: {int(size)}")
            self.nrows = size
            self._df = self.build_df()
            dt = 0
            for incremental in [True, False]:
                if incremental:
                    for irun in range(nruns):
                        dt = stocks.append(incremental)
                        self.metrics["method"].append("append")
                        self.metrics["irun"].append(irun)
                        self.metrics["is_incremental"].append(incremental)
                        self.metrics["size"].append(size)
                        self.metrics["duration"].append(dt)

                if not incremental:
                    for irun in range(nruns):
                        dt = stocks.overwrite(incremental)
                        self.metrics["method"].append("overwrite")
                        self.metrics["irun"].append(irun)
                        self.metrics["is_incremental"].append(incremental)
                        self.metrics["size"].append(size)
                        self.metrics["duration"].append(dt)

                for irun in range(nruns):
                    dt = stocks.merge(incremental)
                    self.metrics["method"].append("merge")
                    self.metrics["irun"].append(irun)
                    self.metrics["is_incremental"].append(incremental)
                    self.metrics["size"].append(size)
                    self.metrics["duration"].append(dt)

                pd.DataFrame(self.metrics).to_csv(
                    "/Volumes/q01/sources/input/tmp/metrics.csv", index=False
                )


stocks = Stocks(nrows=1e7, nstocks=500)

stocks.benchmark(sizes=[1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9], nruns=3)
