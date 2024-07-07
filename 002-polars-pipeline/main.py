import polars as pl
from laktory import models


# --------------------------------------------------------------------------- #
# Pipeline Nodes - Stock Prices                                               #
# --------------------------------------------------------------------------- #

stocks_brz = models.PipelineNode(
    name="brz_stock_prices",
    layer="BRONZE",
    source=models.FileDataSource(
        path="./data/stock_prices.json",
        multiline=True,
    ),
    sink=models.FileDataSink(
        path="./data/brz_stock_prices.parquet",
        format="PARQUET",
    )
)


def process_stocks_slv(df):

    df = df.with_columns(
        created_at=pl.Expr.laktory.sql_expr("data.created_at").cast(pl.datatypes.Datetime),
        symbol=pl.Expr.laktory.sql_expr("data.symbol").cast(pl.datatypes.String),
        name=pl.Expr.laktory.sql_expr("data.symbol").cast(pl.datatypes.String),
        open=pl.Expr.laktory.sql_expr("data.open").cast(pl.datatypes.Float64),
        close=pl.Expr.laktory.sql_expr("data.close").cast(pl.datatypes.Float64),
        low=pl.Expr.laktory.sql_expr("data.low").cast(pl.datatypes.Float64),
        high=pl.Expr.laktory.sql_expr("data.high").cast(pl.datatypes.Float64),
    )

    return df


stocks_slv = models.PipelineNode(
    name="slv_stock_prices",
    layer="SILVER",
    source=models.PipelineNodeDataSource(
        node_name="brz_stock_prices",
    ),
    sink=models.FileDataSink(
        path="./data/slv_stock_prices.parquet",
        format="PARQUET",
    ),
    transformer=models.PolarsChain(nodes=[
        models.PolarsChainNode(
            func_name="process_stocks_slv",
        ),
    ])
)


pipeline = models.Pipeline(
    name="pl-stock-prices",
    dataframe_type="POLARS",
    nodes=[
        stocks_brz,
        stocks_slv,
    ]
)

# Run
pipeline.execute(udfs=[process_stocks_slv])

# Review Data
df = pipeline.nodes[-1].output_df
print(df[["created_at", "symbol", "open", "close"]])


# --------------------------------------------------------------------------- #
# Pipeline Nodes - Stock Metadata                                             #
# --------------------------------------------------------------------------- #

meta_brz = models.PipelineNode(
    name="brz_stock_metadata",
    layer="BRONZE",
    source=models.FileDataSource(
        path="./data/stock_metadata.json",
        multiline=True,
    ),
    sink=models.FileDataSink(
        path="./data/brz_stock_metadata.parquet",
        format="PARQUET",
    )
)

meta_slv = models.PipelineNode(
    name="slv_stock_metadata",
    layer="SILVER",
    source=models.PipelineNodeDataSource(
        node_name="brz_stock_metadata",
    ),
    transformer=models.PolarsChain(nodes=[
        models.PolarsChainNode(with_columns=[
            models.PolarsChainNodeColumn(
                name="symbol",
                sql_expr="data.symbol",
            ),
            models.PolarsChainNodeColumn(
                name="currency",
                sql_expr="data.currency",
            ),
            models.PolarsChainNodeColumn(
                name="first_traded",
                type="timestamp",
                sql_expr="data.firstTradeDate",
            ),
        ])
    ])
)

# --------------------------------------------------------------------------- #
# Pipeline Nodes - Stocks Join                                                #
# --------------------------------------------------------------------------- #

stocks = models.PipelineNode(
    name="slv_stocks",
    layer="SILVER",
    drop_source_columns=False,
    source=models.PipelineNodeDataSource(
        node_name="slv_stock_prices"
    ),
    sink=models.FileDataSink(
        path="./data/slv_stocks.parquet",
        format="PARQUET",
    ),
    transformer=models.PolarsChain(nodes=[
        models.PolarsChainNode(
            func_name="laktory.smart_join",
            func_kwargs={
                "other": models.PipelineNodeDataSource(
                    node_name="slv_stock_metadata",
                    selects=[
                        "symbol",
                        "currency",
                        "first_traded",
                    ],
                ),
                "on": ["symbol"],
            }
        ),
        models.PolarsChainNode(with_columns=[
            models.PolarsChainNodeColumn(
                name="day_id",
                expr='pl.col("created_at").dt.truncate("1d")'
            )
        ])
    ])
)

# --------------------------------------------------------------------------- #
# Pipeline Nodes - Gold Aggregation                                           #
# --------------------------------------------------------------------------- #

gold = models.PipelineNode(
    name="gld_stock_prices_daily",
    layer="GOLD",
    source=models.PipelineNodeDataSource(
        node_name="slv_stocks",
    ),
    sink=models.FileDataSink(
        path="./data/gld_stocks_prices_by_1d.parquet",
        format="PARQUET",
    ),
    transformer=models.PolarsChain(nodes=[
        models.PolarsChainNode(
            func_name="laktory.groupby_and_agg",
            func_kwargs={
                "groupby_columns": ["symbol", "day_id"],
                "agg_expressions": [
                    {
                        "name": "count",
                        "expr": 'pl.col("symbol").count()'
                    },
                    {
                        "name": "low",
                        "expr": 'pl.col("low").min()'
                    },
                    {
                        "name": "high",
                        "expr": 'pl.col("high").max()'
                    },
                    {
                        "name": "open",
                        "expr": 'pl.col("open").first()'
                    },
                    {
                        "name": "close",
                        "expr": 'pl.col("close").last()'
                    },
                ]
            }
        )
    ])
)


# --------------------------------------------------------------------------- #
# Pipeline                                                                    #
# --------------------------------------------------------------------------- #

pipeline = models.Pipeline(
    name="pl-stock-prices",
    dataframe_type="POLARS",
    nodes=[
        stocks_brz,
        stocks_slv,
        meta_brz,
        meta_slv,
        stocks,
        gold
    ]
)

fig = pipeline.dag_figure()
fig.write_html("./dag.html", auto_open=True)

# --------------------------------------------------------------------------- #
# Execute                                                                     #
# --------------------------------------------------------------------------- #

pipeline.execute(udfs=[process_stocks_slv])


# Preview
df = pipeline.nodes[-1].output_df
print(df)


















