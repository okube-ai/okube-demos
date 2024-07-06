import polars as pl
from laktory import models


# --------------------------------------------------------------------------- #
# Build Pipeline                                                              #
# --------------------------------------------------------------------------- #

def custom_transformer_node(df):
    df = df.rename({c: c.upper() for c in df.columns})
    df.with_columns(OPEN2=pl.sqrt("OPEN"))
    return df


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

stocks_slv = models.PipelineNode(
    name="slv_stock_prices",
    layer="SILVER",
    source=models.PipelineNodeDataSource(
        node_name=stocks_brz.name,
    ),
    sink=models.FileDataSink(
        path="./data/slv_stock_prices.parquet",
        format="PARQUET",
    ),
    transformer=models.PolarsChain(nodes=[
        models.PolarsChainNode(with_columns=[
            models.PolarsChainNodeColumn(
                name="created_at",
                type="timestamp",
                sql_expr="data.created_at",
            ),
            models.PolarsChainNodeColumn(
                name="symbol",
                sql_expr="data.symbol",
            ),
            models.PolarsChainNodeColumn(
                name="name",
                sql_expr="data.symbol",
            ),
            models.PolarsChainNodeColumn(
                name="open",
                type="double",
                sql_expr="data.open",
            ),
            models.PolarsChainNodeColumn(
                name="close",
                type="double",
                sql_expr="data.close",
            ),
            models.PolarsChainNodeColumn(
                name="low",
                type="double",
                sql_expr="data.high",
            ),
        ]),
        models.PolarsChainNode(
            func_name="capitalize_columns",
        )
    ])
)

pl = models.Pipeline(
    name="pl-sample-001",
    dataframe_type="POLARS",
    nodes=[
        stocks_brz,
        stocks_slv,
    ]
)

# --------------------------------------------------------------------------- #
# Review Pipeline                                                             #
# --------------------------------------------------------------------------- #

fig = pl.dag_figure()
fig.write_html("./dag.html", auto_open=True)

# --------------------------------------------------------------------------- #
# Execute                                                                     #
# --------------------------------------------------------------------------- #

pl.execute(udfs=[custom_transformer_node])

# Preview
df = pl.nodes[-1].output_df
print(df)
