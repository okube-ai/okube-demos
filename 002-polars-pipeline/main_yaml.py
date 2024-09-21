import polars as pl
from laktory import models


# --------------------------------------------------------------------------- #
# Pipeline                                                                    #
# --------------------------------------------------------------------------- #

with open("./pipeline.yaml", "r") as fp:
    pipeline = models.Pipeline.model_validate_yaml(fp)

fig = pipeline.dag_figure()
fig.write_html("./dag.html", auto_open=True)

# --------------------------------------------------------------------------- #
# Execute                                                                     #
# --------------------------------------------------------------------------- #

pipeline.execute()


# Preview
df = pipeline.nodes[-1].output_df
print(df)
