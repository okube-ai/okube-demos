from laktory import models

# Read Pipeline
with open("./pipeline_polars.yaml", "r") as fp:
    pl = models.Pipeline.model_validate_yaml(fp)

# Visualize
fig = pl.dag_figure()
fig.write_html("./dag.html", auto_open=True)

# Execute
pl.execute()

# Preview
df = pl.nodes[-1].output_df
print(df)
