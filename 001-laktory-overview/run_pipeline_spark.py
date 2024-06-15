from databricks.connect import DatabricksSession
from laktory import models

spark = DatabricksSession.builder.getOrCreate()


# Read Pipeline
with open("./pipeline_spark_dlt.yaml", "r") as fp:
    pl = models.Pipeline.model_validate_yaml(fp)

# Execute
pl.execute(spark=spark, write_sinks=False)

# Preview
df = pl.nodes[-1].output_df
df.show()
