# Databricks notebook source
import plotly.graph_objects as go

# COMMAND ----------

# Process Data
df = spark.read.table("dev.sandbox.slv_asset_prices_static")
df = df.sort("created_at")
df = df.select("created_at", "base_symbol", "value", "_bronze_at", "_silver_at")
df = df.toPandas()
df = df.iloc[6:]  # Zoom on more recent points
df["latency"] = (df["_silver_at"] - df["created_at"]).dt.total_seconds()
display(df)

# Plot Data
fig = go.Figure()
t = go.Scatter(x=df["created_at"], y=df["latency"], mode="markers+lines")
fig.add_trace(t)
fig.layout.update(xaxis_title="timestamp", yaxis_title="latency (seconds)")
display(fig)

# COMMAND ----------

# Process Data
df = spark.read.table("dev.sandbox.slv_asset_prices")
df = df.sort("created_at")
df = df.select("created_at", "base_symbol", "value", "_bronze_at", "_silver_at")
df = df.toPandas()
df["latency"] = (df["_silver_at"] - df["created_at"]).dt.total_seconds()
df = df.iloc[6:]
df = df.iloc[:-2]
display(df)

# Plot Data
fig = go.Figure()
t = go.Scatter(x=df["created_at"], y=df["latency"], mode="markers+lines")
fig.add_trace(t)
fig.layout.update(xaxis_title="timestamp", yaxis_title="latency (seconds)")
display(fig)
