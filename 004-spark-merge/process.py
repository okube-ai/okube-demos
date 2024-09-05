import pandas as pd
import plotly.graph_objs as go

# --------------------------------------------------------------------------- #
# Setup                                                                       #
# --------------------------------------------------------------------------- #

keys = ["method", "is_incremental", "size"]

# --------------------------------------------------------------------------- #
# Read & Process Data                                                         #
# --------------------------------------------------------------------------- #

df = pd.read_csv("metrics.csv")

df = df.groupby(keys).mean().reset_index()

# Labels
df["labels"] = df["size"].replace(
    {
        1000: "1K",
        10000: "10K",
        100000: "100K",
        1000000: "1M",
        10000000: "10M",
        100000000: "100M",
        1000000000: "1B",
    }
)

# Series
df["series"] = df["method"]
loc = df["is_incremental"]
df.loc[loc, "series"] = "increment " + df.loc[loc, "series"]
loc = ~df["is_incremental"]
df.loc[loc, "series"] = "dump " + df.loc[loc, "series"]

df["series"] = df["series"].replace("increment merge", "increment merge (CDF)")

# --------------------------------------------------------------------------- #
# Plot Data                                                                   #
# --------------------------------------------------------------------------- #

series_names = [
    "increment append",
    "increment merge (CDF)",
    "dump overwrite",
    "dump merge",
]

fig = go.Figure()
for k in series_names:
    _df = df[df["series"] == k]
    trace = go.Bar(x=_df["labels"], y=_df["duration"], name=k)
    fig.add_trace(trace)

fig.update_layout(
    barmode="group",
    template="plotly_white",
)
fig.layout.legend.update(orientation="h")
fig.layout.xaxis.update(title="Number of rows")
fig.layout.yaxis.update(title="Duration [sec]")
fig.write_html("metrics_absolute.html", auto_open=True)


fig = go.Figure()
for k in series_names:
    _df = df[df["series"] == k].sort_values("size")
    x = _df["labels"].values
    y = _df["duration"].values
    s = df[df["series"] == "dump overwrite"].sort_values("size")["duration"].values
    trace = go.Bar(x=_df["labels"], y=y / s, name=k)
    fig.add_trace(trace)

fig.update_layout(
    barmode="group",
    template="plotly_white",
)
fig.layout.legend.update(orientation="h")
fig.layout.xaxis.update(title="Number of rows")
fig.layout.yaxis.update(title="Scaled Duration")
fig.write_html("metrics_normalized.html", auto_open=True)
