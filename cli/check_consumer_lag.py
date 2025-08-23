import os
import requests
import json
import pandas as pd

# auth
API_KEY = os.environ['CLOUD_API_KEY']
API_SECRET = os.environ['CLOUD_API_SECRET']
AUTH = (API_KEY, API_SECRET)

# cluster
URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
KAFKA_CLUSTER = os.environ['KAFKA_CLUSTER']

# timeframe, topic, consumer
TIME_FRAME = "2025-08-22T23:00:00Z/now"     # "2025-08-20T16:00:00Z/2025-08-21T08:00:00Z"    OR     "PT3H/now"
GRANULARITY = "PT1H"
TOPIC = "typesense_txn_stream_parsed_txn"
CONSUMER_GROUP_ID = os.environ.get('CONSUMER_GROUP_ID', '')

def format_thousands(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: '{:,.0f}'.format(x) if pd.notnull(x) else x)
    return df

def query_metric(metric, group_by, additional_filters=None):
    filters = [
        {
            "field": "resource.kafka.id",
            "op": "EQ",
            "value": KAFKA_CLUSTER
        }
    ]
    if additional_filters:
        filters.extend(additional_filters)
    data = {
        "aggregations": [{"metric": metric}],
        "filter": {
            "op": "AND",
            "filters": filters
        },
        "granularity": GRANULARITY,
        "group_by": group_by,
        "intervals": [TIME_FRAME],
        "limit": 100
    }
    response = requests.post(URL, auth=AUTH, headers={"Content-Type": "application/json"}, data=json.dumps(data))
    response.raise_for_status()
    return response.json().get("data", [])

# Consumer lag
lag_data = query_metric(
    "io.confluent.kafka.server/consumer_lag_offsets",
    ["metric.consumer_group_id"],
    additional_filters=[
        {
            "field": "metric.consumer_group_id",
            "op": "EQ",
            "value": CONSUMER_GROUP_ID
        }
    ] 
)

# Consumption
consumption_data = query_metric(
    "io.confluent.kafka.server/received_bytes",
    ["metric.topic"],
    additional_filters=[
        {
            "field": "metric.topic",
            "op": "EQ",
            "value": TOPIC
        }
    ]
)

# Production
production_data = query_metric(
    "io.confluent.kafka.server/sent_bytes",
    ["metric.topic"],
    additional_filters=[
        {
            "field": "metric.topic",
            "op": "EQ",
            "value": TOPIC
        }
    ]
)

# Convert DataFrames
df_lag = pd.DataFrame(lag_data)
df_consumption = pd.DataFrame(consumption_data)
df_production = pd.DataFrame(production_data)

# Rename columns
if not df_lag.empty:
    df_lag = df_lag.rename(columns={"value": "consumer_lag"})
if not df_consumption.empty:
    df_consumption = df_consumption.rename(columns={"value": "consumption_bytes"})
if not df_production.empty:
    df_production = df_production.rename(columns={"value": "production_bytes"})

# display consumer lag
if not df_lag.empty:
    df_lag = format_thousands(df_lag, ["consumer_lag"])
    print("\nConsumer Lag:")
    print(df_lag[["timestamp", "metric.consumer_group_id", "consumer_lag"]])

# display consumption & production
if not df_consumption.empty and not df_production.empty:
    # merge
    df_merged = pd.merge(
        df_consumption[["timestamp", "metric.topic", "consumption_bytes"]],
        df_production[["timestamp", "metric.topic", "production_bytes"]],
        on=["timestamp", "metric.topic"],
        how="outer"
    )
    # offset
    df_merged["consumption_bytes"] = pd.to_numeric(df_merged["consumption_bytes"], errors="coerce")
    df_merged["production_bytes"] = pd.to_numeric(df_merged["production_bytes"], errors="coerce")
    df_merged["offset"] = df_merged["consumption_bytes"] - df_merged["production_bytes"]
    df_merged = format_thousands(df_merged, ["consumption_bytes", "production_bytes", "offset"])
    print("\nConsumption and Production:")
    print(df_merged)

