import os
import json
import requests
import pandas as pd
from tabulate import tabulate
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# auth
API_KEY = os.environ['CLOUD_API_KEY']
API_SECRET = os.environ['CLOUD_API_SECRET']
AUTH = (API_KEY, API_SECRET)

# cluster
URL = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query"
KAFKA_CLUSTER = os.environ['KAFKA_CLUSTER']

# timeframe, topic, consumer
TIME_FRAME = "2025-08-28T16:00:00Z/now"     # "2025-08-20T16:00:00Z/2025-08-21T08:00:00Z"    OR     "PT3H/now"
GRANULARITY = "PT1H"
TOPIC = "typesense_txn_stream_parsed_txn"
CONSUMER_GROUP_ID = "connect-sink_http.typesense_txn_stream_parsed_txn_20250829-014031"

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

def display_styled_dataframe(df, title):
    print(f"\n--- {title} ---")
    print(tabulate(df, headers='keys', tablefmt='fancy_grid'))

def display_dataframe_as_table(df, title):
    print(f"\n--- {title} ---")
    print(tabulate(df, headers='keys', tablefmt='fancy_grid'))

def plot_metrics(df, output_path, title):
    # handle date type
    df['consumption_bytes'] = df['consumption_bytes'].astype(str).str.replace(',', '').astype(float)
    df['production_bytes'] = df['production_bytes'].astype(str).str.replace(',', '').astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    plt.figure(figsize=(12, 6))
    plt.plot(df['timestamp'], df['consumption_bytes'], label='Consumption Bytes', color='dodgerblue', linewidth=8, alpha=0.5)
    plt.plot(df['timestamp'], df['production_bytes'], label='Production Bytes', color='green', linewidth=1.5, linestyle=(0, (3, 1)))

    plt.title(title)
    plt.xlabel("Time")
    plt.ylabel("Bytes")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # y-axes scale
    y_min = min(df['consumption_bytes'].min(), df['production_bytes'].min())
    y_max = max(df['consumption_bytes'].max(), df['production_bytes'].max())
    plt.ylim(y_min * 0.95, y_max * 1.05)

    def human_format(num, pos):
        if abs(num) >= 1e9:
            return f'{num/1e9:.1f} G'
        if abs(num) >= 1e6:
            return f'{num/1e6:.1f} M'
        if abs(num) >= 1e3:
            return f'{num/1e3:.1f} K'
        return f'{num:.0f}'
    plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(human_format))

    plt.savefig(output_path)

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

def main():
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

    # prepare consumption & production df
    if not df_consumption.empty and not df_production.empty:
        # rename
        df_consumption = df_consumption.rename(columns={"value": "consumption_bytes"})
        df_production = df_production.rename(columns={"value": "production_bytes"})
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
        # drop column
        df_merged.drop(columns=["metric.topic"], inplace=True)
    
    # prepare consumer lag df
    if not df_lag.empty:
        df_lag = df_lag.rename(columns={"value": "consumer_lag"})
        df_lag.drop(columns=["metric.consumer_group_id"], inplace=True)

    # display
    if not df_merged.empty and not df_lag.empty:
        final_df = pd.merge(df_merged, df_lag, on="timestamp", how="outer")
        final_df = format_thousands(final_df, ["consumption_bytes", "production_bytes", "offset", "consumer_lag"])
        display_dataframe_as_table(final_df, "Kafka Metrics Over Time")
        plot_metrics(final_df, "/app/temp/topic_metric.png", "Kafka Consumption and Production Bytes")

if __name__ == "__main__":
    main()