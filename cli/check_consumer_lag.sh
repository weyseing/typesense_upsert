# ---------------------
# Notes
# ---------------------
# need to set env below
#   - CLOUD_API_KEY
#   - CLOUD_API_SECRET
#   - KAFKA_CLUSTER
#   - CONSUMER_GROUP_ID

AUTH_HEADER=$(echo -n "${CLOUD_API_KEY}:${CLOUD_API_SECRET}" | base64 | tr -d '\n')

curl --location 'https://api.telemetry.confluent.cloud/v2/metrics/cloud/query' \
--header 'Content-Type: application/json' \
--header "Authorization: Basic ${AUTH_HEADER}" \
--data "{
    \"aggregations\": [
        {
            \"metric\": \"io.confluent.kafka.server/consumer_lag_offsets\"
        }
    ],
    \"filter\": {
        \"op\": \"AND\",
        \"filters\": [
            {
                \"field\": \"resource.kafka.id\",
                \"op\": \"EQ\",
                \"value\": \"${KAFKA_CLUSTER}\"
            },
            {
                \"field\": \"metric.consumer_group_id\",
                \"op\": \"EQ\",
                \"value\": \"${CONSUMER_GROUP_ID}\"
            }
        ]
    },
    \"granularity\": \"PT15M\",
    \"group_by\": [
        \"metric.consumer_group_id\"
    ],
    \"intervals\": [
        \"2025-08-19T00:00:00Z/now\"
    ],
    \"limit\": 25
}"
