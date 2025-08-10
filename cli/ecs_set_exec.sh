#!/bin/sh

REGION="ap-southeast-1"
CLUSTER_ARN="arn:aws:ecs:ap-southeast-1:107698500998:cluster/typesense-api"
SERVICE_ARN="arn:aws:ecs:ap-southeast-1:107698500998:service/typesense-api/typesense-api"

# load env
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

# Describe service
echo " ----- CHECKING CONFIG ----- "
aws ecs describe-services \
    --region "$REGION" \
    --cluster "$CLUSTER_ARN" \
    --services "$SERVICE_ARN" | jq .

# Enable execute command
echo " ----- UPDATE CONFIG ----- "
aws ecs update-service \
    --region "$REGION" \
    --cluster "$CLUSTER_ARN" \
    --service "$SERVICE_ARN" \
    --enable-execute-command | jq .