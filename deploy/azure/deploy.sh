#!/usr/bin/env bash
# Deploy the pipeline to Azure as a scheduled Container Apps Job.
#
# Run from the repo root in Azure Cloud Shell (Bash), which already has the az CLI
# and git, and is already signed in:
#   git clone https://github.com/RuikeYuan/media-spend-platform && cd media-spend-platform
#   bash deploy/azure/deploy.sh
#
# What it creates (all in one resource group, so teardown.sh removes everything):
#   - Azure Container Registry (Basic)   builds the image in the cloud, no local Docker
#   - Container Apps environment          consumption plan, with Log Analytics for logs
#   - Container Apps Job                  runs `msp demo` daily at 06:00 UTC, plus one run now
# Re-running the script is safe: names are derived from your subscription id.
set -euo pipefail

LOCATION="${LOCATION:-westeurope}"
RG="${RG:-rg-media-spend-platform}"
SUFFIX="$(az account show --query id -o tsv | tr -d '-' | cut -c1-8)"
ACR="${ACR:-acrmsp${SUFFIX}}"
ENV_NAME="${ENV_NAME:-cae-media-spend}"
JOB="${JOB:-job-media-spend-pipeline}"
IMAGE="msp:$(git rev-parse --short HEAD 2>/dev/null || echo latest)"
CRON="${CRON:-0 6 * * *}"

say() { printf '\n\033[1m▶ %s\033[0m\n' "$1"; }

say "Subscription: $(az account show --query name -o tsv)  |  region: $LOCATION"

say "Registering resource providers (first time on a new subscription takes a few minutes)"
for ns in Microsoft.App Microsoft.OperationalInsights Microsoft.ContainerRegistry; do
  az provider register --namespace "$ns" --wait --output none
done
az extension add --name containerapp --upgrade --yes --output none 2>/dev/null || true

say "Resource group $RG"
az group create --name "$RG" --location "$LOCATION" --output none

say "Container registry $ACR"
if ! az acr show --name "$ACR" --resource-group "$RG" --output none 2>/dev/null; then
  az acr create --name "$ACR" --resource-group "$RG" --location "$LOCATION" \
    --sku Basic --admin-enabled true --output none
fi

say "Building image $IMAGE in Azure (ACR Tasks)"
az acr build --registry "$ACR" --image "$IMAGE" --image "msp:latest" . --output none

say "Container Apps environment $ENV_NAME"
if ! az containerapp env show --name "$ENV_NAME" --resource-group "$RG" --output none 2>/dev/null; then
  az containerapp env create --name "$ENV_NAME" --resource-group "$RG" --location "$LOCATION" --output none
fi

ACR_SERVER="$(az acr show --name "$ACR" --query loginServer -o tsv)"
ACR_USER="$(az acr credential show --name "$ACR" --query username -o tsv)"
ACR_PASS="$(az acr credential show --name "$ACR" --query 'passwords[0].value' -o tsv)"

say "Scheduled job $JOB (cron: $CRON UTC)"
if az containerapp job show --name "$JOB" --resource-group "$RG" --output none 2>/dev/null; then
  az containerapp job update --name "$JOB" --resource-group "$RG" \
    --image "$ACR_SERVER/$IMAGE" --output none
else
  az containerapp job create --name "$JOB" --resource-group "$RG" --environment "$ENV_NAME" \
    --trigger-type Schedule --cron-expression "$CRON" \
    --replica-timeout 900 --replica-retry-limit 0 --replica-completion-count 1 --parallelism 1 \
    --image "$ACR_SERVER/$IMAGE" \
    --registry-server "$ACR_SERVER" --registry-username "$ACR_USER" --registry-password "$ACR_PASS" \
    --cpu 0.5 --memory 1Gi \
    --command "msp" --args "demo" \
    --output none
fi

say "Starting one run now"
EXECUTION="$(az containerapp job start --name "$JOB" --resource-group "$RG" --query name -o tsv)"
echo "Execution: $EXECUTION"

for _ in $(seq 1 40); do
  STATUS="$(az containerapp job execution show --name "$JOB" --resource-group "$RG" \
            --job-execution-name "$EXECUTION" --query properties.status -o tsv 2>/dev/null || echo Pending)"
  printf '  status: %s\n' "$STATUS"
  case "$STATUS" in Succeeded|Failed|Stopped|Degraded) break ;; esac
  sleep 15
done

say "Done"
cat <<EOF
Job:        $JOB  (resource group $RG)
Last run:   $EXECUTION -> ${STATUS:-unknown}
Portal:     Container Apps Jobs > $JOB > Execution history (logs appear there after a few minutes)
Run again:  az containerapp job start -n $JOB -g $RG
Remove all: bash deploy/azure/teardown.sh
EOF

[ "${STATUS:-}" = "Succeeded" ]
