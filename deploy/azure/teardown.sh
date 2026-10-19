#!/usr/bin/env bash
# Delete everything deploy.sh created (one resource group), so nothing keeps costing credit.
set -euo pipefail
RG="${RG:-rg-media-spend-platform}"
az group delete --name "$RG" --yes --no-wait
echo "Deleting resource group $RG in the background (takes a few minutes)."
