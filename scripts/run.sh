#!/usr/bin/env bash
# Start one pipeline run and wait for it. Usage: scripts/run.sh [seasons]   e.g. 2021-current
set -euo pipefail

seasons="${1:-current}"
outputs="build/terraform-outputs.json"
[[ -f "$outputs" ]] || { echo "No $outputs. Run 'make deploy' first." >&2; exit 1; }
state_machine="$(jq -r '.state_machine_arn.value' "$outputs")"

execution="$(aws stepfunctions start-execution \
  --state-machine-arn "$state_machine" \
  --name "manual-$(date -u +%Y%m%dT%H%M%S)" \
  --input "$(jq -cn --arg s "$seasons" '{seasons: $s}')" \
  --query executionArn --output text)"
echo "Started: $execution"
region="$(cut -d: -f4 <<<"$execution")"
echo "Console: https://${region}.console.aws.amazon.com/states/home?region=${region}#/v2/executions/details/$execution"

while true; do
  status="$(aws stepfunctions describe-execution --execution-arn "$execution" --query status --output text)"
  case "$status" in
    RUNNING) printf '.'; sleep 15 ;;
    SUCCEEDED)
      echo; echo "Succeeded."
      aws stepfunctions describe-execution --execution-arn "$execution" --query output --output text \
        | jq '{extract: .extract.summary, warehouse: .load, lake: .athena}'
      exit 0 ;;
    *)
      echo; echo "Finished with status $status." >&2
      aws stepfunctions get-execution-history --execution-arn "$execution" --output json \
        | jq -r '.events[]
            | (.taskFailedEventDetails // .lambdaFunctionFailedEventDetails // .executionFailedEventDetails) as $f
            | select($f) | "\(.type): \($f.error // "") \(($f.cause // "") | .[0:800])"' >&2
      exit 1 ;;
  esac
done
