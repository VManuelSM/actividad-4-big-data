#!/usr/bin/env bash
set -euo pipefail

SESSION_GAP_MINUTES="${SESSION_GAP_MINUTES:-30}"

mkdir -p output

cat data/clickstream_sample.csv \
  | python3 mapreduce/mapper_clickstream.py \
  | sort -t $'\t' -k1,1 -k2,2n \
  | SESSION_GAP_MINUTES="$SESSION_GAP_MINUTES" python3 mapreduce/reducer_sessionize.py \
  > output/sessions.tsv

cat output/sessions.tsv \
  | python3 mapreduce/mapper_user_agg.py \
  | sort -t $'\t' -k1,1 \
  | python3 mapreduce/reducer_user_agg.py \
  > output/user_metrics.tsv

echo "Sesiones generadas: output/sessions.tsv"
echo "Agregado usuario: output/user_metrics.tsv"
