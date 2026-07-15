#!/bin/bash
# Daily update script for EODHD data collection
# Run this at 20:00 daily for incremental updates

./main_ultrafast.py \
  --stocks config/syariah_stocks.txt \
  --update-mode \
  --max-age 1 \
  --update-window 7 \
  --skip-validation \
  --delay 0.1 \
  "$@"
