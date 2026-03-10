#!/bin/bash
# Run data collection with parallel processing (unbuffered output)

# Use -u flag for unbuffered output (real-time progress display)
python3 -u main_ultrafast.py --stocks config/syariah_stocks.txt --intraday-days 600 --skip-actions --workers 5 "$@"
