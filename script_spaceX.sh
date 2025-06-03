#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
LOG_FILE="$SCRIPT_DIR/logs/spaceX_data_load.log"

{
    echo "===== INIT DB SCRIPT == $(date '+%Y-%m-%d %H:%M:%S') ===="
    bash "$SCRIPT_DIR/postgres/script_server_publicist.sh"
    echo "===== FINISH INIT DB == $(date '+%Y-%m-%d %H:%M:%S') ===="
    echo ""

    echo "===== DATA LOAD SCRIPT == $(date '+%Y-%m-%d %H:%M:%S') ===="
    python3 "$SCRIPT_DIR/spaceX_API_download.py"
    echo "===== FINISH DATA LOAD == $(date '+%Y-%m-%d %H:%M:%S') ===="
    echo ""
} >> "$LOG_FILE" 2>&1
