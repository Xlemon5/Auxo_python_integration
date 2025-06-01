#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
LOG_FILE="$SCRIPT_DIR/logs/API_download.log"

{
    echo "=====DOWNLOAD FILE $1 == $(date '+%Y-%m-%d %H:%M:%S') ===="
    python3 "$SCRIPT_DIR/API_download.py"
    echo "=====FINISH DOWNLOAD $1 == $(date '+%Y-%m-%d %H:%M:%S') ===="
} > "$LOG_FILE" 2>&1