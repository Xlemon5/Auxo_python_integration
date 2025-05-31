#!/bin/bash

SCRIPT_DIR=$(dirname "$0")
LOG_FILE="$SCRIPT_DIR/PgSQL_connect.log"

{
    echo "=====DOWNLOAD SCRIPT $1 == $(date '+%Y-%m-%d %H:%M:%S') ===="
    python3 "$SCRIPT_DIR/connect_to_PgSQL_DB.py"
    echo "=====FINISH DOWNLOAD $1 == $(date '+%Y-%m-%d %H:%M:%S') ===="
} > "$LOG_FILE" 2>&1