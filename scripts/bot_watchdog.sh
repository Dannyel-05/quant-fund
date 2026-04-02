#!/bin/bash
# Bot watchdog — restart if not running during market hours (Mon-Fri 06-22 UTC)
HOUR=$(date -u +%H)
DOW=$(date -u +%u)  # 1=Mon 7=Sun
[ "$DOW" -gt 5 ] && exit 0   # skip weekend
[ "$HOUR" -lt 6 ] && exit 0  # skip pre-dawn
[ "$HOUR" -ge 22 ] && exit 0 # skip late night

PID_FILE="/home/dannyelticala/quant-fund/output/bot.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        exit 0  # running fine
    fi
fi
# Not running — start it
cd "/home/dannyelticala/quant-fund"
nohup python3 main.py bot start >> "/home/dannyelticala/quant-fund/logs/bot_watchdog.log" 2>&1 &
echo "$!" > "$PID_FILE"
echo "$(date -u): Bot restarted by watchdog" >> "/home/dannyelticala/quant-fund/logs/bot_watchdog.log"
