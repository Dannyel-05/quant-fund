#!/bin/bash
cd /home/dannyelticala/quant-fund
if [ -f output/bot.pid ]; then
  PID=$(cat output/bot.pid)
  if kill -0 $PID 2>/dev/null; then
    echo "$(date): Bot already running (PID $PID)"
    exit 0
  fi
fi
echo "$(date): Bot not running, starting..."
mkdir -p logs
nohup python3 main.py bot start \
  >> logs/bot_$(date +%Y%m%d).log 2>&1 &
echo $! > output/bot.pid
echo "$(date): Bot started with PID $!"
