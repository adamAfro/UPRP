#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
nohup python3 wikin.py > wikin.log 2>&1 &
PID=$!
echo "Skrypt wikin.py uruchomiony w tle z PID: $PID"