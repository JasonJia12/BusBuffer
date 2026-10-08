#!/bin/bash
# caffeinate keeps the mac awake
cd "$(dirname "$0")/.."
caffeinate -is python3 -u recorder/record.py 2>&1 | tee -a data/recorder.log
