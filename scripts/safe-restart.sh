#!/bin/bash
# Safe telbot restart — checks for in-flight claude subprocesses first.
# Usage: ./safe-restart.sh [--force]
set -e
FORCE="${1:-}"

# Count active claude subprocesses spawned by telbot (cmdline includes telbot's --add-dir)
ACTIVE=$(ps aux | grep -E "claude --print.*claude-telegram-bridge" | grep -v grep | wc -l | tr -d ' ')

if [ "$ACTIVE" -gt 0 ] && [ "$FORCE" != "--force" ]; then
  echo "⚠️  $ACTIVE in-flight claude session(s) running. Aborting restart."
  echo "   Re-run with --force to restart anyway (will SIGTERM running agents)."
  ps aux | grep -E "claude --print.*claude-telegram-bridge" | grep -v grep | awk '{print "   PID="$2" started="$9}'
  exit 1
fi

if [ "$ACTIVE" -gt 0 ]; then
  echo "⚠️  Force-restarting with $ACTIVE running session(s) — they will be killed."
fi

launchctl stop com.gamma.claude-telegram
launchctl start com.gamma.claude-telegram
sleep 1
NEW=$(ps aux | grep "bridge.py" | grep -v grep | wc -l | tr -d ' ')
echo "✅ Restarted. bridge.py instances: $NEW"
