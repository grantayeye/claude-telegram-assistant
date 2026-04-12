#!/bin/bash
# Claude Code Telegram Assistant — Setup Script
# Run this on a new machine to get up and running.

set -e

echo "=== Claude Code Telegram Assistant Setup ==="
echo ""

# Check dependencies
echo "Checking dependencies..."

if ! command -v python3 &>/dev/null; then
    echo "❌ python3 not found. Install Python 3.9+ first."
    exit 1
fi

if ! command -v claude &>/dev/null; then
    echo "❌ claude CLI not found. Install Claude Code first:"
    echo "   npm install -g @anthropic-ai/claude-code"
    exit 1
fi

echo "✅ python3 found: $(python3 --version)"
echo "✅ claude found: $(claude --version 2>&1 | head -1)"

# Install Python dependencies
echo ""
echo "Installing Python dependencies..."
pip3 install python-telegram-bot croniter 2>&1 | tail -1

# Config setup
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -f "$SCRIPT_DIR/config.json" ]; then
    echo ""
    echo "=== Configuration ==="
    echo ""

    read -p "Your name: " USER_NAME
    read -p "Brief description (role, company, style preferences): " USER_CONTEXT
    read -p "Timezone (e.g. America/New_York): " TIMEZONE
    TIMEZONE=${TIMEZONE:-America/New_York}

    echo ""
    echo "You need a Telegram bot token from @BotFather."
    echo "Open Telegram, message @BotFather, send /newbot, and follow the prompts."
    echo ""
    read -p "Telegram bot token: " BOT_TOKEN

    echo ""
    echo "To get your Telegram user ID, message @userinfobot on Telegram."
    read -p "Your Telegram user ID: " USER_ID

    echo ""
    read -p "Email delivery? (y/n): " WANT_EMAIL

    CLAUDE_PATH=$(which claude)

    if [ "$WANT_EMAIL" = "y" ] || [ "$WANT_EMAIL" = "Y" ]; then
        read -p "Your email address: " EMAIL_TO
        echo "Email method: 'smtp' (Gmail, etc) or 'graph' (Microsoft Graph API)"
        read -p "Method (smtp/graph): " EMAIL_METHOD

        if [ "$EMAIL_METHOD" = "smtp" ]; then
            read -p "SMTP host (e.g. smtp.gmail.com): " SMTP_HOST
            read -p "SMTP port (default 587): " SMTP_PORT
            SMTP_PORT=${SMTP_PORT:-587}
            read -p "SMTP username: " SMTP_USER
            read -s -p "SMTP password/app password: " SMTP_PASS
            echo ""

            cat > "$SCRIPT_DIR/config.json" << EOFCONFIG
{
  "bot_token": "$BOT_TOKEN",
  "user_id": $USER_ID,
  "user_name": "$USER_NAME",
  "user_context": "$USER_CONTEXT",
  "timezone": "$TIMEZONE",
  "claude_path": "$CLAUDE_PATH",
  "context_dirs": [],
  "email": {
    "enabled": true,
    "to": "$EMAIL_TO",
    "method": "smtp",
    "smtp_host": "$SMTP_HOST",
    "smtp_port": $SMTP_PORT,
    "smtp_user": "$SMTP_USER",
    "smtp_pass": "$SMTP_PASS"
  }
}
EOFCONFIG
        else
            read -p "Path to graph_helper.py: " GRAPH_HELPER
            cat > "$SCRIPT_DIR/config.json" << EOFCONFIG
{
  "bot_token": "$BOT_TOKEN",
  "user_id": $USER_ID,
  "user_name": "$USER_NAME",
  "user_context": "$USER_CONTEXT",
  "timezone": "$TIMEZONE",
  "claude_path": "$CLAUDE_PATH",
  "context_dirs": [],
  "email": {
    "enabled": true,
    "to": "$EMAIL_TO",
    "method": "graph",
    "graph_helper": "$GRAPH_HELPER"
  }
}
EOFCONFIG
        fi
    else
        cat > "$SCRIPT_DIR/config.json" << EOFCONFIG
{
  "bot_token": "$BOT_TOKEN",
  "user_id": $USER_ID,
  "user_name": "$USER_NAME",
  "user_context": "$USER_CONTEXT",
  "timezone": "$TIMEZONE",
  "claude_path": "$CLAUDE_PATH",
  "context_dirs": [],
  "email": {
    "enabled": false
  }
}
EOFCONFIG
    fi

    echo ""
    echo "✅ Config saved to $SCRIPT_DIR/config.json"
else
    echo "✅ config.json already exists"
fi

echo ""
echo "=== Ready! ==="
echo ""
echo "Start the bot:"
echo "  cd $SCRIPT_DIR"
echo "  python3 bridge.py"
echo ""
echo "Or run in background with tmux:"
echo "  tmux new-session -d -s claude-tg 'python3 bridge.py'"
echo ""
echo "To run on startup, add a launchd plist or crontab entry."
