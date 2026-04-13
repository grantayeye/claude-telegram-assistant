# Claude Telegram Bridge (Telbot)

Telegram bot that bridges messages to Claude Code CLI with persistent sessions, scheduled jobs, email processing, and semantic memory.

## Architecture

```
Telegram ──→ bridge.py ──→ Claude CLI (--resume) ──→ Telegram
                │
                ├── Job Scheduler (cron, 4 concurrent max)
                ├── Email Webhook (port 8791, MS Graph push)
                ├── Memory (ChromaDB vector store)
                └── Session Manager (per-topic context)
```

Single async Python process. Telegram long-polling, job scheduler, and email webhook all run on one event loop.

## Quick Start

```bash
cp config.example.json config.json  # Edit with your tokens
pip install -r requirements.txt
python3 bridge.py
```

Or use the launchd service (macOS):
```bash
./setup.sh              # Installs plist
./scripts/safe-restart.sh  # Restart without killing in-flight Claude sessions
```

## Configuration

`config.json` fields:

| Field | Description |
|---|---|
| `bot_token` | Telegram Bot API token (or env `CLAUDE_TG_BOT_TOKEN`) |
| `user_id` | Your Telegram user ID (only this user can interact) |
| `user_name` | Name injected into system prompt |
| `user_context` | Additional context about the user |
| `personality` | Behavior instructions for Claude |
| `timezone` | TZ name for cron scheduling (e.g. `America/New_York`) |
| `claude_path` | Path to Claude CLI binary (default: `claude`) |
| `context_dirs` | Paths added via `--add-dir` to every invocation |
| `group_chat_id` | Telegram group ID for job output (null = DM) |
| `email_webhook_port` | HTTP port for MS Graph push notifications (default 8791) |
| `email` | Email config object (method, smtp/graph settings) |

## Message Flow

1. Telegram message arrives, auth checked against `user_id`
2. Intent detection: remember/recall → memory ops; recurring task → job creation
3. If topic is busy, message queues (FIFO, auto-drains when current request completes)
4. Claude CLI spawned with `--resume <session_id>` for context continuity
5. Streaming JSON parsed for text blocks, tool calls, status
6. Response chunked at 4000 chars, sent back to same topic/thread
7. Conversation logged to `history.jsonl` and ChromaDB

## Sessions

Each Telegram topic gets an independent Claude session. Sessions persist across bot restarts via `sessions.json`.

```json
{
  "topic_123": { "id": "session-uuid-here" }
}
```

Commands: `/new` resets session, `/stop` cancels in-flight request.

**Topic Overrides:** Some topics have hard-coded `prompt_prefix` and `cwd` overrides (e.g., ATools GSD topic uses `~/ATools` as working directory).

## Scheduled Jobs

Jobs live in `jobs.json`. The scheduler checks every 30 seconds and fires jobs when their cron schedule matches.

### Job Schema

```json
{
  "name": "daily-digest",
  "schedule": "0 8 * * 1-5",
  "delivery": "telegram",
  "prompt": "Summarize today's calendar and tasks",
  "enabled": true,
  "topic_id": 123,
  "timeout_seconds": 300,
  "last_run": null,
  "consecutive_errors": 0,
  "created": "2026-04-12T00:00:00Z"
}
```

### Delivery Modes

| Mode | Behavior |
|---|---|
| `telegram` | Posts output to `topic_id` or DM |
| `email` | Sends via SMTP or MS Graph |
| `both` | Telegram + email |
| `silent` | Logs only, no delivery |
| `on-failure` | Delivers only if output contains error keywords |

### Concurrency & Health

- Max 4 concurrent jobs (semaphore-controlled)
- Timeout: 60-900s per job (default 300s)
- `consecutive_errors` tracks failures; alert at 3, recovery notification on success
- Run records saved to `runs/YYYY-MM-DD/HHMMSS_slug.json` (30-day retention)

### Commands

| Command | Description |
|---|---|
| `/jobs` | List all jobs with schedule, status, health |
| `/lastrun [name]` | Show most recent run output |
| `/runjob <name>` | Manually trigger a job |
| `/togglejob <name>` | Enable/disable a job |

Jobs can also be created via natural language — the bot parses scheduling requests into cron + prompt.

## Email Processing

MS Graph webhook pushes new emails to `http://localhost:8791/email`.

**Flow:**
1. Instant Telegram notification (priority icon for VIP senders)
2. Full email fetched via `graph_helper.py`
3. Routing rules applied (Bradd → action, employees → reply+CC, external → draft for approval, spam → ignore)
4. Analysis posted to configured Telegram topic
5. Stored in memory for future context

## Memory System

ChromaDB vector store at `memory_db/` with two collections:

- **conversations**: Message pairs indexed for semantic search
- **facts**: Explicit notes (`/remember`) and auto-extracted knowledge from jobs

Operations:
- `/remember <fact>` — save to memory
- `/forget <fact>` — semantic search + delete
- `/recall <query>` — search memory, returns top matches

Top 5 relevant memories are automatically injected into Claude's context for each request.

## File Structure

```
bridge.py              # Main bot (entry point)
memory.py              # ChromaDB memory system
config.json            # Runtime configuration
config.example.json    # Template
jobs.json              # Scheduled jobs
sessions.json          # Active session mappings
history.jsonl          # Conversation log
memory_db/             # ChromaDB SQLite store
runs/                  # Job execution records (YYYY-MM-DD/)
scripts/
  safe-restart.sh      # Restart without killing Claude
  create-topic.py      # Create new Telegram group topic
memory/                # File-based memory (Claude Code native)
```

## Boundary Rules

This bot (Telbot) runs alongside Clawdbot, a separate system also branded as "Grant."

- Telbot's files: `~/clawd/projects/claude-telegram-bridge/`
- Clawdbot's files: `~/.clawdbot/`, `~/clawd/memory/` — read OK, never write
- Shared: `~/.clawdbot/graph-email/graph_helper.py` (email auth, read-only)
- Each system has its own memory, jobs, and sessions
