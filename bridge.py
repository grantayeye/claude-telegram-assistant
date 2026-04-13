#!/usr/bin/env python3
"""
Claude Code Telegram Assistant
Config-driven personal assistant via Telegram with scheduled jobs,
dual-channel delivery, cron scheduling, and full local tool access.

All personal config lives in config.json — see config.example.json.
"""

import asyncio
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from croniter import croniter
from memory import Memory
from telegram import Update, Bot
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
from telegram.constants import ParseMode

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ===================== CONFIG =====================

APP_DIR = Path(__file__).parent
CONFIG_FILE = APP_DIR / "config.json"

if not CONFIG_FILE.exists():
    print(f"No config.json found in {APP_DIR}")
    print("Copy config.example.json to config.json and fill it in.")
    sys.exit(1)

CFG = json.loads(CONFIG_FILE.read_text())

BOT_TOKEN = os.environ.get("CLAUDE_TG_BOT_TOKEN") or CFG["bot_token"]
USER_ID = CFG["user_id"]
USER_NAME = CFG.get("user_name", "User")
USER_CONTEXT = CFG.get("user_context", "")
PERSONALITY = CFG.get("personality", "Direct and action-oriented. Skip fluff, get to the point. Do the work first, explain after. Pick sensible defaults instead of asking questions. Be concise. Have opinions.")
TZ = ZoneInfo(CFG.get("timezone", "America/New_York"))
CLAUDE_PATH = CFG.get("claude_path", "claude")
CONTEXT_DIRS = [os.path.expanduser(d) for d in CFG.get("context_dirs", [])]
EMAIL_CFG = CFG.get("email", {})
EMAIL_WATCH = CFG.get("email_watch", {})

SESSION_DIR = Path(tempfile.mkdtemp(prefix="claude-tg-"))
IMAGE_DIR = SESSION_DIR / "images"
IMAGE_DIR.mkdir(exist_ok=True)
JOBS_FILE = APP_DIR / "jobs.json"
HISTORY_FILE = APP_DIR / "history.jsonl"
RUNS_DIR = APP_DIR / "runs"
RUNS_DIR.mkdir(exist_ok=True)
RUNS_RETENTION_DAYS = 30

# Topic-specific overrides (topic_id → config)
TOPIC_OVERRIDES = {
    "226": {
        "cwd": str(Path.home() / "ATools"),
        "prompt_prefix": (
            "You are working inside the ATools project (~/ATools). "
            "Use the GSD framework for this task. Run /gsd-quick for simple requests, "
            "or /gsd-do if a GSD project is already initialized in this repo. "
            "Always work on the dev branch. Follow ATools CLAUDE.md conventions.\n\n"
        ),
    },
}

# Session state — keyed by topic_id (or "dm" for direct messages)
SESSIONS_FILE = APP_DIR / "sessions.json"
sessions = {}  # {topic_key: {"id": None, "busy": False, ...}} — runtime only

def _load_sessions_from_disk():
    """Restore session_id mappings on boot so Claude --resume keeps working across restarts."""
    if not SESSIONS_FILE.exists():
        return
    try:
        data = json.loads(SESSIONS_FILE.read_text())
        for topic_key, sid in data.items():
            sessions[topic_key] = {"id": sid, "busy": False, "pending": [], "current_task": None}
        logger.info(f"Restored {len(sessions)} session(s) from {SESSIONS_FILE}")
    except Exception as e:
        logger.error(f"Failed to load sessions: {e}")

def _save_sessions_to_disk():
    """Persist topic_key → session_id mapping (busy/task/pending are runtime-only)."""
    try:
        snapshot = {k: v["id"] for k, v in sessions.items() if v.get("id")}
        SESSIONS_FILE.write_text(json.dumps(snapshot, indent=2))
    except Exception as e:
        logger.error(f"Failed to save sessions: {e}")
sent_images = set()
GROUP_CHAT_ID = CFG.get("group_chat_id", None)  # Set after adding bot to group

# Vector memory
mem = Memory(str(APP_DIR / "memory_db"))


def is_authorized(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == USER_ID


async def send_to_chat(msg, text, **kwargs):
    """Send a message to the same chat/thread without quoting the original."""
    params = {"chat_id": msg.chat.id, "text": text, **kwargs}
    if msg.message_thread_id:
        params["message_thread_id"] = msg.message_thread_id
    return await msg.get_bot().send_message(**params)


def get_topic_key(msg) -> str:
    """Get a unique key for the conversation scope — topic ID or 'dm'."""
    if msg and msg.message_thread_id:
        return str(msg.message_thread_id)
    if msg and msg.chat.type == "private":
        return "dm"
    return "general"


def get_session(topic_key: str) -> dict:
    """Get or create session state for a topic."""
    if topic_key not in sessions:
        sessions[topic_key] = {"id": None, "busy": False}
    return sessions[topic_key]


def now_tz() -> datetime:
    return datetime.now(TZ)


def log_conversation(role: str, text: str, session_id: str = None, topic_key: str = None):
    """Append a message to the conversation history log."""
    entry = {
        "ts": now_tz().isoformat(),
        "role": role,
        "text": text[:5000],  # cap at 5k chars per entry
    }
    if session_id:
        entry["session"] = session_id
    if topic_key:
        entry["topic"] = topic_key
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def get_topic_history(topic_key: str, limit: int = 10, max_chars: int = 3000) -> str:
    """Get recent conversation history for a specific topic."""
    if not HISTORY_FILE.exists():
        return ""
    entries = []
    for line in HISTORY_FILE.read_text().splitlines()[-200:]:
        try:
            e = json.loads(line)
            if e.get("topic") == topic_key:
                entries.append(e)
        except json.JSONDecodeError:
            continue
    if not entries:
        return ""
    recent = entries[-limit:]
    lines = []
    total = 0
    for e in recent:
        role = e["role"]
        text = e["text"]
        label = {"user": "Bradd", "assistant": "Grant", "job": "Job"}.get(role, role)
        line = f"[{label}]: {text}"
        total += len(line)
        if total > max_chars:
            break
        lines.append(line)
    if not lines:
        return ""
    return "Recent messages in this topic:\n" + "\n".join(lines)


# ===================== JOBS SYSTEM =====================

def load_jobs() -> list:
    if JOBS_FILE.exists():
        return json.loads(JOBS_FILE.read_text())
    return []


def save_jobs(jobs: list):
    JOBS_FILE.write_text(json.dumps(jobs, indent=2))


def next_run_str(job: dict) -> str:
    schedule = job.get("schedule", "")
    if not schedule:
        return "no schedule"
    try:
        cron = croniter(schedule, now_tz())
        nxt = cron.get_next(datetime)
        return nxt.strftime("%a %m/%d %I:%M%p")
    except Exception:
        return "invalid cron"


DELIVERY_ICONS = {
    "telegram": "💬", "email": "📧", "both": "💬📧",
    "silent": "🔇", "on-failure": "🚨",
}


async def jobs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    jobs = load_jobs()
    if not jobs:
        await send_to_chat(update.message,"No jobs. Use /addjob or just describe a recurring task.")
        return
    lines = ["📋 Scheduled Jobs:\n"]
    for i, job in enumerate(jobs):
        enabled = "✅" if job.get("enabled", True) else "⏸️"
        delivery = DELIVERY_ICONS.get(job.get("delivery", "telegram"), "💬")
        last = job.get("last_run", "never")
        if last != "never":
            try:
                last = datetime.fromisoformat(last).strftime("%m/%d %I:%M%p")
            except Exception:
                pass
        nxt = next_run_str(job)
        ls = job.get("last_status", "—")
        ce = int(job.get("consecutive_errors", 0))
        dur = job.get("last_duration_ms")
        dur_s = f"{dur/1000:.1f}s" if isinstance(dur, (int, float)) else "—"
        health = f"{ls} • {dur_s}"
        if ce > 0:
            health += f" • {ce} err{'s' if ce != 1 else ''} in a row"
        lines.append(
            f"{enabled} [{i}] {delivery} {job['name']}\n"
            f"   Schedule: {job.get('schedule', '?')}\n"
            f"   Next: {nxt} | Last: {last} ({health})\n"
            f"   {job['prompt'][:60]}..."
        )
    await send_to_chat(update.message,"\n".join(lines))


async def templates_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await send_to_chat(update.message,
        "📝 Job Templates — copy and edit:\n\n"
        "LinkedIn Post (weekday mornings):\n"
        "/addjob LinkedIn Post | 30 9 * * 1-5 | telegram |\n"
        "Draft a LinkedIn post about [your industry]. Keep under 1300 chars.\n\n"
        "News Digest (daily morning):\n"
        "/addjob Tech News | 0 8 * * * | email |\n"
        "Search for today's top industry news. Summarize 5 stories with links.\n\n"
        "Site Health (every 6h, alert only):\n"
        "/addjob Site Health | 0 */6 * * * | on-failure |\n"
        "Curl [your-site.com]. Report ONLY if down or slow.\n\n"
        "Or just describe what you want in plain language!"
    )


async def addjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/addjob", "", 1).strip()
    if not text:
        await send_to_chat(update.message,
            "Usage: /addjob <name> | <cron> | <delivery> | <prompt>\n\n"
            "Cron: standard (min hour dom mon dow)\n"
            "  0 9 * * 1-5  = 9am weekdays\n"
            "  0 */6 * * *  = every 6 hours\n\n"
            "Delivery: telegram, email, both, silent, on-failure\n\n"
            "Or just describe what you want naturally!"
        )
        return

    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 4:
        await send_to_chat(update.message,"Need 4 parts: name | cron | delivery | prompt\n\nOr just describe it naturally.")
        return

    name = parts[0]
    schedule = parts[1]
    delivery = parts[2].lower()
    prompt = "|".join(parts[3:])

    try:
        croniter(schedule)
    except Exception:
        await send_to_chat(update.message,f"Invalid cron: {schedule}")
        return

    if delivery not in ("telegram", "email", "both", "silent", "on-failure"):
        await send_to_chat(update.message,"Delivery must be: telegram, email, both, silent, or on-failure")
        return

    jobs = load_jobs()
    job = {
        "name": name, "schedule": schedule, "delivery": delivery,
        "prompt": prompt, "enabled": True, "last_run": None,
        "created": now_tz().isoformat(),
    }
    jobs.append(job)
    save_jobs(jobs)

    nxt = next_run_str(job)
    icon = DELIVERY_ICONS.get(delivery, "💬")
    await send_to_chat(update.message,f"✅ Added: {name}\n{icon} Delivery: {delivery}\nNext run: {nxt}")


async def rmjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/rmjob", "", 1).strip()
    jobs = load_jobs()
    try:
        idx = int(text)
        removed = jobs.pop(idx)
        save_jobs(jobs)
        await send_to_chat(update.message,f"Removed: {removed['name']}")
    except (ValueError, IndexError):
        await send_to_chat(update.message,f"Usage: /rmjob <index> (0-{len(jobs)-1})")


async def togglejob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/togglejob", "", 1).strip()
    jobs = load_jobs()
    try:
        idx = int(text)
        jobs[idx]["enabled"] = not jobs[idx].get("enabled", True)
        save_jobs(jobs)
        state = "✅ enabled" if jobs[idx]["enabled"] else "⏸️ paused"
        await send_to_chat(update.message,f"{jobs[idx]['name']}: {state}")
    except (ValueError, IndexError):
        await send_to_chat(update.message,f"Usage: /togglejob <index>")


async def editjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/editjob", "", 1).strip()
    parts = text.split(None, 2)
    if len(parts) < 3:
        await send_to_chat(update.message,"Usage: /editjob <index> <field> <value>\nFields: name, schedule, delivery, prompt")
        return
    jobs = load_jobs()
    try:
        idx = int(parts[0])
        field = parts[1].lower()
        value = parts[2]
        if field not in ("name", "schedule", "delivery", "prompt"):
            await send_to_chat(update.message,"Fields: name, schedule, delivery, prompt")
            return
        if field == "schedule":
            croniter(value)
        if field == "delivery" and value not in ("telegram", "email", "both", "silent", "on-failure"):
            await send_to_chat(update.message,"Delivery: telegram, email, both, silent, on-failure")
            return
        jobs[idx][field] = value
        save_jobs(jobs)
        await send_to_chat(update.message,f"Updated {jobs[idx]['name']}: {field} = {value}")
    except (ValueError, IndexError) as e:
        await send_to_chat(update.message,f"Error: {e}")


async def runjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/runjob", "", 1).strip()
    jobs = load_jobs()
    try:
        idx = int(text)
        job = jobs[idx]
        await send_to_chat(update.message,f"Running: {job['name']}...")
        result = await run_claude_oneshot(job["prompt"])
        await deliver_result(job, result)
        jobs[idx]["last_run"] = now_tz().isoformat()
        save_jobs(jobs)
    except (ValueError, IndexError):
        await send_to_chat(update.message,f"Usage: /runjob <index>")
    except Exception as e:
        await send_to_chat(update.message,f"Job error: {str(e)[:500]}")


# ===================== NATURAL LANGUAGE JOB CREATION =====================

RECURRING_SIGNALS = [
    "every day", "every morning", "every evening", "every week", "every hour",
    "daily", "weekly", "weekday", "weekdays", "hourly", "monthly",
    "every monday", "every tuesday", "every wednesday", "every thursday", "every friday",
    "every saturday", "every sunday",
    "each morning", "each day", "each week",
    "recurring", "schedule", "remind me every", "check every",
    "on a schedule", "at 9am", "at 8am", "at noon",
    "twice a day", "three times", "once a day", "once a week",
]


REMEMBER_SIGNALS = [
    "remember that", "remember this", "don't forget", "keep in mind",
    "note that", "save this", "make a note", "store this",
    "remember:", "fyi ", "for the record",
]

RECALL_SIGNALS = [
    "what did we", "what was that", "do you remember", "when did we",
    "last time we", "previously we", "we talked about", "we discussed",
    "remind me what", "what happened with", "what did i say about",
    "did we ever", "have we", "what do you know about",
]


def looks_like_recurring(text: str) -> bool:
    lower = text.lower()
    if not any(sig in lower for sig in RECURRING_SIGNALS):
        return False
    # Don't treat questions about existing tasks/scheduling as new job requests
    question_markers = ["?", "are there", "is there", "do we have", "can you fix",
                        "what tasks", "which tasks", "how many", "running this way",
                        "powered by", "want all tasks", "any others"]
    if any(q in lower for q in question_markers):
        return False
    return True


def looks_like_remember(text: str) -> bool:
    lower = text.lower()
    return any(sig in lower for sig in REMEMBER_SIGNALS)


def looks_like_recall(text: str) -> bool:
    lower = text.lower()
    return any(sig in lower for sig in RECALL_SIGNALS)


def get_job_parse_prompt() -> str:
    return f"""You are a job scheduler assistant. The user wants to create a recurring task.
Parse their natural language request into a structured job definition.

You MUST respond with ONLY a JSON object (no markdown, no explanation, no backticks):
{{
  "name": "Short name for the job (3-5 words)",
  "schedule": "cron expression (min hour dom mon dow) in {CFG.get('timezone', 'America/New_York')} timezone",
  "delivery": "telegram or email or both or on-failure",
  "prompt": "The detailed prompt that Claude should execute each time this job runs. Include all necessary context and instructions. Be specific about what to do and how to format the output."
}}

Cron reference:
- 0 8 * * 1-5 = 8am weekdays
- 0 9 * * * = 9am daily
- 0 */6 * * * = every 6 hours
- 0 8 * * 1 = 8am Mondays

Delivery guide:
- "telegram" for quick updates, alerts, drafts to review
- "email" for longer reports, digests, summaries
- "both" for important items
- "on-failure" for health checks (only notify if something's wrong)

The user is {USER_NAME}. {USER_CONTEXT}

User request: """


async def parse_and_create_job(text: str, update: Update):
    msg = update.message
    status = await send_to_chat(msg,"🔧 Setting up recurring task...")

    try:
        result = await run_claude_oneshot(get_job_parse_prompt() + text)
        result = result.strip()
        if result.startswith("```"):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)

        job_def = json.loads(result)

        name = job_def.get("name", "Unnamed Job")
        schedule = job_def.get("schedule", "")
        delivery = job_def.get("delivery", "telegram")
        prompt = job_def.get("prompt", "")

        if not schedule or not prompt:
            await safe_edit(status, "Couldn't parse that into a job. Try /addjob for manual creation.")
            return

        croniter(schedule)

        if delivery not in ("telegram", "email", "both", "silent", "on-failure"):
            delivery = "telegram"

        # Capture topic so job results post to the right thread
        topic_id = update.message.message_thread_id if update.message else None

        jobs = load_jobs()
        job = {
            "name": name, "schedule": schedule, "delivery": delivery,
            "prompt": prompt, "enabled": True, "last_run": None,
            "created": now_tz().isoformat(),
            "topic_id": topic_id,
        }
        jobs.append(job)
        save_jobs(jobs)

        nxt = next_run_str(job)
        icon = DELIVERY_ICONS.get(delivery, "💬")
        await safe_edit(status,
            f"✅ Job created!\n\n"
            f"Name: {name}\n"
            f"Schedule: {schedule}\n"
            f"{icon} Delivery: {delivery}\n"
            f"Next run: {nxt}\n\n"
            f"Prompt: {prompt[:200]}{'...' if len(prompt) > 200 else ''}\n\n"
            f"Use /jobs to see all, /runjob {len(jobs)-1} to test now."
        )

    except json.JSONDecodeError:
        await safe_edit(status, "Couldn't parse job definition. Try /addjob for manual creation.")
    except Exception as e:
        logger.error(f"Job parse error: {e}", exc_info=True)
        await safe_edit(status, f"Error creating job: {str(e)[:300]}")


# ===================== DELIVERY SYSTEM =====================

async def deliver_result(job: dict, result: str):
    delivery = job.get("delivery", "telegram")
    name = job["name"]
    job_topic = str(job["topic_id"]) if job.get("topic_id") else "dm"
    log_conversation("job", f"[{name}] {result}", topic_key=job_topic)
    mem.add_job_result(name, result)

    if delivery == "on-failure":
        lower = result.lower()
        failure_signals = ["error", "fail", "down", "timeout", "unreachable", "issue", "problem", "alert", "critical"]
        if not any(sig in lower for sig in failure_signals):
            logger.info(f"Job '{name}' passed — suppressing (on-failure mode)")
            return
        delivery = "telegram"

    if delivery == "silent":
        logger.info(f"Job '{name}' completed silently ({len(result)} chars)")
        return

    if delivery in ("telegram", "both"):
        topic_id = job.get("topic_id")
        await send_telegram(f"📋 {name}:\n\n{result}", topic_id=topic_id)

    if delivery in ("email", "both"):
        await send_email(subject=f"[Claude] {name}", body=result)


async def send_telegram(text: str, topic_id: int = None):
    """Send to DM or a specific group topic."""
    bot = Bot(BOT_TOKEN)
    chat_id = GROUP_CHAT_ID if (GROUP_CHAT_ID and topic_id) else USER_ID
    chunks = split_message(text, 4000)
    for chunk in chunks:
        kwargs = {"chat_id": chat_id, "text": chunk}
        if topic_id and GROUP_CHAT_ID:
            kwargs["message_thread_id"] = topic_id
        try:
            await bot.send_message(**kwargs, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            try:
                await bot.send_message(**kwargs)
            except Exception as e:
                logger.error(f"Telegram send failed: {e}")


async def send_email(subject: str, body: str):
    if not EMAIL_CFG.get("enabled"):
        logger.warning(f"Email not configured — would have sent: {subject}")
        return

    html_body = body.replace("\n", "<br>")
    html_body = f"<div style='font-family: sans-serif; font-size: 14px;'>{html_body}</div>"
    to_addr = EMAIL_CFG.get("to", "")

    method = EMAIL_CFG.get("method", "graph")

    if method == "graph":
        helper = os.path.expanduser(EMAIL_CFG.get("graph_helper", ""))
        if not helper or not Path(helper).exists():
            logger.error("Graph helper not found")
            return
        cmd = ["python3", helper, "send", "--to", to_addr, "--subject", subject, "--body", html_body]
    elif method == "smtp":
        # Fallback: use python's smtplib via a small inline script
        smtp_host = EMAIL_CFG.get("smtp_host", "")
        smtp_port = EMAIL_CFG.get("smtp_port", 587)
        smtp_user = EMAIL_CFG.get("smtp_user", "")
        smtp_pass = EMAIL_CFG.get("smtp_pass", "")
        cmd = [
            "python3", "-c",
            f"""
import smtplib
from email.mime.text import MIMEText
msg = MIMEText({repr(html_body)}, 'html')
msg['Subject'] = {repr(subject)}
msg['From'] = {repr(smtp_user)}
msg['To'] = {repr(to_addr)}
with smtplib.SMTP({repr(smtp_host)}, {smtp_port}) as s:
    s.starttls()
    s.login({repr(smtp_user)}, {repr(smtp_pass)})
    s.send_message(msg)
"""
        ]
    else:
        logger.error(f"Unknown email method: {method}")
        return

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        if proc.returncode == 0:
            logger.info(f"Email sent: {subject}")
        else:
            logger.error(f"Email failed: {stderr.decode()[:200]}")
    except Exception as e:
        logger.error(f"Email error: {e}")


# ===================== SCHEDULER =====================

ERROR_ALERT_THRESHOLD = 3  # alert after this many consecutive failures
MAX_CONCURRENT_JOBS = 4
DEFAULT_JOB_TIMEOUT = 300
MAX_JOB_TIMEOUT = 900
_job_semaphore = None  # initialized in job_scheduler
_jobs_lock = asyncio.Lock()  # protects jobs.json read/write
_running_jobs = set()  # job names currently executing


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:40]


def write_run_record(job_name: str, started_at, duration_ms: int, status: str,
                     output: str = "", error: str = "", delivery_status: str = ""):
    """Persist a run record to ~/clawd/projects/claude-telegram-bridge/runs/YYYY-MM-DD/HHMMSS_<slug>.json"""
    try:
        day_dir = RUNS_DIR / started_at.strftime("%Y-%m-%d")
        day_dir.mkdir(exist_ok=True)
        fname = f"{started_at.strftime('%H%M%S')}_{_slug(job_name)}.json"
        record = {
            "job": job_name,
            "started_at": started_at.isoformat(),
            "duration_ms": duration_ms,
            "status": status,
            "output": (output or "")[:20000],
            "error": (error or "")[:2000],
            "delivery_status": delivery_status,
        }
        (day_dir / fname).write_text(json.dumps(record, indent=2))
    except Exception as e:
        logger.error(f"Failed to write run record for {job_name}: {e}")


def prune_old_runs():
    """Delete run dirs older than RUNS_RETENTION_DAYS. Called opportunistically."""
    try:
        cutoff = (now_tz() - timedelta(days=RUNS_RETENTION_DAYS)).date()
        for child in RUNS_DIR.iterdir():
            if not child.is_dir():
                continue
            try:
                day = datetime.strptime(child.name, "%Y-%m-%d").date()
                if day < cutoff:
                    for f in child.iterdir():
                        f.unlink(missing_ok=True)
                    child.rmdir()
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Run prune failed: {e}")

async def _execute_job(job_name: str, job_prompt: str, timeout_seconds: int):
    """Run a single job. Returns dict with execution outcome (no jobs.json mutation here)."""
    start_ts = now_tz()
    run_output = ""
    run_error = ""
    run_status = "ok"
    delivery_status = ""
    failed = False
    try:
        result = await run_claude_oneshot(job_prompt, timeout_seconds=timeout_seconds)
        run_output = result or ""
    except asyncio.TimeoutError:
        failed = True
        run_status = "timeout"
        run_error = f"{timeout_seconds}s timeout"
        logger.error(f"Job '{job_name}' timed out ({timeout_seconds}s)")
    except Exception as e:
        failed = True
        run_status = "error"
        run_error = str(e)
        logger.error(f"Job '{job_name}' failed: {e}", exc_info=True)

    duration_ms = int((now_tz() - start_ts).total_seconds() * 1000)
    return {
        "start_ts": start_ts,
        "duration_ms": duration_ms,
        "status": run_status,
        "output": run_output,
        "error": run_error,
        "delivery_status": delivery_status,
        "failed": failed,
    }


async def _run_job_with_concurrency(job: dict, current_iso: str):
    """Acquire semaphore, run job, deliver, persist outcome to jobs.json under lock."""
    job_name = job["name"]
    if job_name in _running_jobs:
        logger.info(f"Job '{job_name}' already running — skipping this tick")
        return
    _running_jobs.add(job_name)
    try:
        async with _job_semaphore:
            timeout_seconds = max(60, min(MAX_JOB_TIMEOUT, int(job.get("timeout_seconds", DEFAULT_JOB_TIMEOUT))))
            logger.info(f"Running scheduled job: {job_name} (timeout={timeout_seconds}s)")
            outcome = await _execute_job(job_name, job["prompt"], timeout_seconds)

            # Deliver only if execution succeeded
            if not outcome["failed"]:
                try:
                    await deliver_result(job, outcome["output"])
                    outcome["delivery_status"] = "delivered"
                except Exception as deliv_err:
                    outcome["delivery_status"] = f"delivery_failed: {str(deliv_err)[:200]}"
                    logger.error(f"Delivery failed for '{job_name}': {deliv_err}")

            # Persist run record + update jobs.json
            write_run_record(
                job_name=job_name,
                started_at=outcome["start_ts"],
                duration_ms=outcome["duration_ms"],
                status=outcome["status"],
                output=outcome["output"],
                error=outcome["error"],
                delivery_status=outcome["delivery_status"],
            )

            async with _jobs_lock:
                jobs = load_jobs()
                for i, j in enumerate(jobs):
                    if j["name"] != job_name:
                        continue
                    prev_errors = int(j.get("consecutive_errors", 0))
                    j["last_run"] = current_iso
                    j["last_duration_ms"] = outcome["duration_ms"]

                    if outcome["failed"]:
                        new_errors = prev_errors + 1
                        j["consecutive_errors"] = new_errors
                        j["last_status"] = outcome["status"]
                        if outcome["error"]:
                            j["last_error"] = outcome["error"][:500]
                        if new_errors == ERROR_ALERT_THRESHOLD:
                            asyncio.create_task(send_telegram(
                                f"⚠️ Job '{job_name}' has failed {new_errors} times in a row. "
                                f"Last status: {outcome['status']}. Check bridge.log or /lastrun {i}."
                            ))
                    else:
                        j["consecutive_errors"] = 0
                        j["last_status"] = "ok"
                        # Recovery: if was failing, notify it's healthy again
                        if prev_errors >= ERROR_ALERT_THRESHOLD:
                            asyncio.create_task(send_telegram(
                                f"✅ Job '{job_name}' recovered after {prev_errors} consecutive failures."
                            ))
                    break
                save_jobs(jobs)
    finally:
        _running_jobs.discard(job_name)


async def job_scheduler():
    global _job_semaphore
    _job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    logger.info(f"Job scheduler started (max_concurrent={MAX_CONCURRENT_JOBS})")
    while True:
        await asyncio.sleep(30)
        try:
            current = now_tz()
            current_iso = current.isoformat()
            due_jobs = []

            async with _jobs_lock:
                jobs = load_jobs()
                save_needed = False
                for i, job in enumerate(jobs):
                    if not job.get("enabled", True):
                        continue
                    schedule = job.get("schedule", "")
                    if not schedule:
                        continue

                    last_run = job.get("last_run")
                    if last_run is None:
                        # First seen — anchor to now, wait for next scheduled time
                        jobs[i]["last_run"] = current_iso
                        save_needed = True
                        continue

                    try:
                        last_dt = datetime.fromisoformat(last_run)
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=TZ)
                        cron = croniter(schedule, last_dt)
                        if current >= cron.get_next(datetime):
                            due_jobs.append(dict(job))  # snapshot
                    except Exception as parse_err:
                        logger.error(f"Job '{job.get('name','?')}' schedule parse error: {parse_err}")
                        continue
                if save_needed:
                    save_jobs(jobs)

            # Launch all due jobs concurrently (semaphore caps in-flight)
            for job in due_jobs:
                asyncio.create_task(_run_job_with_concurrency(job, current_iso))

            # Housekeeping
            prune_old_runs()
        except Exception as e:
            logger.error(f"Scheduler error: {e}", exc_info=True)


# ===================== EMAIL WATCHER =====================

EMAIL_WEBHOOK_PORT = CFG.get("email_webhook_port", 8791)

async def email_webhook_server():
    """HTTP server that receives email push notifications from the Graph webhook server."""
    if not EMAIL_WATCH.get("enabled"):
        logger.info("Email webhook disabled")
        return

    ignore_from = EMAIL_WATCH.get("ignore_from", [])
    ignore_subjects = EMAIL_WATCH.get("ignore_subjects", [])
    priority_from = EMAIL_WATCH.get("priority_from", [])
    topic_id = EMAIL_WATCH.get("topic_id")

    from aiohttp import web

    async def handle_email(request):
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad json")

        messages = data.get("messages", [])
        for msg in messages:
            sender = msg.get("from", "").lower()
            subject = msg.get("subject", "")
            email_id = msg.get("id", "")

            # Filter noise
            if any(ig in sender for ig in ignore_from):
                logger.info(f"Email ignored (filtered): {sender} — {subject}")
                continue
            if any(ig in subject.lower() for ig in ignore_subjects):
                logger.info(f"Email ignored (subject): {subject}")
                continue

            is_priority = any(pf in sender for pf in priority_from)
            icon = "🔴" if is_priority else "📩"

            logger.info(f"Email push: {sender} — {subject}")

            # Instant notification
            await send_telegram(f"{icon} Email from {msg.get('from', '?')}\nSubject: {subject}", topic_id=topic_id)

            # Process in background
            asyncio.create_task(_process_email_bg(email_id, sender, subject, topic_id))

            # Store in memory
            mem.add_fact(f"Email received from {msg.get('from', '?')}: {subject}", source="email_push")

        return web.Response(status=202, text="ok")

    async def handle_health(request):
        return web.Response(text='{"status":"ok","service":"claude-telegram-email"}', content_type="application/json")

    app = web.Application()
    app.router.add_post("/email", handle_email)
    app.router.add_get("/", handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", EMAIL_WEBHOOK_PORT)
    await site.start()
    logger.info(f"Email webhook listening on http://127.0.0.1:{EMAIL_WEBHOOK_PORT}/email")


async def _process_email_bg(email_id: str, sender: str, subject: str, topic_id: int):
    """Background task: fetch full email, run Claude analysis, take action."""
    try:
        full_email = await fetch_email(email_id)
        if full_email:
            result = await process_email(full_email, sender, subject)
            await send_telegram(f"📋 Analysis:\n{result}", topic_id=topic_id)
        else:
            logger.warning(f"Could not fetch email {email_id}")
    except Exception as e:
        logger.error(f"Background email processing failed: {e}")
        await send_telegram(f"⚠️ Couldn't process email from {sender}: {str(e)[:200]}", topic_id=topic_id)


async def fetch_email(email_id: str) -> dict:
    """Fetch full email content via Graph API."""
    graph_helper = os.path.expanduser(EMAIL_CFG.get("graph_helper", ""))
    if not graph_helper:
        return None

    cmd = [
        "python3", graph_helper, "get",
        f"/me/messages/{email_id}?$select=subject,from,toRecipients,ccRecipients,body,bodyPreview,receivedDateTime,importance,hasAttachments"
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0:
        return None
    try:
        return json.loads(stdout.decode())
    except json.JSONDecodeError:
        return None


EMAIL_PROCESS_PROMPT = """You are Grant, an AI assistant for Bradd Konert, owner of Gamma Tech Services (MSP). An email just arrived at grant@gamma.tech. Analyze it and take appropriate action.

You have full tool access. You CAN send emails, read files, and run commands.

═══ ROUTING RULES (follow strictly) ═══

Rule 0 — Self-Sent: FROM grant@gamma.tech or grantayeye@gmail.com → IGNORE. Say "Self-sent, ignored."

Rule 1 — From Bradd (bradd@gamma.tech / bradd.konert@gmail.com):
- Direct request/action item → Do it, reply to confirm
- CC/FYI only → Read, retain context, stay quiet
- Task assignment to someone else (grant is CC'd) → Track the task in ~/clawd/projects/claude-telegram-bridge/tasks/active.json
- DO NOT auto-acknowledge or reply just to say "got it"

Rule 2 — From @gamma.tech Employees:
- Reply if you can help. ALWAYS CC bradd@gamma.tech.
- If you need something from Bradd, send him a SEPARATE email (not CC on the thread).

Rule 3 — External Contacts:
- DO NOT reply directly to them.
- Email bradd@gamma.tech with subject "Draft Reply: [original subject]"
- Include: who it's from, what they want, your proposed response.
- Wait for Bradd's approval before sending.

Rule 4 — Spam/Automated/Newsletters/GitHub/Marketing/noreply:
- Ignore completely. Say "Spam/automated, no action."

═══ CC PROTOCOL ═══
- CC'd WITHOUT "Grant" mentioned in body → Monitor only, track if task. Stay quiet.
- CC'd WITH "Grant" or "Grant Ayeye" mentioned → Take action as directed.

═══ OUTBOUND RULES (when sending any email) ═══
- Send via: python3 ~/.clawdbot/graph-email/graph_helper.py send --to "addr" --subject "subj" --body "<html>"
- ALWAYS CC bradd@gamma.tech on emails to anyone other than Bradd: add --cc "bradd@gamma.tech"
- When emailing Bradd directly → do NOT CC yourself
- Signature is auto-appended by the send script — do NOT add it manually (causes double-signature)
- Do NOT add a closing like "Best," or "Thanks," — the signature handles it
- No leading whitespace before first <p> tag
- All times in EST (America/New_York)

═══ TASK TRACKING ═══
On every email, classify: Is this a task assignment? Action for me? FYI? Task completion?
If task assignment, create/update entry in ~/clawd/projects/claude-telegram-bridge/tasks/active.json:
{
  "id": "task-XXX",
  "assigned_to": "email@gamma.tech",
  "task": "description",
  "source": "Email from X, date",
  "created": "YYYY-MM-DD",
  "deadline": null,
  "follow_up_after": "YYYY-MM-DD",
  "status": "active",
  "nudge_count": 0,
  "notes": []
}

═══ CRM UPDATE ═══
Check sender against ~/clawd/crm/contacts.json. If new contact, add entry. If existing, update last_heard_from. Log interaction in ~/clawd/crm/interactions.json.

═══ NEVER DO ═══
- Send email as Bradd
- Share pricing, quotes, or financial info
- Make commitments (scheduling, deadlines, scope) to non-Gamma people
- Share Bradd's personal info (cell, personal email, home address)
- Reply to legal, contract, or liability emails
- Reply to vendor/sales pitches
- Forward threads outside original recipients
- Apologize on behalf of the company for service issues
- Discuss employee matters (HR, pay, performance)
- Reply to government/regulatory bodies

═══ RESPONSE FORMAT (for Telegram notification) ═══
1. **Summary** — 1-2 sentences
2. **Rule** — Which rule (0-4) applied
3. **Action taken** — What you did (sent reply, tracked task, drafted for Bradd, etc.)
4. **Priority** — Low / Medium / High

Keep it concise. Bradd reads on mobile.
"""


async def process_email(email_data: dict, sender: str, subject: str) -> str:
    """Have Claude analyze an email and suggest action."""
    from_addr = email_data.get("from", {}).get("emailAddress", {})
    from_str = f"{from_addr.get('name', '')} <{from_addr.get('address', '')}>"

    to_list = [r["emailAddress"]["address"] for r in email_data.get("toRecipients", [])]
    cc_list = [r["emailAddress"]["address"] for r in email_data.get("ccRecipients", [])]

    body = email_data.get("bodyPreview", "") or ""
    full_body = email_data.get("body", {}).get("content", "")
    # Use preview for prompt (shorter), but include some of full body for context
    if full_body and len(body) < 200:
        # Strip HTML tags roughly
        import re as _re
        clean = _re.sub(r'<[^>]+>', ' ', full_body)
        clean = _re.sub(r'\s+', ' ', clean).strip()
        body = clean[:3000]

    prompt = EMAIL_PROCESS_PROMPT + f"""

Email details:
From: {from_str}
To: {', '.join(to_list)}
CC: {', '.join(cc_list)}
Subject: {subject}
Date: {email_data.get('receivedDateTime', '')}
Importance: {email_data.get('importance', 'normal')}
Has Attachments: {email_data.get('hasAttachments', False)}

Body:
{body}
"""

    return await run_claude_oneshot(prompt)


# ===================== CLAUDE EXECUTION =====================

def build_claude_cmd(output_format="json", streaming=False):
    cmd = [CLAUDE_PATH, "--print", "--dangerously-skip-permissions"]
    if streaming:
        cmd.extend(["--verbose", "--output-format", "stream-json", "--include-partial-messages"])
    else:
        cmd.extend(["--output-format", output_format])
    for d in CONTEXT_DIRS:
        cmd.extend(["--add-dir", d])
    # Inject personality, user context, and boundary rules
    boundary_rules = (
        "\n\nIDENTITY:\n"
        "- PUBLIC persona: 'Grant' (email grant@gamma.tech, how Bradd refers to you publicly). Speak/sign as Grant externally.\n"
        "- INTERNAL infrastructure name: 'Telbot' — the Telegram bridge at ~/clawd/projects/claude-telegram-bridge/.\n"
        "- 'Clawdbot' / 'OpenClaw' is a SEPARATE backend system also branded publicly as Grant. Both run in parallel.\n"
        "  When Bradd says 'Grant' he usually means the public persona. Use Telbot vs Clawdbot for infra clarity.\n"
        "\nBOUNDARY RULES (NEVER VIOLATE):\n"
        "- NEVER modify files in: ~/.clawdbot/, ~/.openclaw/, ~/clawd/MEMORY.md, ~/clawd/USER.md,\n"
        "  ~/clawd/AGENTS.md, ~/clawd/TOOLS.md, ~/clawd/IDENTITY.md, ~/clawd/SOUL.md, ~/clawd/memory/.\n"
        "  Those belong to Clawdbot. Reading them is OK; never write.\n"
        "- You MAY invoke ~/.clawdbot/graph-email/graph_helper.py as a tool for email send/read (shared auth\n"
        "  during parallel run). You may NOT edit it or anything else in ~/.clawdbot/.\n"
        "- Your memory lives ONLY in ~/clawd/projects/claude-telegram-bridge/memory/. Write there.\n"
        "- Your scheduled jobs live ONLY in ~/clawd/projects/claude-telegram-bridge/jobs.json.\n"
        "- If asked about recurring tasks, only discuss YOUR jobs.json — not Clawdbot cron jobs.\n"
        "- Your memory was cloned from Clawdbot on 2026-04-12. Some entries reference wrong models\n"
        "  (MiniMax — you run Claude Code exclusively) or Clawdbot paths. See the warning at top of MEMORY.md.\n"
        "  When in doubt, ignore the entry and ask Bradd.\n"
        "\nMANAGING SCHEDULED JOBS (natural language — no special syntax needed):\n"
        "When Bradd asks you to create, edit, disable, enable, or remove a recurring task, just do it directly\n"
        "by reading and editing ~/clawd/projects/claude-telegram-bridge/jobs.json. The scheduler reloads every\n"
        "30s, so no restart needed.\n"
        "\nJob schema (one object per job in the array):\n"
        "  name: short label\n"
        "  schedule: cron expression in America/New_York TZ (e.g. '0 8 * * 1-5' = 8am weekdays)\n"
        "  delivery: 'telegram' | 'email' | 'both' | 'silent' | 'on-failure'\n"
        "  prompt: full instruction Claude will execute when the job fires\n"
        "  enabled: true | false\n"
        "  topic_id: integer (Telegram topic id) or null for DM\n"
        "  timeout_seconds: optional, 60-900, default 300 (use 600 for heavy jobs)\n"
        "  last_run: null on creation (scheduler will set it)\n"
        "  consecutive_errors: 0 on creation\n"
        "  created: ISO timestamp\n"
        "\nTo create a new Telegram TOPIC for a job, run:\n"
        "  python3 ~/clawd/projects/claude-telegram-bridge/scripts/create-topic.py 'Topic Name'\n"
        "  → prints the new topic_id on the last line. Use that in the job's topic_id field.\n"
        "\nWorkflow when Bradd describes a recurring task:\n"
        "1. Confirm understanding in one short sentence (don't ask permission for obvious defaults).\n"
        "2. If it deserves its own topic, create one with the script. Otherwise reuse an existing topic_id\n"
        "   (check current jobs.json to see what exists).\n"
        "3. Edit jobs.json to append the new job. Set last_run=null, consecutive_errors=0.\n"
        "4. Confirm what you set up: schedule in plain English, topic name, when it'll first fire.\n"
        "Use /jobs, /lastrun, /runjob, /togglejob commands as escape hatches but you don't need them — direct file edits work."
    )
    system_extra = f"Personality: {PERSONALITY}\n\nUser: {USER_NAME}. {USER_CONTEXT}{boundary_rules}"
    cmd.extend(["--append-system-prompt", system_extra])
    return cmd


async def run_claude_oneshot(prompt: str, timeout_seconds: int = 300) -> str:
    cmd = build_claude_cmd(output_format="json")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(Path.home())
    )
    proc.stdin.write(prompt.encode("utf-8"))
    proc.stdin.close()

    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        # Kill the subprocess so it doesn't leak
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        raise
    raw = stdout.decode("utf-8", errors="replace")
    try:
        return json.loads(raw).get("result", raw)
    except json.JSONDecodeError:
        return raw


async def run_claude_streaming(prompt: str, update: Update, context: ContextTypes.DEFAULT_TYPE, status_msg, topic_key: str = "dm", cwd: str = None):
    sess = get_session(topic_key)
    cmd = build_claude_cmd(streaming=True)
    if sess.get("id"):
        cmd.extend(["--resume", sess["id"]])

    work_dir = cwd or str(Path.home())
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        limit=1024 * 1024,  # 1MB line buffer for large JSON events
    )
    proc.stdin.write(prompt.encode("utf-8"))
    proc.stdin.close()

    full_text = ""
    streaming_text = ""
    last_update_len = 0
    last_edit_ts = 0.0
    last_event_ts = asyncio.get_event_loop().time()
    tool_history = []  # last 3 tool calls (status indicator only — not persisted)
    current_tool = ""
    current_block_type = None  # 'text' or 'tool_use'
    current_text_buffer = ""
    sent_text_blocks = 0

    bot = Bot(BOT_TOKEN)
    chat_id = status_msg.chat_id
    thread_id = status_msg.message_thread_id

    def short_tool_summary(name: str, inp: dict) -> str:
        if not isinstance(inp, dict):
            return name
        if name in ("Read", "Edit", "Write", "NotebookEdit"):
            return f"{name} {inp.get('file_path','?')}"
        if name == "Bash":
            cmd = (inp.get('command') or '').strip().split('\n')[0]
            return f"Bash {cmd[:60]}"
        if name == "Grep":
            return f"Grep '{(inp.get('pattern') or '')[:40]}'"
        if name == "Glob":
            return f"Glob {inp.get('pattern','?')}"
        if name == "WebFetch":
            return f"WebFetch {inp.get('url','?')[:60]}"
        if name == "WebSearch":
            return f"WebSearch '{(inp.get('query') or '')[:40]}'"
        return name

    def render_status() -> str:
        lines = []
        if tool_history:
            lines.append("🔧 " + " → ".join(tool_history[-3:]))
        if current_tool:
            lines.append(f"⏳ Currently: {current_tool}")
        elif streaming_text:
            tail = streaming_text[-300:].replace("`", "")
            lines.append(f"⏳ {tail}")
        else:
            lines.append("⏳ Working…")
        return "\n".join(lines)[:4096]

    async def maybe_edit(force: bool = False):
        nonlocal last_edit_ts
        loop_now = asyncio.get_event_loop().time()
        if not force and (loop_now - last_edit_ts) < 1.5:
            return
        last_edit_ts = loop_now
        await safe_edit(status_msg, render_status())

    async def typing_pulse():
        """Fire 'typing' chat action every 4s + send 'still working' nudge if stalled >60s."""
        nonlocal last_event_ts
        last_nudge = asyncio.get_event_loop().time()
        try:
            while True:
                try:
                    kwargs = {"chat_id": chat_id, "action": "typing"}
                    if thread_id:
                        kwargs["message_thread_id"] = thread_id
                    await bot.send_chat_action(**kwargs)
                except Exception:
                    pass
                # Stall detection: no stream event in 60s
                loop_now = asyncio.get_event_loop().time()
                if loop_now - last_event_ts > 60 and loop_now - last_nudge > 60:
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=f"⌛ Still working — {int(loop_now - last_event_ts)}s since last update. (Will time out at 5min total.)",
                            message_thread_id=thread_id,
                        )
                    except Exception:
                        pass
                    last_nudge = loop_now
                await asyncio.sleep(4)
        except asyncio.CancelledError:
            pass

    pulse_task = asyncio.create_task(typing_pulse())

    try:
        async for line in proc.stdout:
            last_event_ts = asyncio.get_event_loop().time()
            line = line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = event.get("type", "")
            sid = event.get("session_id")
            if sid and sess.get("id") != sid:
                sess["id"] = sid
                _save_sessions_to_disk()

            if etype == "stream_event":
                inner = event.get("event", {})
                inner_type = inner.get("type", "")
                if inner_type == "content_block_start":
                    block = inner.get("content_block", {})
                    bt = block.get("type")
                    current_block_type = bt
                    if bt == "tool_use":
                        tool_name = block.get("name", "tool")
                        tool_input = block.get("input", {}) or {}
                        current_tool = short_tool_summary(tool_name, tool_input)
                        await maybe_edit(force=True)
                    elif bt == "text":
                        current_text_buffer = ""
                        streaming_text = ""
                        last_update_len = 0
                elif inner_type == "content_block_delta":
                    delta = inner.get("delta", {})
                    if delta.get("type") == "text_delta":
                        chunk_text = delta.get("text", "")
                        current_text_buffer += chunk_text
                        streaming_text = current_text_buffer
                        if len(streaming_text) - last_update_len > 200:
                            last_update_len = len(streaming_text)
                            await maybe_edit()
                elif inner_type == "content_block_stop":
                    if current_block_type == "text" and current_text_buffer.strip():
                        # Persist this narration block in chat
                        try:
                            for chunk in split_message(current_text_buffer.strip(), 4000):
                                kw = {"chat_id": chat_id, "text": chunk}
                                if thread_id:
                                    kw["message_thread_id"] = thread_id
                                await bot.send_message(**kw)
                            sent_text_blocks += 1
                        except Exception as send_err:
                            logger.error(f"Failed to send narration block: {send_err}")
                        current_text_buffer = ""
                        streaming_text = ""
                        last_update_len = 0
                    elif current_block_type == "tool_use":
                        if current_tool:
                            tool_history.append(current_tool)
                            current_tool = ""
                            await maybe_edit(force=True)
                    current_block_type = None

            elif etype == "result":
                full_text = event.get("result", "")
                logger.info(f"Result: {len(full_text)} chars")

    except asyncio.CancelledError:
        proc.terminate()
        pulse_task.cancel()
        raise
    finally:
        pulse_task.cancel()

    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()

    # If we already streamed narration as persistent messages, we're done.
    try:
        await status_msg.delete()
    except Exception:
        pass

    if sent_text_blocks > 0:
        return

    # Fallback: agent finished without any text blocks (rare). Send the result event's text.
    if not full_text.strip() and streaming_text.strip():
        full_text = streaming_text
    if not full_text.strip():
        try:
            kw = {"chat_id": chat_id, "text": "✅ Done (no text response)."}
            if thread_id:
                kw["message_thread_id"] = thread_id
            await bot.send_message(**kw)
        except Exception:
            pass
        return

    await send_response(update, context, full_text, topic_key)


# ===================== MESSAGE HANDLING =====================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if msg:
        chat = msg.chat
        logger.info(f"Incoming: chat_id={chat.id} type={chat.type} title={chat.title} thread={msg.message_thread_id} from={msg.from_user.id if msg.from_user else '?'} text={msg.text[:50] if msg.text else ''}")

    if not is_authorized(update):
        return

    if not msg:
        return

    prompt = msg.text or msg.caption or ""

    # Include quoted/replied-to message for context
    if msg.reply_to_message:
        reply = msg.reply_to_message
        quoted_text = reply.text or reply.caption or ""
        if quoted_text:
            prompt = f"[Replying to: {quoted_text}]\n\n{prompt}"

    image_paths = []
    if msg.photo:
        photo = msg.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        img_path = IMAGE_DIR / f"{photo.file_id}.jpg"
        await file.download_to_drive(str(img_path))
        image_paths.append(str(img_path))

    if msg.document:
        mime = msg.document.mime_type or ""
        if mime.startswith("image/"):
            file = await context.bot.get_file(msg.document.file_id)
            ext = msg.document.file_name.split(".")[-1] if msg.document.file_name else "png"
            img_path = IMAGE_DIR / f"{msg.document.file_id}.{ext}"
            await file.download_to_drive(str(img_path))
            image_paths.append(str(img_path))

    if not prompt and not image_paths:
        return

    topic_key = get_topic_key(msg)
    sess = get_session(topic_key)

    if image_paths:
        img_instructions = "\n".join(f"Read and analyze this image file: {p}" for p in image_paths)
        prompt = f"{img_instructions}\n\n{prompt}" if prompt else img_instructions

    # Detect natural language intents
    if not image_paths:
        if looks_like_remember(prompt):
            result = mem.remember(prompt)
            log_conversation("user", prompt, topic_key=topic_key)
            log_conversation("assistant", result, topic_key=topic_key)
            await send_to_chat(msg,f"🧠 {result}")
            return

        if looks_like_recall(prompt):
            results = mem.search(prompt, n_results=5)
            if results:
                lines = ["🧠 Here's what I found:\n"]
                for r in results:
                    if r["distance"] > 0.5:
                        continue
                    ts = r["timestamp"][:16].replace("T", " ") if r["timestamp"] else ""
                    lines.append(f"[{ts}]\n{r['text'][:300]}\n")
                if len(lines) > 1:
                    log_conversation("user", prompt, topic_key=topic_key)
                    recall_text = "\n".join(lines)
                    log_conversation("assistant", recall_text, topic_key=topic_key)
                    for chunk in split_message(recall_text, 4000):
                        await send_to_chat(msg,chunk)
                    return

    if sess.get("busy"):
        sess.setdefault("pending", []).append({"prompt": prompt, "update": update, "context": context})
        position = len(sess["pending"])
        await send_to_chat(msg, f"📥 Queued (position {position}). Will run when current finishes.\n\nSend /stop to cancel the current request and skip ahead to your queued message. /new to wipe everything.")
        return

    await _process_prompt(prompt, update, context, topic_key)


async def _process_prompt(prompt: str, update: Update, context: ContextTypes.DEFAULT_TYPE, topic_key: str):
    """Run a prompt through Claude streaming. Handles queueing follow-ups."""
    sess = get_session(topic_key)
    msg = update.message
    sess["busy"] = True
    status_msg = await send_to_chat(msg, "⏳")

    async def _runner():
        try:
            logger.info(f"Message from {USER_NAME} [topic:{topic_key}]: {prompt[:100]}")
            log_conversation("user", prompt, sess.get("id"), topic_key=topic_key)

            context_parts = []
            topic_history = get_topic_history(topic_key)
            if topic_history:
                context_parts.append(topic_history)
            memory_context = mem.get_context_for_prompt(prompt)
            if memory_context:
                context_parts.append(memory_context)
            if context_parts:
                augmented = "\n\n---\n".join(context_parts) + f"\n\n---\nCurrent request: {prompt}"
            else:
                augmented = prompt

            override = TOPIC_OVERRIDES.get(topic_key, {})
            if override.get("prompt_prefix"):
                augmented = override["prompt_prefix"] + augmented

            await run_claude_streaming(augmented, update, context, status_msg, topic_key, cwd=override.get("cwd"))
        except asyncio.CancelledError:
            await safe_edit(status_msg, "🛑 Stopped by /stop.")
            raise
        except asyncio.TimeoutError:
            await safe_edit(status_msg, "Timed out (5 min limit). Try again or /new.")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            await safe_edit(status_msg, f"Error: {str(e)[:500]}")

    task = asyncio.create_task(_runner())
    sess["current_task"] = task
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        sess["busy"] = False
        sess["current_task"] = None

    # Process next queued prompt if any
    pending = sess.get("pending") or []
    if pending:
        nxt = pending.pop(0)
        # Schedule it without blocking this handler return
        asyncio.create_task(_process_prompt(nxt["prompt"], nxt["update"], nxt["context"], topic_key))


HELP_TEXT = """Claude Code Assistant

Just talk to me — text, photos, documents. Describe recurring tasks naturally and I'll schedule them.

Session:
/new — Clear conversation in this thread (stops current request + drops queue)
/stop — Cancel the in-flight Claude request in this thread (queued messages still run)
/details — Send full transcript (thinking, tool calls, results) of this thread's most recent Claude session as a JSONL file

Sending while busy: your message gets queued and runs after the current one finishes. Use /stop to skip ahead.
/status — Session info, job count, memory stats

Scheduled Jobs:
/jobs — List all scheduled jobs
/addjob — Add a job (name | cron | delivery | prompt)
/runjob <#> — Run a job immediately
/lastrun [# or name] — Show last run record (output, status, duration). Omit to see most recent run of any job.
/rmjob <#> — Delete a job
/editjob <#> <field> <value> — Edit a job
/togglejob <#> — Pause/resume a job
/templates — Example job templates
Or just say "every morning do X" and I'll set it up.

Memory:
/recall <query> — Semantic search across all past conversations
/remember <text> — Save a fact to memory
/forget <query> — Remove facts from memory
Or just say "remember that..." or "what did we discuss about..."

History:
/search <query> — Keyword search conversation logs
/history [count] — Show recent conversation entries

Tips:
• Each topic has its own conversation context
• Reply to a message to include it as context
• Send images and I'll analyze them
• Jobs created in a topic post results back to that topic"""


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await send_to_chat(update.message, HELP_TEXT)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await send_to_chat(update.message, HELP_TEXT)


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    topic_key = get_topic_key(update.message)
    sess = get_session(topic_key)
    jobs = load_jobs()
    active_jobs = [j for j in jobs if j.get("enabled", True)]
    stats = mem.stats()

    # Per-job health summary for active jobs
    job_lines = []
    any_unhealthy = False
    for j in active_jobs:
        name = j.get("name", "?")
        ls = j.get("last_status", "—")
        ce = int(j.get("consecutive_errors", 0))
        dur = j.get("last_duration_ms")
        last = j.get("last_run")
        if last:
            try:
                last = datetime.fromisoformat(last).strftime("%m/%d %H:%M")
            except Exception:
                last = str(last)[:16]
        else:
            last = "never"
        icon = "✅" if ls == "ok" else ("⏳" if ls in ("—", None) else "⚠️")
        if ce >= ERROR_ALERT_THRESHOLD:
            icon = "🚨"
            any_unhealthy = True
        elif ce > 0:
            any_unhealthy = True
        dur_s = f"{dur/1000:.1f}s" if isinstance(dur, (int, float)) else "—"
        err_note = f" • {ce} err{'s' if ce != 1 else ''} in a row" if ce else ""
        job_lines.append(f"  {icon} {name}: {ls} • {dur_s} • {last}{err_note}")

    health_line = "🚨 UNHEALTHY" if any_unhealthy else "✅ All jobs healthy"
    job_block = "\n".join(job_lines) if job_lines else "  (no active jobs)"

    await send_to_chat(update.message,
        f"Topic: {topic_key}\n"
        f"Session: {sess.get('id') or 'none'}\n"
        f"Status: {'busy' if sess.get('busy') else 'idle'}\n"
        f"Active topics: {len(sessions)}\n"
        f"Jobs: {len(active_jobs)} active / {len(jobs)} total — {health_line}\n"
        f"{job_block}\n"
        f"Memory: {stats['conversations']} convos, {stats['facts']} facts\n"
        f"Time: {now_tz().strftime('%I:%M%p %Z')}"
    )


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the in-flight Claude session for this topic. Doesn't clear queued messages."""
    if not is_authorized(update):
        return
    msg = update.message
    topic_key = get_topic_key(msg)
    sess = get_session(topic_key)
    task = sess.get("current_task")
    if task and not task.done():
        task.cancel()
        await send_to_chat(msg, "🛑 Cancelling current request. Queued messages (if any) will run next.")
    else:
        await send_to_chat(msg, "Nothing running in this thread.")


async def details_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the full Claude Code session transcript (JSONL) for the current topic's most recent session."""
    if not is_authorized(update):
        return
    msg = update.message
    topic_key = get_topic_key(msg)
    sess = get_session(topic_key)
    sid = sess.get("id")
    if not sid:
        await send_to_chat(msg, "No session yet for this topic. Send a message first.")
        return
    transcript = Path.home() / ".claude" / "projects" / "-Users-grant" / f"{sid}.jsonl"
    if not transcript.exists():
        await send_to_chat(msg, f"Transcript not found at {transcript}")
        return
    size_kb = transcript.stat().st_size / 1024
    try:
        bot = Bot(BOT_TOKEN)
        kwargs = {
            "chat_id": msg.chat_id,
            "document": transcript.open("rb"),
            "filename": f"transcript-{topic_key}-{sid[:8]}.jsonl",
            "caption": f"Session {sid[:8]} ({size_kb:.1f} KB) — full transcript with thinking, tool calls, and results.",
        }
        if msg.message_thread_id:
            kwargs["message_thread_id"] = msg.message_thread_id
        await bot.send_document(**kwargs)
    except Exception as e:
        await send_to_chat(msg, f"Failed to send transcript: {e}")


async def lastrun_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show last persisted run record. /lastrun [job_index_or_name]"""
    if not is_authorized(update):
        return
    arg = update.message.text.replace("/lastrun", "", 1).strip()
    jobs = load_jobs()

    # Resolve target job name (optional)
    target_name = None
    if arg:
        if arg.isdigit():
            idx = int(arg)
            if 0 <= idx < len(jobs):
                target_name = jobs[idx].get("name")
        else:
            # Substring match on name
            for j in jobs:
                if arg.lower() in j.get("name", "").lower():
                    target_name = j.get("name")
                    break
        if not target_name:
            await send_to_chat(update.message, f"No job matches '{arg}'. Try /jobs to see list.")
            return

    # Walk runs dir newest-first
    if not RUNS_DIR.exists():
        await send_to_chat(update.message, "No runs recorded yet.")
        return

    day_dirs = sorted([d for d in RUNS_DIR.iterdir() if d.is_dir()], reverse=True)
    found = None
    for day in day_dirs:
        files = sorted(day.iterdir(), reverse=True)
        for f in files:
            try:
                rec = json.loads(f.read_text())
            except Exception:
                continue
            if target_name is None or rec.get("job") == target_name:
                found = rec
                break
        if found:
            break

    if not found:
        await send_to_chat(update.message, f"No runs found{' for ' + target_name if target_name else ''}.")
        return

    started = found.get("started_at", "?")
    try:
        started = datetime.fromisoformat(started).strftime("%m/%d %H:%M:%S")
    except Exception:
        pass
    dur_s = found.get("duration_ms", 0) / 1000
    status_icon = "✅" if found.get("status") == "ok" else "⚠️"
    out = (found.get("output") or "").strip()
    err = (found.get("error") or "").strip()
    msg = (
        f"{status_icon} {found.get('job')}\n"
        f"When: {started} ({dur_s:.1f}s)\n"
        f"Status: {found.get('status')} • Delivery: {found.get('delivery_status') or '—'}\n"
    )
    if err:
        msg += f"Error: {err[:500]}\n"
    if out:
        msg += f"\nOutput:\n{out[:2000]}"
        if len(out) > 2000:
            msg += f"\n... [truncated, {len(out)} chars total]"
    await send_to_chat(update.message, msg[:4000])


async def new_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    topic_key = get_topic_key(update.message)
    old = sessions.get(topic_key, {})
    task = old.get("current_task")
    if task and not task.done():
        task.cancel()
    sessions[topic_key] = {"id": None, "busy": False, "pending": [], "current_task": None}
    _save_sessions_to_disk()
    await send_to_chat(update.message, "Session cleared for this thread (stopped current + dropped queue).")


async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search conversation history: /search <query>"""
    if not is_authorized(update):
        return
    query = update.message.text.replace("/search", "", 1).strip().lower()
    if not query:
        await send_to_chat(update.message,"Usage: /search <query>\nSearches all past conversations.")
        return
    if not HISTORY_FILE.exists():
        await send_to_chat(update.message,"No conversation history yet.")
        return

    matches = []
    for line in HISTORY_FILE.read_text().splitlines():
        try:
            entry = json.loads(line)
            if query in entry.get("text", "").lower():
                ts = entry["ts"][:16].replace("T", " ")
                role = entry["role"]
                text = entry["text"][:150]
                matches.append(f"[{ts}] {role}: {text}")
        except json.JSONDecodeError:
            continue

    if not matches:
        await send_to_chat(update.message,f"No results for: {query}")
        return

    # Show most recent matches (last 15)
    recent = matches[-15:]
    header = f"🔍 {len(matches)} results for \"{query}\""
    if len(matches) > 15:
        header += f" (showing last 15)"
    result = header + "\n\n" + "\n\n".join(recent)
    for chunk in split_message(result, 4000):
        await send_to_chat(update.message,chunk)


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show recent conversation history: /history [count]"""
    if not is_authorized(update):
        return
    if not HISTORY_FILE.exists():
        await send_to_chat(update.message,"No conversation history yet.")
        return

    text = update.message.text.replace("/history", "", 1).strip()
    count = int(text) if text.isdigit() else 20

    lines = HISTORY_FILE.read_text().splitlines()
    recent = lines[-count:]

    entries = []
    for line in recent:
        try:
            entry = json.loads(line)
            ts = entry["ts"][:16].replace("T", " ")
            role = entry["role"]
            text = entry["text"][:200]
            entries.append(f"[{ts}] {role}: {text}")
        except json.JSONDecodeError:
            continue

    if not entries:
        await send_to_chat(update.message,"No history found.")
        return

    total = len(lines)
    result = f"📜 Last {len(entries)} of {total} total entries\n\n" + "\n\n".join(entries)
    for chunk in split_message(result, 4000):
        await send_to_chat(update.message,chunk)


async def recall_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Semantic search across all memory: /recall <query>"""
    if not is_authorized(update):
        return
    query = update.message.text.replace("/recall", "", 1).strip()
    if not query:
        stats = mem.stats()
        await send_to_chat(update.message,
            f"Usage: /recall <query>\n"
            f"Semantically searches all past conversations and saved facts.\n\n"
            f"Memory stats:\n"
            f"  Conversations: {stats['conversations']}\n"
            f"  Facts: {stats['facts']}"
        )
        return

    results = mem.search(query, n_results=10)
    if not results:
        await send_to_chat(update.message,f"No memories found for: {query}")
        return

    lines = [f"🧠 Memory search: \"{query}\"\n"]
    for r in results:
        ts = r["timestamp"][:16].replace("T", " ") if r["timestamp"] else "?"
        dist = f"{r['distance']:.2f}"
        rtype = r["type"]
        text = r["text"][:300]
        lines.append(f"[{ts}] ({rtype}, relevance: {dist})\n{text}\n")

    result = "\n".join(lines)
    for chunk in split_message(result, 4000):
        await send_to_chat(update.message,chunk)


async def remember_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Explicitly save a fact: /remember <text>"""
    if not is_authorized(update):
        return
    text = update.message.text.replace("/remember", "", 1).strip()
    if not text:
        await send_to_chat(update.message,"Usage: /remember <something to save>")
        return
    result = mem.remember(text)
    await send_to_chat(update.message,f"🧠 {result}")


async def forget_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove facts from memory: /forget <query>"""
    if not is_authorized(update):
        return
    query = update.message.text.replace("/forget", "", 1).strip()
    if not query:
        await send_to_chat(update.message,"Usage: /forget <what to forget>")
        return
    result = mem.forget(query)
    await send_to_chat(update.message,f"🧠 {result}")


async def send_response(update: Update, context: ContextTypes.DEFAULT_TYPE, response: str, topic_key: str = "dm"):
    sess = get_session(topic_key)
    log_conversation("assistant", response, sess.get("id"), topic_key=topic_key)

    # Store conversation pair in vector memory
    user_msg = update.message.text or update.message.caption or ""
    if user_msg:
        mem.add_conversation(user_msg, response, sess.get("id"))
    img_pattern = re.compile(r'(/[\w/.~-]+\.(?:png|jpg|jpeg|gif|webp))', re.IGNORECASE)
    for img_path in img_pattern.findall(response):
        expanded = os.path.expanduser(img_path)
        if os.path.exists(expanded) and expanded not in sent_images:
            try:
                with open(expanded, "rb") as f:
                    params = {"chat_id": update.message.chat.id, "photo": f}
                    if update.message.message_thread_id:
                        params["message_thread_id"] = update.message.message_thread_id
                    await update.message.get_bot().send_photo(**params)
                sent_images.add(expanded)
            except Exception as e:
                logger.warning(f"Failed to send image {img_path}: {e}")

    if response.strip():
        for chunk in split_message(response, 4000):
            try:
                await send_to_chat(update.message,chunk, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                try:
                    await send_to_chat(update.message,chunk)
                except Exception as e:
                    logger.error(f"Failed to send chunk: {e}")


async def safe_edit(msg, text):
    try:
        await msg.edit_text(text[:4096])
    except Exception:
        pass


def split_message(text: str, limit: int = 4000) -> list:
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at < limit // 2:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


# ===================== STARTUP =====================

async def post_init(app: Application):
    _load_sessions_from_disk()
    asyncio.create_task(job_scheduler())
    asyncio.create_task(email_webhook_server())


def main():
    print(f"Claude Code Telegram Assistant")
    print(f"User: {USER_NAME} ({USER_ID})")
    print(f"Timezone: {TZ}")
    print(f"Jobs: {JOBS_FILE}")
    print(f"Email: {'enabled' if EMAIL_CFG.get('enabled') else 'disabled'}")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).concurrent_updates(True).build()

    for name, handler in [
        ("start", start_cmd), ("help", help_cmd), ("status", status_cmd), ("new", new_cmd),
        ("jobs", jobs_cmd), ("addjob", addjob_cmd), ("rmjob", rmjob_cmd),
        ("togglejob", togglejob_cmd), ("editjob", editjob_cmd),
        ("runjob", runjob_cmd), ("templates", templates_cmd), ("lastrun", lastrun_cmd),
        ("details", details_cmd), ("stop", stop_cmd),
        ("search", search_cmd), ("history", history_cmd),
        ("recall", recall_cmd), ("remember", remember_cmd), ("forget", forget_cmd),
    ]:
        app.add_handler(CommandHandler(name, handler))

    app.add_handler(MessageHandler(
        filters.TEXT | filters.PHOTO | filters.Document.ALL,
        handle_message
    ))

    print("Bot running. Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
