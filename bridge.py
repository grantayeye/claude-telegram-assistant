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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from croniter import croniter
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
TZ = ZoneInfo(CFG.get("timezone", "America/New_York"))
CLAUDE_PATH = CFG.get("claude_path", "claude")
CONTEXT_DIRS = [os.path.expanduser(d) for d in CFG.get("context_dirs", [])]
EMAIL_CFG = CFG.get("email", {})

SESSION_DIR = Path(tempfile.mkdtemp(prefix="claude-tg-"))
IMAGE_DIR = SESSION_DIR / "images"
IMAGE_DIR.mkdir(exist_ok=True)
JOBS_FILE = APP_DIR / "jobs.json"

# Session state
session = {"id": None, "busy": False}
sent_images = set()


def is_authorized(update: Update) -> bool:
    return update.effective_user and update.effective_user.id == USER_ID


def now_tz() -> datetime:
    return datetime.now(TZ)


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
        await update.message.reply_text("No jobs. Use /addjob or just describe a recurring task.")
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
        lines.append(
            f"{enabled} [{i}] {delivery} {job['name']}\n"
            f"   Schedule: {job.get('schedule', '?')}\n"
            f"   Next: {nxt} | Last: {last}\n"
            f"   {job['prompt'][:60]}..."
        )
    await update.message.reply_text("\n".join(lines))


async def templates_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await update.message.reply_text(
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
        await update.message.reply_text(
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
        await update.message.reply_text("Need 4 parts: name | cron | delivery | prompt\n\nOr just describe it naturally.")
        return

    name = parts[0]
    schedule = parts[1]
    delivery = parts[2].lower()
    prompt = "|".join(parts[3:])

    try:
        croniter(schedule)
    except Exception:
        await update.message.reply_text(f"Invalid cron: {schedule}")
        return

    if delivery not in ("telegram", "email", "both", "silent", "on-failure"):
        await update.message.reply_text("Delivery must be: telegram, email, both, silent, or on-failure")
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
    await update.message.reply_text(f"✅ Added: {name}\n{icon} Delivery: {delivery}\nNext run: {nxt}")


async def rmjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/rmjob", "", 1).strip()
    jobs = load_jobs()
    try:
        idx = int(text)
        removed = jobs.pop(idx)
        save_jobs(jobs)
        await update.message.reply_text(f"Removed: {removed['name']}")
    except (ValueError, IndexError):
        await update.message.reply_text(f"Usage: /rmjob <index> (0-{len(jobs)-1})")


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
        await update.message.reply_text(f"{jobs[idx]['name']}: {state}")
    except (ValueError, IndexError):
        await update.message.reply_text(f"Usage: /togglejob <index>")


async def editjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/editjob", "", 1).strip()
    parts = text.split(None, 2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /editjob <index> <field> <value>\nFields: name, schedule, delivery, prompt")
        return
    jobs = load_jobs()
    try:
        idx = int(parts[0])
        field = parts[1].lower()
        value = parts[2]
        if field not in ("name", "schedule", "delivery", "prompt"):
            await update.message.reply_text("Fields: name, schedule, delivery, prompt")
            return
        if field == "schedule":
            croniter(value)
        if field == "delivery" and value not in ("telegram", "email", "both", "silent", "on-failure"):
            await update.message.reply_text("Delivery: telegram, email, both, silent, on-failure")
            return
        jobs[idx][field] = value
        save_jobs(jobs)
        await update.message.reply_text(f"Updated {jobs[idx]['name']}: {field} = {value}")
    except (ValueError, IndexError) as e:
        await update.message.reply_text(f"Error: {e}")


async def runjob_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    text = update.message.text.replace("/runjob", "", 1).strip()
    jobs = load_jobs()
    try:
        idx = int(text)
        job = jobs[idx]
        await update.message.reply_text(f"Running: {job['name']}...")
        result = await run_claude_oneshot(job["prompt"])
        await deliver_result(job, result)
        jobs[idx]["last_run"] = now_tz().isoformat()
        save_jobs(jobs)
    except (ValueError, IndexError):
        await update.message.reply_text(f"Usage: /runjob <index>")
    except Exception as e:
        await update.message.reply_text(f"Job error: {str(e)[:500]}")


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


def looks_like_recurring(text: str) -> bool:
    lower = text.lower()
    return any(sig in lower for sig in RECURRING_SIGNALS)


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
    status = await msg.reply_text("🔧 Setting up recurring task...")

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
        await send_telegram(f"📋 {name}:\n\n{result}")

    if delivery in ("email", "both"):
        await send_email(subject=f"[Claude] {name}", body=result)


async def send_telegram(text: str):
    bot = Bot(BOT_TOKEN)
    chunks = split_message(text, 4000)
    for chunk in chunks:
        try:
            await bot.send_message(chat_id=USER_ID, text=chunk, parse_mode=ParseMode.MARKDOWN)
        except Exception:
            try:
                await bot.send_message(chat_id=USER_ID, text=chunk)
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

async def job_scheduler():
    logger.info("Job scheduler started")
    while True:
        await asyncio.sleep(30)
        try:
            jobs = load_jobs()
            changed = False
            current = now_tz()

            for i, job in enumerate(jobs):
                if not job.get("enabled", True):
                    continue
                schedule = job.get("schedule", "")
                if not schedule:
                    continue

                last_run = job.get("last_run")
                should_run = False

                if last_run is None:
                    try:
                        croniter(schedule, current)
                        should_run = True
                    except Exception:
                        continue
                else:
                    try:
                        last_dt = datetime.fromisoformat(last_run)
                        if last_dt.tzinfo is None:
                            last_dt = last_dt.replace(tzinfo=TZ)
                        cron = croniter(schedule, last_dt)
                        if current >= cron.get_next(datetime):
                            should_run = True
                    except Exception:
                        continue

                if should_run:
                    logger.info(f"Running scheduled job: {job['name']}")
                    try:
                        result = await run_claude_oneshot(job["prompt"])
                        await deliver_result(job, result)
                    except Exception as e:
                        logger.error(f"Job '{job['name']}' failed: {e}")
                        await send_telegram(f"❌ Job '{job['name']}' failed: {str(e)[:300]}")
                    jobs[i]["last_run"] = current.isoformat()
                    changed = True

            if changed:
                save_jobs(jobs)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")


# ===================== CLAUDE EXECUTION =====================

def build_claude_cmd(output_format="json", streaming=False):
    cmd = [CLAUDE_PATH, "--print", "--dangerously-skip-permissions"]
    if streaming:
        cmd.extend(["--verbose", "--output-format", "stream-json", "--include-partial-messages"])
    else:
        cmd.extend(["--output-format", output_format])
    for d in CONTEXT_DIRS:
        cmd.extend(["--add-dir", d])
    return cmd


async def run_claude_oneshot(prompt: str) -> str:
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

    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=300)
    raw = stdout.decode("utf-8", errors="replace")
    try:
        return json.loads(raw).get("result", raw)
    except json.JSONDecodeError:
        return raw


async def run_claude_streaming(prompt: str, update: Update, context: ContextTypes.DEFAULT_TYPE, status_msg):
    cmd = build_claude_cmd(streaming=True)
    if session.get("id"):
        cmd.extend(["--resume", session["id"]])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(Path.home())
    )
    proc.stdin.write(prompt.encode("utf-8"))
    proc.stdin.close()

    full_text = ""
    streaming_text = ""
    last_update_len = 0

    try:
        async for line in proc.stdout:
            line = line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = event.get("type", "")
            sid = event.get("session_id")
            if sid:
                session["id"] = sid

            if etype == "stream_event":
                inner = event.get("event", {})
                inner_type = inner.get("type", "")
                if inner_type == "content_block_delta":
                    delta = inner.get("delta", {})
                    if delta.get("type") == "text_delta":
                        streaming_text += delta.get("text", "")
                        if len(streaming_text) - last_update_len > 300:
                            await safe_edit(status_msg, f"⏳ ...{streaming_text[-200:]}")
                            last_update_len = len(streaming_text)
                elif inner_type == "content_block_start":
                    block = inner.get("content_block", {})
                    if block.get("type") == "tool_use":
                        await safe_edit(status_msg, f"🔧 Using {block.get('name', 'tool')}...")
                        streaming_text = ""
                        last_update_len = 0

            elif etype == "result":
                full_text = event.get("result", "")
                logger.info(f"Result: {len(full_text)} chars")

    except asyncio.CancelledError:
        proc.terminate()
        raise

    await asyncio.wait_for(proc.wait(), timeout=10)

    if not full_text.strip() and streaming_text.strip():
        full_text = streaming_text

    if not full_text.strip():
        await safe_edit(status_msg, "Claude returned an empty response.")
        return

    try:
        await status_msg.delete()
    except Exception:
        pass

    await send_response(update, context, full_text)


# ===================== MESSAGE HANDLING =====================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return

    msg = update.message
    if not msg:
        return

    if session.get("busy"):
        await msg.reply_text("Still working on the last request. Wait or /new to reset.")
        return

    prompt = msg.text or msg.caption or ""

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

    if image_paths:
        img_instructions = "\n".join(f"Read and analyze this image file: {p}" for p in image_paths)
        prompt = f"{img_instructions}\n\n{prompt}" if prompt else img_instructions

    # Detect recurring task requests
    if not image_paths and looks_like_recurring(prompt):
        session["busy"] = True
        try:
            await parse_and_create_job(prompt, update)
        finally:
            session["busy"] = False
        return

    session["busy"] = True
    status_msg = await msg.reply_text("⏳")

    try:
        logger.info(f"Message from {USER_NAME}: {prompt[:100]}")
        await run_claude_streaming(prompt, update, context, status_msg)
    except asyncio.TimeoutError:
        await safe_edit(status_msg, "Timed out (5 min limit). Try again or /new.")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await safe_edit(status_msg, f"Error: {str(e)[:500]}")
    finally:
        session["busy"] = False


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    await update.message.reply_text(
        f"Claude Code Assistant\n\n"
        f"Hi {USER_NAME}! Just talk to me — text, photos, documents.\n"
        f"Describe recurring tasks naturally and I'll schedule them.\n\n"
        f"Session: /new /status\n"
        f"Jobs: /jobs /addjob /templates\n"
        f"      /runjob /rmjob /editjob /togglejob"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    jobs = load_jobs()
    active = sum(1 for j in jobs if j.get("enabled", True))
    await update.message.reply_text(
        f"Session: {session.get('id') or 'none'}\n"
        f"Status: {'busy' if session.get('busy') else 'idle'}\n"
        f"Jobs: {active} active / {len(jobs)} total\n"
        f"Time: {now_tz().strftime('%I:%M%p %Z')}"
    )


async def new_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    session["id"] = None
    sent_images.clear()
    await update.message.reply_text("Session cleared.")


async def send_response(update: Update, context: ContextTypes.DEFAULT_TYPE, response: str):
    img_pattern = re.compile(r'(/[\w/.~-]+\.(?:png|jpg|jpeg|gif|webp))', re.IGNORECASE)
    for img_path in img_pattern.findall(response):
        expanded = os.path.expanduser(img_path)
        if os.path.exists(expanded) and expanded not in sent_images:
            try:
                with open(expanded, "rb") as f:
                    await update.message.reply_photo(photo=f)
                sent_images.add(expanded)
            except Exception as e:
                logger.warning(f"Failed to send image {img_path}: {e}")

    if response.strip():
        for chunk in split_message(response, 4000):
            try:
                await update.message.reply_text(chunk, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                try:
                    await update.message.reply_text(chunk)
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
    asyncio.create_task(job_scheduler())


def main():
    print(f"Claude Code Telegram Assistant")
    print(f"User: {USER_NAME} ({USER_ID})")
    print(f"Timezone: {TZ}")
    print(f"Jobs: {JOBS_FILE}")
    print(f"Email: {'enabled' if EMAIL_CFG.get('enabled') else 'disabled'}")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    for name, handler in [
        ("start", start_cmd), ("status", status_cmd), ("new", new_cmd),
        ("jobs", jobs_cmd), ("addjob", addjob_cmd), ("rmjob", rmjob_cmd),
        ("togglejob", togglejob_cmd), ("editjob", editjob_cmd),
        ("runjob", runjob_cmd), ("templates", templates_cmd),
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
