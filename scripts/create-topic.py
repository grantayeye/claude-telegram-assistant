#!/usr/bin/env python3
"""Create a new forum topic in Telbot's group. Usage: create-topic.py "Topic Name" [icon_color]
Icon colors (decimal): 7322096 blue, 16766590 yellow, 13338331 purple, 9367192 green,
                       16749490 red, 16478047 orange, 16766590 yellow (default).
Prints the new topic id.
"""
import sys, json, urllib.request, urllib.parse
from pathlib import Path

CFG = json.loads((Path(__file__).parent.parent / "config.json").read_text())
BOT_TOKEN = CFG["bot_token"]
CHAT_ID = CFG["group_chat_id"]

name = sys.argv[1] if len(sys.argv) > 1 else "Untitled"
color = int(sys.argv[2]) if len(sys.argv) > 2 else 7322096  # default blue

url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
payload = urllib.parse.urlencode({
    "chat_id": CHAT_ID,
    "name": name,
    "icon_color": color,
}).encode()

req = urllib.request.Request(url, data=payload, method="POST")
try:
    with urllib.request.urlopen(req, timeout=15) as resp:
        body = json.loads(resp.read())
except urllib.error.HTTPError as e:
    body = json.loads(e.read())

if body.get("ok"):
    r = body["result"]
    print(f"Created topic: id={r['message_thread_id']} name={r['name']!r}")
    print(r["message_thread_id"])
else:
    print(f"ERROR: {body.get('description', body)}", file=sys.stderr)
    sys.exit(1)
