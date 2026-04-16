import os
import re

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
SESSION_STRING = (os.getenv("SESSION_STRING") or "").strip()
LOG_FILE = os.getenv("LOG_FILE", "bot_log.txt")
PORT = int(os.getenv("PORT", "10000"))

DELAY_MIN_SECONDS = int(os.getenv("DELAY_MIN_SECONDS", "20"))
DELAY_MAX_SECONDS = int(os.getenv("DELAY_MAX_SECONDS", "50"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
INITIAL_SCAN_LIMIT = int(os.getenv("INITIAL_SCAN_LIMIT", "100"))
LATEST_RECHECK_LIMIT = int(os.getenv("LATEST_RECHECK_LIMIT", "10"))
RECENT_IDS_LIMIT = int(os.getenv("RECENT_IDS_LIMIT", "100"))

DEFAULT_SCAN_COUNT = 100
SCAN_COUNT_ALL = -1

URL_REGEX = re.compile(r'(?i)\b(?:https?://|www\.|t\.me/)\S+')
INVITE_LINK_RE = re.compile(
    r"(?i)(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([A-Za-z0-9_-]+)"
)
TG_JOIN_INVITE_RE = re.compile(r"(?i)tg://join\?invite=([A-Za-z0-9_-]+)")

if not API_ID or not API_HASH:
    raise ValueError("API_ID and API_HASH are required")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is required")

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
