import asyncio
import json
import logging
import os
import random
import re
import time
from threading import Thread
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from flask import Flask
import psycopg
from psycopg.rows import dict_row
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo


# =========================================================
# CONFIG
# =========================================================
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
SESSION_STRING = os.getenv("SESSION_STRING")
DATABASE_URL = os.getenv("DATABASE_URL")
LOG_FILE = os.getenv("LOG_FILE", "bot_log.txt")

MIN_DELAY_SECONDS = int(os.getenv("MIN_DELAY_SECONDS", "5"))
NORMAL_DELAY_MIN = int(os.getenv("NORMAL_DELAY_MIN", "30"))
NORMAL_DELAY_MAX = int(os.getenv("NORMAL_DELAY_MAX", "60"))
RECENT_IDS_LIMIT = int(os.getenv("RECENT_IDS_LIMIT", "100"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
LATEST_RECHECK_LIMIT = int(os.getenv("LATEST_RECHECK_LIMIT", "10"))
KEEP_ALIVE_PORT = int(os.getenv("PORT", "10000"))

URL_REGEX = re.compile(r'(?i)\b(?:https?://|www\.|t\.me/)[^\s<>"\'\]\)]+' )

if not API_ID or not API_HASH:
    raise ValueError("API_ID and API_HASH are required")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is required")


# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =========================================================
# KEEP ALIVE FOR RENDER
# =========================================================
app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is alive"


@app.route("/healthz")
def healthz():
    return "ok"


def run_web() -> None:
    app.run(host="0.0.0.0", port=KEEP_ALIVE_PORT)


def keep_alive() -> None:
    t = Thread(target=run_web, daemon=True)
    t.start()


# =========================================================
# GLOBALS
# =========================================================
if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

owner_id: Optional[int] = None
pending_input_state: Dict[int, Dict[str, Any]] = {}
runtime_cache: Dict[str, Dict[str, Any]] = {}

# in-memory mirror loaded from DB
user_data: Dict[str, Dict[str, Any]] = {}


# =========================================================
# DB HELPERS
# =========================================================
def get_db_connection():
    return psycopg.connect(DATABASE_URL, sslmode="require", row_factory=dict_row)


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_configs (
                    user_id BIGINT PRIMARY KEY
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_pairs (
                    user_id BIGINT NOT NULL,
                    pair_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    channel_a TEXT NOT NULL DEFAULT '',
                    channel_b TEXT NOT NULL DEFAULT '',
                    last_processed_id BIGINT NOT NULL DEFAULT 0,
                    recent_sent_ids TEXT NOT NULL DEFAULT '[]',
                    PRIMARY KEY (user_id, pair_id)
                )
                """
            )
        conn.commit()


def load_all_user_data() -> Dict[str, Dict[str, Any]]:
    data: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM user_configs")
            users = cur.fetchall()
            for row in users:
                user_id = str(row["user_id"])
                data[user_id] = {"pairs": []}

            cur.execute(
                """
                SELECT user_id, pair_id, name, channel_a, channel_b, last_processed_id, recent_sent_ids
                FROM user_pairs
                ORDER BY user_id, pair_id
                """
            )
            pairs = cur.fetchall()
            for row in pairs:
                user_id = str(row["user_id"])
                if user_id not in data:
                    data[user_id] = {"pairs": []}
                try:
                    recent_ids = json.loads(row["recent_sent_ids"] or "[]")
                except Exception:
                    recent_ids = []
                data[user_id]["pairs"].append({
                    "pair_id": int(row["pair_id"]),
                    "name": row["name"],
                    "channel_a": row["channel_a"],
                    "channel_b": row["channel_b"],
                    "last_processed_id": int(row["last_processed_id"] or 0),
                    "recent_sent_ids": recent_ids[-RECENT_IDS_LIMIT:],
                })
    return data


def save_user_record(user_id: int) -> None:
    key = str(user_id)
    record = user_data.get(key, {"pairs": []})

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO user_configs (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING",
                (user_id,),
            )
            cur.execute("DELETE FROM user_pairs WHERE user_id = %s", (user_id,))

            for pair in record.get("pairs", []):
                cur.execute(
                    """
                    INSERT INTO user_pairs (
                        user_id, pair_id, name, channel_a, channel_b, last_processed_id, recent_sent_ids
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        int(pair["pair_id"]),
                        pair.get("name", f"pair{pair['pair_id']}"),
                        pair.get("channel_a", ""),
                        pair.get("channel_b", ""),
                        int(pair.get("last_processed_id", 0)),
                        json.dumps(pair.get("recent_sent_ids", [])[-RECENT_IDS_LIMIT:]),
                    ),
                )
        conn.commit()


def ensure_user_record(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    if key not in user_data:
        user_data[key] = {"pairs": []}
        save_user_record(user_id)
    if "pairs" not in user_data[key] or not isinstance(user_data[key]["pairs"], list):
        user_data[key]["pairs"] = []
    return user_data[key]


def trim_recent_ids(ids_list: List[int], limit: int = RECENT_IDS_LIMIT) -> List[int]:
    return ids_list[-limit:]


def next_pair_id(record: Dict[str, Any]) -> int:
    pairs = record.get("pairs", [])
    if not pairs:
        return 1
    return max(int(p.get("pair_id", 0)) for p in pairs) + 1


def get_pair_by_id(user_id: int, pair_id: int) -> Optional[Dict[str, Any]]:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        if int(pair.get("pair_id", 0)) == int(pair_id):
            return pair
    return None


# =========================================================
# TEXT HELPERS
# =========================================================
def clean_urls(text: Optional[str]) -> str:
    if not text:
        return ""

    cleaned = URL_REGEX.sub("", text)
    cleaned = re.sub(r'\(\s*\)', '', cleaned)
    cleaned = re.sub(r'\[\s*\]', '', cleaned)
    cleaned = re.sub(r'<\s*>', '', cleaned)
    cleaned = re.sub(r'[ \t]{2,}', ' ', cleaned)
    cleaned = re.sub(r' *\n *', '\n', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    return cleaned.strip()


def get_clean_text_from_message(msg) -> str:
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    return clean_urls(raw)


def is_forwarded(msg) -> bool:
    return bool(getattr(msg, "fwd_from", None))


def is_video_message(msg) -> bool:
    if not msg:
        return False

    if getattr(msg, "video", None):
        return True

    if getattr(msg, "video_note", None):
        return True

    document = getattr(msg, "document", None)
    if document:
        mime = getattr(document, "mime_type", "") or ""
        if mime.startswith("video/"):
            return True

        for attr in getattr(document, "attributes", []):
            if isinstance(attr, DocumentAttributeVideo):
                return True

    return False


# =========================================================
# SAFE TELEGRAM CALLS
# =========================================================
async def run_with_floodwait(coro_factory, *args, **kwargs):
    while True:
        try:
            return await coro_factory(*args, **kwargs)
        except FloodWaitError as e:
            wait_time = int(getattr(e, "seconds", 0))
            logger.warning("FloodWait detected. Sleeping for %s seconds.", wait_time)
            await asyncio.sleep(wait_time)
        except Exception:
            raise


async def safe_get_entity(link_or_username: str):
    return await run_with_floodwait(client.get_entity, link_or_username)


async def safe_get_message(entity, msg_id: int):
    return await run_with_floodwait(client.get_messages, entity, ids=msg_id)


async def safe_send_message(entity, text: str):
    if not text.strip():
        return None
    return await run_with_floodwait(client.send_message, entity, text)


async def safe_send_file(entity, file, caption: Optional[str] = None):
    kwargs = {}
    if caption is not None:
        kwargs["caption"] = caption
    logger.info("SENDING FILE | caption_exists=%s", bool(caption))
    return await run_with_floodwait(client.send_file, entity, file, **kwargs)


async def safe_send_album(entity, files: List[Any], captions: List[str]):
    return await run_with_floodwait(client.send_file, entity, files, caption=captions)


# =========================================================
# RUNTIME CACHE
# =========================================================
def get_pair_runtime(user_id: int, pair_id: int) -> Dict[str, Any]:
    user_key = str(user_id)
    pair_key = str(pair_id)

    if user_key not in runtime_cache:
        runtime_cache[user_key] = {}

    if pair_key not in runtime_cache[user_key]:
        runtime_cache[user_key][pair_key] = {
            "source_entity": None,
            "dest_entity": None,
            "source_chat_id": None,
            "dest_chat_id": None,
            "lock": asyncio.Lock(),
            "last_action_time": 0.0,
            "processed_grouped_ids_live": set(),
        }

    return runtime_cache[user_key][pair_key]


async def resolve_pair_entities(user_id: int, pair: Dict[str, Any]) -> bool:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    channel_a = pair.get("channel_a", "").strip()
    channel_b = pair.get("channel_b", "").strip()

    if not channel_a or not channel_b:
        runtime["source_entity"] = None
        runtime["dest_entity"] = None
        runtime["source_chat_id"] = None
        runtime["dest_chat_id"] = None
        return False

    try:
        source_entity = await safe_get_entity(channel_a)
        dest_entity = await safe_get_entity(channel_b)

        runtime["source_entity"] = source_entity
        runtime["dest_entity"] = dest_entity
        runtime["source_chat_id"] = getattr(source_entity, "id", None)
        runtime["dest_chat_id"] = getattr(dest_entity, "id", None)

        logger.info(
            "Resolved pair %s for user %s | A=%s | B=%s",
            pair_id, user_id, channel_a, channel_b
        )
        return True
    except Exception as e:
        logger.exception("Failed to resolve pair %s for user %s: %s", pair_id, user_id, e)
        return False


async def resolve_all_pairs(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        await resolve_pair_entities(user_id, pair)


# =========================================================
# PAIR STATE HELPERS
# =========================================================
def is_duplicate(pair: Dict[str, Any], source_msg_id: int) -> bool:
    return source_msg_id in pair.get("recent_sent_ids", [])


def mark_sent_ids(user_id: int, pair: Dict[str, Any], source_ids: List[int]) -> None:
    recent_ids = pair.get("recent_sent_ids", [])
    for sid in source_ids:
        if sid not in recent_ids:
            recent_ids.append(sid)
    pair["recent_sent_ids"] = trim_recent_ids(recent_ids)
    save_user_record(user_id)


def update_last_processed(user_id: int, pair: Dict[str, Any], msg_id: int) -> None:
    if msg_id > int(pair.get("last_processed_id", 0)):
        pair["last_processed_id"] = msg_id
        save_user_record(user_id)


async def apply_human_delay(user_id: int, pair_id: int) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    last_action_time = runtime.get("last_action_time", 0.0)

    if last_action_time <= 0:
        return

    target_delay = max(MIN_DELAY_SECONDS, random.randint(NORMAL_DELAY_MIN, NORMAL_DELAY_MAX))
    elapsed = time.time() - last_action_time

    if elapsed < target_delay:
        wait_for = target_delay - elapsed
        logger.info("Human-like delay for pair %s: sleeping %.2f seconds", pair_id, wait_for)
        await asyncio.sleep(wait_for)


def mark_action_done(user_id: int, pair_id: int) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["last_action_time"] = time.time()


# =========================================================
# SEND LOGIC
# =========================================================
async def repost_previous_message_if_valid(user_id: int, pair: Dict[str, Any], previous_msg) -> List[int]:
    if not previous_msg:
        return []

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    dest_entity = runtime["dest_entity"]

    if is_forwarded(previous_msg):
        logger.info("Previous message %s is forwarded. Skipping previous.", previous_msg.id)
        return []

    if is_duplicate(pair, previous_msg.id):
        logger.info("Previous message %s already sent. Skipping duplicate.", previous_msg.id)
        return []

    try:
        if previous_msg.media:
            caption = get_clean_text_from_message(previous_msg)
            await safe_send_file(dest_entity, previous_msg.media, caption=caption if caption else None)
        else:
            text = get_clean_text_from_message(previous_msg)
            if text:
                await safe_send_message(dest_entity, text)
            else:
                logger.info("Previous message %s empty after URL cleaning. Skipped.", previous_msg.id)
                return []
        logger.info("Reposted previous message %s for pair %s", previous_msg.id, pair_id)
        return [previous_msg.id]
    except Exception:
        logger.exception("Failed to repost previous message %s for pair %s", previous_msg.id, pair_id)
        return []


async def repost_single_video_message(user_id: int, pair: Dict[str, Any], msg) -> List[int]:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_entity = runtime["source_entity"]
    dest_entity = runtime["dest_entity"]

    if is_forwarded(msg):
        logger.info("Video message %s is forwarded. Skipping pair %s.", msg.id, pair_id)
        return []

    if is_duplicate(pair, msg.id):
        logger.info("Video message %s already sent. Skipping duplicate for pair %s.", msg.id, pair_id)
        return []

    logger.info(
        "TRY SEND VIDEO | pair_id=%s | msg_id=%s | forwarded=%s | duplicate=%s",
        pair_id, msg.id, is_forwarded(msg), is_duplicate(pair, msg.id),
    )

    sent_source_ids: List[int] = []

    previous_msg = None
    try:
        previous_msg = await safe_get_message(source_entity, msg.id - 1)
    except Exception:
        logger.exception("Failed to fetch previous message for video %s pair %s", msg.id, pair_id)

    sent_source_ids.extend(await repost_previous_message_if_valid(user_id, pair, previous_msg))

    try:
        caption = get_clean_text_from_message(msg)
        await safe_send_file(dest_entity, msg.media, caption=caption if caption else None)
        sent_source_ids.append(msg.id)
        logger.info("Reposted single video message %s for pair %s", msg.id, pair_id)
    except Exception:
        logger.exception("Failed to repost single video message %s for pair %s", msg.id, pair_id)

    return sent_source_ids


async def repost_album(user_id: int, pair: Dict[str, Any], album_messages: List[Any]) -> List[int]:
    if not album_messages:
        return []

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_entity = runtime["source_entity"]
    dest_entity = runtime["dest_entity"]

    album_messages = sorted(album_messages, key=lambda x: x.id)
    grouped_id = getattr(album_messages[0], "grouped_id", None)

    if any(is_forwarded(m) for m in album_messages):
        logger.info("Album grouped_id=%s forwarded. Skipping pair %s.", grouped_id, pair_id)
        return []

    album_ids = [m.id for m in album_messages]
    if any(is_duplicate(pair, mid) for mid in album_ids):
        logger.info("Album grouped_id=%s duplicate found. Skipping pair %s.", grouped_id, pair_id)
        return []

    sent_source_ids: List[int] = []

    first_album_id = album_messages[0].id
    previous_msg = None
    try:
        previous_msg = await safe_get_message(source_entity, first_album_id - 1)
    except Exception:
        logger.exception("Failed to fetch previous for album %s pair %s", grouped_id, pair_id)

    sent_source_ids.extend(await repost_previous_message_if_valid(user_id, pair, previous_msg))

    files = []
    captions = []

    for m in album_messages:
        if m.media:
            files.append(m.media)
            captions.append(get_clean_text_from_message(m))
        else:
            logger.warning("Album item %s has no media, pair %s", m.id, pair_id)

    if not files:
        return sent_source_ids

    try:
        await safe_send_album(dest_entity, files, captions)
        sent_source_ids.extend(album_ids)
        logger.info("Reposted album grouped_id=%s for pair %s", grouped_id, pair_id)
    except Exception:
        logger.exception("Failed to repost album grouped_id=%s for pair %s", grouped_id, pair_id)

    return sent_source_ids


# =========================================================
# CORE PROCESSING
# =========================================================
async def process_message_object(user_id: int, pair: Dict[str, Any], msg) -> None:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    if runtime["source_entity"] is None or runtime["dest_entity"] is None:
        return

    async with runtime["lock"]:
        try:
            if getattr(msg, "grouped_id", None):
                update_last_processed(user_id, pair, msg.id)
                return

            if not is_video_message(msg):
                update_last_processed(user_id, pair, msg.id)
                return

            await apply_human_delay(user_id, pair_id)

            sent_ids = await repost_single_video_message(user_id, pair, msg)
            if sent_ids:
                mark_sent_ids(user_id, pair, sent_ids)
                mark_action_done(user_id, pair_id)

            update_last_processed(user_id, pair, msg.id)
        except Exception:
            logger.exception("Unexpected error while processing message %s pair %s", getattr(msg, "id", "unknown"), pair_id)
            update_last_processed(user_id, pair, getattr(msg, "id", 0))


async def process_album_object(user_id: int, pair: Dict[str, Any], album_messages: List[Any]) -> None:
    if not album_messages:
        return

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    album_messages = sorted(album_messages, key=lambda x: x.id)
    highest_id = album_messages[-1].id

    async with runtime["lock"]:
        try:
            has_video = any(is_video_message(m) for m in album_messages)
            if not has_video:
                update_last_processed(user_id, pair, highest_id)
                return

            grouped_id = getattr(album_messages[0], "grouped_id", None)
            if grouped_id is not None:
                seen_groups = runtime["processed_grouped_ids_live"]
                if grouped_id in seen_groups:
                    update_last_processed(user_id, pair, highest_id)
                    return
                seen_groups.add(grouped_id)
                if len(seen_groups) > 1000:
                    runtime["processed_grouped_ids_live"] = set(list(seen_groups)[-500:])

            await apply_human_delay(user_id, pair_id)

            sent_ids = await repost_album(user_id, pair, album_messages)
            if sent_ids:
                mark_sent_ids(user_id, pair, sent_ids)
                mark_action_done(user_id, pair_id)

            update_last_processed(user_id, pair, highest_id)
        except Exception:
            logger.exception("Unexpected error while processing album grouped_id=%s pair %s", getattr(album_messages[0], "grouped_id", None), pair_id)
            update_last_processed(user_id, pair, highest_id)


# =========================================================
# STARTUP / CHECK SCAN
# =========================================================
async def fetch_latest_message_id(source_entity) -> int:
    try:
        latest = await run_with_floodwait(client.get_messages, source_entity, limit=1)
        if latest:
            return latest[0].id
    except Exception:
        logger.exception("Failed to fetch latest message id.")
    return 0


async def scan_pair(user_id: int, pair: Dict[str, Any]) -> None:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_entity = runtime["source_entity"]

    if not source_entity:
        return

    last_processed_id = int(pair.get("last_processed_id", 0))
    logger.info("Scan begin for pair %s user %s | last_processed_id=%s", pair_id, user_id, last_processed_id)

    try:
        current_latest_id = await fetch_latest_message_id(source_entity)
        logger.info("Current latest source message id for pair %s: %s", pair_id, current_latest_id)

        if current_latest_id > last_processed_id:
            grouped_map: Dict[int, List[Any]] = {}

            async for msg in client.iter_messages(source_entity, min_id=last_processed_id, reverse=True):
                grouped_id = getattr(msg, "grouped_id", None)
                if grouped_id:
                    grouped_map.setdefault(grouped_id, []).append(msg)
                else:
                    await process_message_object(user_id, pair, msg)

            for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
                await process_album_object(user_id, pair, album_msgs)
    except Exception:
        logger.exception("Failed during missed messages scan for pair %s", pair_id)

    try:
        latest_msgs = await run_with_floodwait(client.get_messages, source_entity, limit=LATEST_RECHECK_LIMIT)
        latest_msgs = sorted(latest_msgs, key=lambda x: x.id)

        grouped_map_10: Dict[int, List[Any]] = {}
        for msg in latest_msgs:
            grouped_id = getattr(msg, "grouped_id", None)
            if grouped_id:
                grouped_map_10.setdefault(grouped_id, []).append(msg)
            else:
                await process_message_object(user_id, pair, msg)

        for _, album_msgs in sorted(grouped_map_10.items(), key=lambda item: min(m.id for m in item[1])):
            await process_album_object(user_id, pair, album_msgs)

        logger.info("Latest %s recheck completed for pair %s", LATEST_RECHECK_LIMIT, pair_id)
    except Exception:
        logger.exception("Failed latest recheck for pair %s", pair_id)


async def scan_all_pairs(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        await resolve_pair_entities(user_id, pair)
        await scan_pair(user_id, pair)


# =========================================================
# COMMAND REPLIES
# =========================================================
async def send_self_reply(chat_id: int, text: str) -> None:
    try:
        await safe_send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send self reply to chat %s", chat_id)


def format_pairs_list(user_id: int) -> str:
    record = ensure_user_record(user_id)
    pairs = record.get("pairs", [])
    if not pairs:
        return "No pairs configured.\nUse /addpair"

    lines = ["Configured pairs:"]
    for pair in pairs:
        lines.append(
            f"\nID: {pair['pair_id']}"
            f"\nName: {pair.get('name', 'pair' + str(pair['pair_id']))}"
            f"\nA: {pair.get('channel_a', '')}"
            f"\nB: {pair.get('channel_b', '')}"
            f"\nlast_processed_id: {pair.get('last_processed_id', 0)}"
            f"\nrecent_sent_ids_count: {len(pair.get('recent_sent_ids', []))}"
        )
    lines.append("\nCommands:")
    lines.append("/addpair")
    lines.append("/editA <id>")
    lines.append("/editB <id>")
    lines.append("/deletepair <id>")
    lines.append("/check")
    lines.append("/check <id>")
    lines.append("/status")
    return "\n".join(lines)


def format_status(user_id: int) -> str:
    record = ensure_user_record(user_id)
    pairs = record.get("pairs", [])
    if not pairs:
        return "No pairs configured."

    lines = ["Status:"]
    for pair in pairs:
        lines.append(
            f"\nPair {pair['pair_id']} ({pair.get('name', '')})"
            f"\nA: {pair.get('channel_a', '')}"
            f"\nB: {pair.get('channel_b', '')}"
            f"\nlast_processed_id: {pair.get('last_processed_id', 0)}"
            f"\nrecent_sent_ids_count: {len(pair.get('recent_sent_ids', []))}"
        )
    return "\n".join(lines)


# =========================================================
# COMMAND HANDLERS
# =========================================================
@client.on(events.NewMessage(outgoing=True, pattern=r"^/start$"))
async def start_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await send_self_reply(
        event.chat_id,
        "Multiple-pair mode is enabled.\n\n"
        "Use:\n"
        "/addpair - add new source/destination pair\n"
        "/listpairs - show all pairs\n"
        "/check - check all pairs\n"
        "/status - show status"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/addpair$"))
async def addpair_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    pending_input_state[event.chat_id] = {"mode": "await_new_pair_a", "user_id": owner_id}
    await send_self_reply(event.chat_id, "Send the link for Channel A of the new pair.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/listpairs$"))
async def listpairs_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await send_self_reply(event.chat_id, format_pairs_list(owner_id))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/status$"))
async def status_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await send_self_reply(event.chat_id, format_status(owner_id))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check$"))
async def check_all_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await send_self_reply(event.chat_id, "Checking all pairs...")
    await scan_all_pairs(owner_id)
    await send_self_reply(event.chat_id, "Check completed.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check (\d+)$"))
async def check_pair_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(owner_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    await send_self_reply(event.chat_id, f"Checking pair {pair_id}...")
    resolved = await resolve_pair_entities(owner_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Pair {pair_id} channels not valid.")
        return

    await scan_pair(owner_id, pair)
    await send_self_reply(event.chat_id, f"Check completed for pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/deletepair (\d+)$"))
async def deletepair_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    pair_id = int(event.pattern_match.group(1))
    record = ensure_user_record(owner_id)

    pair = get_pair_by_id(owner_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    record["pairs"] = [p for p in record["pairs"] if int(p["pair_id"]) != pair_id]
    save_user_record(owner_id)

    user_key = str(owner_id)
    if user_key in runtime_cache and str(pair_id) in runtime_cache[user_key]:
        del runtime_cache[user_key][str(pair_id)]

    await send_self_reply(event.chat_id, f"Pair {pair_id} deleted.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editA (\d+)$"))
async def edit_a_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(owner_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    pending_input_state[event.chat_id] = {"mode": "edit_pair_a", "user_id": owner_id, "pair_id": pair_id}
    await send_self_reply(event.chat_id, f"Send the new link for Channel A of pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editB (\d+)$"))
async def edit_b_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(owner_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    pending_input_state[event.chat_id] = {"mode": "edit_pair_b", "user_id": owner_id, "pair_id": pair_id}
    await send_self_reply(event.chat_id, f"Send the new link for Channel B of pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True))
async def onboarding_input_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    text = (event.raw_text or "").strip()
    if text.startswith("/"):
        return

    state = pending_input_state.get(event.chat_id)
    if not state:
        return

    user_id = state["user_id"]
    record = ensure_user_record(user_id)
    mode = state["mode"]

    try:
        if mode == "await_new_pair_a":
            await safe_get_entity(text)
            pending_input_state[event.chat_id] = {
                "mode": "await_new_pair_b",
                "user_id": user_id,
                "channel_a": text
            }
            await send_self_reply(event.chat_id, "Send the link for Channel B of the new pair.")

        elif mode == "await_new_pair_b":
            await safe_get_entity(text)
            channel_a = state["channel_a"]
            channel_b = text

            pair_id = next_pair_id(record)
            new_pair = {
                "pair_id": pair_id,
                "name": f"pair{pair_id}",
                "channel_a": channel_a,
                "channel_b": channel_b,
                "last_processed_id": 0,
                "recent_sent_ids": []
            }
            record["pairs"].append(new_pair)
            save_user_record(user_id)

            pending_input_state.pop(event.chat_id, None)

            resolved = await resolve_pair_entities(user_id, new_pair)

            await send_self_reply(
                event.chat_id,
                f"Pair {pair_id} added successfully.\n\n"
                f"A: {channel_a}\n"
                f"B: {channel_b}\n\n"
                "Use /listpairs to view all pairs."
            )

            if resolved:
                await scan_pair(user_id, new_pair)

        elif mode == "edit_pair_a":
            pair_id = int(state["pair_id"])
            pair = get_pair_by_id(user_id, pair_id)
            if not pair:
                pending_input_state.pop(event.chat_id, None)
                await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
                return

            await safe_get_entity(text)
            pair["channel_a"] = text
            pair["last_processed_id"] = 0
            pair["recent_sent_ids"] = []
            save_user_record(user_id)
            pending_input_state.pop(event.chat_id, None)

            resolved = await resolve_pair_entities(user_id, pair)

            await send_self_reply(
                event.chat_id,
                f"Pair {pair_id} Channel A updated.\n\n"
                f"A: {pair['channel_a']}\n"
                f"B: {pair.get('channel_b', '')}"
            )
            if resolved:
                await scan_pair(user_id, pair)

        elif mode == "edit_pair_b":
            pair_id = int(state["pair_id"])
            pair = get_pair_by_id(user_id, pair_id)
            if not pair:
                pending_input_state.pop(event.chat_id, None)
                await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
                return

            await safe_get_entity(text)
            pair["channel_b"] = text
            save_user_record(user_id)
            pending_input_state.pop(event.chat_id, None)

            resolved = await resolve_pair_entities(user_id, pair)

            await send_self_reply(
                event.chat_id,
                f"Pair {pair_id} Channel B updated.\n\n"
                f"A: {pair.get('channel_a', '')}\n"
                f"B: {pair['channel_b']}"
            )
            if resolved:
                await scan_pair(user_id, pair)

    except Exception:
        logger.exception("Invalid or inaccessible channel link provided: %s", text)
        await send_self_reply(event.chat_id, "Invalid or inaccessible channel link. Please send a valid public channel link.")


# =========================================================
# LIVE EVENTS
# =========================================================
@client.on(events.NewMessage())
async def live_new_message_handler(event):
    global owner_id
    if owner_id is None:
        return

    if event.out:
        return

    msg = event.message

    record = ensure_user_record(owner_id)
    for pair in record["pairs"]:
        pair_id = int(pair["pair_id"])
        runtime = get_pair_runtime(owner_id, pair_id)
        source_chat_id = runtime.get("source_chat_id")

        if not source_chat_id:
            continue

        event_channel_id = None
        try:
            if getattr(msg, "peer_id", None) and hasattr(msg.peer_id, "channel_id"):
                event_channel_id = msg.peer_id.channel_id
        except Exception:
            pass

        logger.info(
            "LIVE MSG DETECTED | pair_id=%s | event.chat_id=%s | peer.channel_id=%s | source_chat_id=%s | msg_id=%s",
            pair_id, getattr(event, "chat_id", None), event_channel_id, source_chat_id, getattr(msg, "id", None),
        )

        if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
            continue

        logger.info(
            "SOURCE MATCHED | pair_id=%s | msg_id=%s | grouped_id=%s | has_media=%s | is_video=%s",
            pair_id, msg.id, getattr(msg, "grouped_id", None), bool(getattr(msg, "media", None)), is_video_message(msg),
        )

        if getattr(msg, "grouped_id", None):
            return

        await process_message_object(owner_id, pair, msg)
        return


@client.on(events.Album())
async def live_album_handler(event):
    global owner_id
    if owner_id is None:
        return

    album_messages = list(event.messages or [])
    if not album_messages:
        return

    first_msg = album_messages[0]

    record = ensure_user_record(owner_id)
    for pair in record["pairs"]:
        pair_id = int(pair["pair_id"])
        runtime = get_pair_runtime(owner_id, pair_id)
        source_chat_id = runtime.get("source_chat_id")

        if not source_chat_id:
            continue

        event_channel_id = None
        try:
            if getattr(first_msg, "peer_id", None) and hasattr(first_msg.peer_id, "channel_id"):
                event_channel_id = first_msg.peer_id.channel_id
        except Exception:
            pass

        logger.info(
            "ALBUM DETECTED | pair_id=%s | event.chat_id=%s | peer.channel_id=%s | source_chat_id=%s | ids=%s",
            pair_id, getattr(event, "chat_id", None), event_channel_id, source_chat_id, [m.id for m in album_messages],
        )

        if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
            continue

        await process_album_object(owner_id, pair, album_messages)
        return


# =========================================================
# POLLING LOOP
# =========================================================
async def poll_new_updates_loop():
    global owner_id

    while True:
        try:
            if owner_id is None:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            record = ensure_user_record(owner_id)

            for pair in record["pairs"]:
                pair_id = int(pair["pair_id"])
                runtime = get_pair_runtime(owner_id, pair_id)
                source_entity = runtime.get("source_entity")
                dest_entity = runtime.get("dest_entity")

                if not source_entity or not dest_entity:
                    resolved = await resolve_pair_entities(owner_id, pair)
                    if not resolved:
                        continue

                last_processed_id = int(pair.get("last_processed_id", 0))
                grouped_map = {}

                async for msg in client.iter_messages(get_pair_runtime(owner_id, pair_id)["source_entity"], min_id=last_processed_id, reverse=True):
                    grouped_id = getattr(msg, "grouped_id", None)
                    if grouped_id:
                        grouped_map.setdefault(grouped_id, []).append(msg)
                    else:
                        await process_message_object(owner_id, pair, msg)

                for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
                    await process_album_object(owner_id, pair, album_msgs)

        except Exception:
            logger.exception("Polling loop error")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# =========================================================
# LOGIN / INIT
# =========================================================
async def login_flow() -> None:
    if SESSION_STRING:
        logger.info("Using SESSION_STRING authentication.")
        return

    if await client.is_user_authorized():
        logger.info("Using existing authorized session file.")
        return

    logger.info("No valid session found. Starting login flow.")
    phone = input("Enter your phone number (with country code, e.g. +959...): ").strip()

    await client.send_code_request(phone)
    code = input("Enter the login code you received: ").strip()

    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        logger.info("2FA password required.")
        while True:
            password = input("Enter your 2FA password: ").strip()
            try:
                await client.sign_in(password=password)
                break
            except PasswordHashInvalidError:
                print("Invalid 2FA password. Try again.")
                logger.warning("Invalid 2FA password entered.")

    logger.info("Login successful.")


async def initialize_runtime() -> None:
    global owner_id, user_data

    init_db()
    user_data = load_all_user_data()

    me = await client.get_me()
    owner_id = me.id
    logger.info("Logged in as user_id=%s", owner_id)

    ensure_user_record(owner_id)
    await resolve_all_pairs(owner_id)

    record = ensure_user_record(owner_id)
    if record["pairs"]:
        for pair in record["pairs"]:
            await scan_pair(owner_id, pair)
    else:
        logger.info("No pairs configured yet. Use /addpair.")


# =========================================================
# MAIN
# =========================================================
async def main():
    logger.info("Bot startup initiated.")

    await client.connect()
    keep_alive()
    await login_flow()
    await initialize_runtime()

    asyncio.create_task(poll_new_updates_loop())

    logger.info("User-bot is now running.")
    print("User-bot is running. Use /addpair in your Saved Messages to configure pairs.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception:
        logger.exception("Fatal error in main.")
