import asyncio
import json
import logging
import os
import random
import re
import time
from typing import Any, Dict, List, Optional

import psycopg
from dotenv import load_dotenv
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

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
DATABASE_URL = os.getenv("DATABASE_URL")
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
LOG_FILE = os.getenv("LOG_FILE", "bot_log.txt")

MIN_DELAY_SECONDS = 5
NORMAL_DELAY_MIN = 30
NORMAL_DELAY_MAX = 60
RECENT_IDS_LIMIT = 100
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
LATEST_RECHECK_LIMIT = int(os.getenv("LATEST_RECHECK_LIMIT", "10"))

URL_REGEX = re.compile(r'(?i)\b(?:https?://|www\.|t\.me/)[^\s<>"\'\]\)]+' )

if not API_ID or not API_HASH:
    raise ValueError("API_ID or API_HASH missing. Put them in .env or Render environment variables.")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL missing. Put your Neon Postgres connection string in .env or Render environment variables.")

API_ID = int(API_ID)


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
# GLOBALS
# =========================================================
if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

db_conn: Optional[psycopg.Connection] = None
user_data: Dict[str, Dict[str, Any]] = {}
runtime_cache: Dict[str, Dict[str, Any]] = {}
pending_input_state: Dict[int, Dict[str, Any]] = {}
owner_id: Optional[int] = None


# =========================================================
# DATABASE HELPERS
# =========================================================
def get_default_user_record() -> Dict[str, Any]:
    return {
        "channel_a": "",
        "channel_b": "",
        "last_processed_id": 0,
        "recent_sent_ids": [],
    }


async def init_db() -> None:
    global db_conn

    if db_conn is None or db_conn.closed:
        db_conn = psycopg.connect(DATABASE_URL, autocommit=True)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_configs (
                user_id BIGINT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    logger.info("Database initialized successfully.")


async def load_all_user_data() -> Dict[str, Dict[str, Any]]:
    if db_conn is None:
        await init_db()

    data: Dict[str, Dict[str, Any]] = {}
    with db_conn.cursor() as cur:
        cur.execute("SELECT user_id, data FROM user_configs")
        rows = cur.fetchall()

    for row in rows:
        user_id, raw = row
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        if not isinstance(raw, dict):
            raw = {}
        merged = get_default_user_record()
        merged.update(raw)
        merged["recent_sent_ids"] = trim_recent_ids(list(merged.get("recent_sent_ids", [])))
        merged["last_processed_id"] = int(merged.get("last_processed_id", 0))
        data[str(user_id)] = merged
    return data


async def save_user_record(user_id: int) -> None:
    if db_conn is None:
        await init_db()

    key = str(user_id)
    record = user_data.get(key, get_default_user_record())
    record["recent_sent_ids"] = trim_recent_ids(list(record.get("recent_sent_ids", [])))
    payload = json.dumps(record, ensure_ascii=False)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO user_configs (user_id, data, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
            """,
            (user_id, payload),
        )


async def ensure_user_record(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    if key not in user_data:
        user_data[key] = get_default_user_record()
        await save_user_record(user_id)
    return user_data[key]


# =========================================================
# GENERIC HELPERS
# =========================================================
def trim_recent_ids(ids_list: List[int], limit: int = RECENT_IDS_LIMIT) -> List[int]:
    return ids_list[-limit:]


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


def get_clean_text_from_message(msg) -> str:
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    return clean_urls(raw)


# =========================================================
# TELEGRAM SAFE EXECUTION HELPERS
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


async def safe_get_entity(link_or_username: str):
    return await run_with_floodwait(client.get_entity, link_or_username)


async def safe_get_message(entity, msg_id: int):
    return await run_with_floodwait(client.get_messages, entity, ids=msg_id)


# =========================================================
# RUNTIME CACHE HELPERS
# =========================================================
def get_runtime(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    if key not in runtime_cache:
        runtime_cache[key] = {
            "source_entity": None,
            "dest_entity": None,
            "source_chat_id": None,
            "dest_chat_id": None,
            "lock": asyncio.Lock(),
            "last_action_time": 0.0,
            "processed_grouped_ids_live": set(),
        }
    return runtime_cache[key]


async def resolve_user_channels(user_id: int) -> bool:
    record = await ensure_user_record(user_id)
    channel_a = record.get("channel_a", "").strip()
    channel_b = record.get("channel_b", "").strip()

    if not channel_a or not channel_b:
        logger.info("User %s has incomplete channel setup.", user_id)
        return False

    runtime = get_runtime(user_id)

    try:
        source_entity = await safe_get_entity(channel_a)
        dest_entity = await safe_get_entity(channel_b)

        runtime["source_entity"] = source_entity
        runtime["dest_entity"] = dest_entity
        runtime["source_chat_id"] = getattr(source_entity, "id", None)
        runtime["dest_chat_id"] = getattr(dest_entity, "id", None)

        logger.info("Resolved channels for user %s | A=%s | B=%s", user_id, channel_a, channel_b)
        return True
    except Exception as e:
        logger.exception("Failed to resolve channels for user %s: %s", user_id, e)
        return False


async def is_duplicate(user_id: int, source_msg_id: int) -> bool:
    record = await ensure_user_record(user_id)
    return source_msg_id in record.get("recent_sent_ids", [])


async def mark_sent_ids(user_id: int, source_ids: List[int]) -> None:
    record = await ensure_user_record(user_id)
    recent_ids = record.get("recent_sent_ids", [])
    for sid in source_ids:
        if sid not in recent_ids:
            recent_ids.append(sid)
    record["recent_sent_ids"] = trim_recent_ids(recent_ids)
    await save_user_record(user_id)


async def update_last_processed(user_id: int, msg_id: int) -> None:
    record = await ensure_user_record(user_id)
    if msg_id > int(record.get("last_processed_id", 0)):
        record["last_processed_id"] = int(msg_id)
        await save_user_record(user_id)


async def apply_human_delay(user_id: int) -> None:
    runtime = get_runtime(user_id)
    last_action_time = runtime.get("last_action_time", 0.0)

    if last_action_time <= 0:
        return

    target_delay = max(MIN_DELAY_SECONDS, random.randint(NORMAL_DELAY_MIN, NORMAL_DELAY_MAX))
    elapsed = time.time() - last_action_time
    if elapsed < target_delay:
        wait_for = target_delay - elapsed
        logger.info("Human-like delay for user %s: sleeping %.2f seconds", user_id, wait_for)
        await asyncio.sleep(wait_for)


def mark_action_done(user_id: int) -> None:
    runtime = get_runtime(user_id)
    runtime["last_action_time"] = time.time()


# =========================================================
# SEND LOGIC
# =========================================================
async def repost_previous_message_if_valid(user_id: int, previous_msg) -> List[int]:
    if not previous_msg:
        return []

    if is_forwarded(previous_msg):
        logger.info("Previous message %s is forwarded. Skipping previous.", previous_msg.id)
        return []

    if await is_duplicate(user_id, previous_msg.id):
        logger.info("Previous message %s already sent. Skipping duplicate.", previous_msg.id)
        return []

    dest_entity = get_runtime(user_id)["dest_entity"]

    try:
        if previous_msg.media:
            caption = get_clean_text_from_message(previous_msg)
            await safe_send_file(dest_entity, previous_msg.media, caption=caption if caption else None)
        else:
            text = get_clean_text_from_message(previous_msg)
            if text:
                await safe_send_message(dest_entity, text)
            else:
                logger.info("Previous message %s became empty after URL cleaning. Skipped.", previous_msg.id)
                return []
        logger.info("Reposted previous message %s", previous_msg.id)
        return [previous_msg.id]
    except Exception:
        logger.exception("Failed to repost previous message %s", previous_msg.id)
        return []


async def repost_single_video_message(user_id: int, msg) -> List[int]:
    if is_forwarded(msg):
        logger.info("Video message %s is forwarded. Skipping.", msg.id)
        return []

    if await is_duplicate(user_id, msg.id):
        logger.info("Video message %s already sent. Skipping duplicate.", msg.id)
        return []

    runtime = get_runtime(user_id)
    source_entity = runtime["source_entity"]
    dest_entity = runtime["dest_entity"]

    logger.info(
        "TRY SEND VIDEO | msg_id=%s | forwarded=%s | duplicate=%s",
        msg.id,
        is_forwarded(msg),
        await is_duplicate(user_id, msg.id),
    )

    sent_source_ids: List[int] = []

    previous_msg = None
    try:
        previous_msg = await safe_get_message(source_entity, msg.id - 1)
    except Exception:
        logger.exception("Failed to fetch previous message for video %s", msg.id)

    sent_source_ids.extend(await repost_previous_message_if_valid(user_id, previous_msg))

    try:
        caption = get_clean_text_from_message(msg)
        await safe_send_file(dest_entity, msg.media, caption=caption if caption else None)
        sent_source_ids.append(msg.id)
        logger.info("Reposted single video message %s", msg.id)
    except Exception:
        logger.exception("Failed to repost single video message %s", msg.id)

    return sent_source_ids


async def repost_album(user_id: int, album_messages: List[Any]) -> List[int]:
    if not album_messages:
        return []

    album_messages = sorted(album_messages, key=lambda x: x.id)
    grouped_id = getattr(album_messages[0], "grouped_id", None)

    if any(is_forwarded(m) for m in album_messages):
        logger.info("Album grouped_id=%s is forwarded. Skipping album.", grouped_id)
        return []

    album_ids = [m.id for m in album_messages]
    duplicate_found = any([await is_duplicate(user_id, mid) for mid in album_ids])
    if duplicate_found:
        logger.info("Album grouped_id=%s has duplicate source IDs. Skipping album.", grouped_id)
        return []

    runtime = get_runtime(user_id)
    source_entity = runtime["source_entity"]
    dest_entity = runtime["dest_entity"]

    sent_source_ids: List[int] = []

    first_album_id = album_messages[0].id
    previous_msg = None
    try:
        previous_msg = await safe_get_message(source_entity, first_album_id - 1)
    except Exception:
        logger.exception("Failed to fetch previous message for album %s", grouped_id)

    sent_source_ids.extend(await repost_previous_message_if_valid(user_id, previous_msg))

    files = []
    captions = []
    for m in album_messages:
        if m.media:
            files.append(m.media)
            captions.append(get_clean_text_from_message(m))
        else:
            logger.warning("Album item %s has no media. Ignored.", m.id)

    if not files:
        logger.warning("Album grouped_id=%s had no sendable media.", grouped_id)
        return sent_source_ids

    try:
        await safe_send_album(dest_entity, files, captions)
        sent_source_ids.extend(album_ids)
        logger.info("Reposted album grouped_id=%s | msg_ids=%s", grouped_id, album_ids)
    except Exception:
        logger.exception("Failed to repost album grouped_id=%s", grouped_id)

    return sent_source_ids


# =========================================================
# CORE PROCESSING
# =========================================================
async def process_message_object(user_id: int, msg) -> None:
    runtime = get_runtime(user_id)

    if runtime["source_entity"] is None or runtime["dest_entity"] is None:
        logger.warning("Source/Destination entities are not resolved for user %s", user_id)
        return

    async with runtime["lock"]:
        try:
            if getattr(msg, "grouped_id", None):
                await update_last_processed(user_id, msg.id)
                return

            if not is_video_message(msg):
                await update_last_processed(user_id, msg.id)
                return

            await apply_human_delay(user_id)

            sent_ids = await repost_single_video_message(user_id, msg)
            if sent_ids:
                await mark_sent_ids(user_id, sent_ids)
                mark_action_done(user_id)

            await update_last_processed(user_id, msg.id)
        except Exception:
            logger.exception("Unexpected error while processing message %s", getattr(msg, "id", "unknown"))
            await update_last_processed(user_id, getattr(msg, "id", 0))


async def process_album_object(user_id: int, album_messages: List[Any]) -> None:
    if not album_messages:
        return

    album_messages = sorted(album_messages, key=lambda x: x.id)
    highest_id = album_messages[-1].id
    runtime = get_runtime(user_id)

    async with runtime["lock"]:
        try:
            has_video = any(is_video_message(m) for m in album_messages)
            if not has_video:
                await update_last_processed(user_id, highest_id)
                return

            grouped_id = getattr(album_messages[0], "grouped_id", None)
            if grouped_id is not None:
                seen_groups = runtime["processed_grouped_ids_live"]
                if grouped_id in seen_groups:
                    await update_last_processed(user_id, highest_id)
                    return
                seen_groups.add(grouped_id)

            await apply_human_delay(user_id)

            sent_ids = await repost_album(user_id, album_messages)
            if sent_ids:
                await mark_sent_ids(user_id, sent_ids)
                mark_action_done(user_id)

            await update_last_processed(user_id, highest_id)
        except Exception:
            logger.exception("Unexpected error while processing album grouped_id=%s", getattr(album_messages[0], "grouped_id", None))
            await update_last_processed(user_id, highest_id)


# =========================================================
# STARTUP SCAN LOGIC
# =========================================================
async def fetch_latest_message_id(source_entity) -> int:
    try:
        latest = await run_with_floodwait(client.get_messages, source_entity, limit=1)
        if latest:
            return latest[0].id
    except Exception:
        logger.exception("Failed to fetch latest message id.")
    return 0


async def startup_scan_for_user(user_id: int) -> None:
    record = await ensure_user_record(user_id)
    runtime = get_runtime(user_id)
    source_entity = runtime["source_entity"]

    if not source_entity:
        logger.warning("startup_scan_for_user skipped: source_entity missing for user %s", user_id)
        return

    last_processed_id = int(record.get("last_processed_id", 0))
    logger.info("Startup scan begin for user %s | last_processed_id=%s", user_id, last_processed_id)

    try:
        current_latest_id = await fetch_latest_message_id(source_entity)
        logger.info("Current latest source message id for user %s: %s", user_id, current_latest_id)

        if current_latest_id > last_processed_id:
            grouped_map: Dict[int, List[Any]] = {}
            async for msg in client.iter_messages(source_entity, min_id=last_processed_id, reverse=True):
                grouped_id = getattr(msg, "grouped_id", None)
                if grouped_id:
                    grouped_map.setdefault(grouped_id, []).append(msg)
                else:
                    await process_message_object(user_id, msg)

            for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
                await process_album_object(user_id, album_msgs)
    except Exception:
        logger.exception("Failed during missed messages startup scan for user %s", user_id)

    try:
        latest_msgs = await run_with_floodwait(client.get_messages, source_entity, limit=LATEST_RECHECK_LIMIT)
        latest_msgs = sorted(latest_msgs, key=lambda x: x.id)

        grouped_map_10: Dict[int, List[Any]] = {}
        for msg in latest_msgs:
            grouped_id = getattr(msg, "grouped_id", None)
            if grouped_id:
                grouped_map_10.setdefault(grouped_id, []).append(msg)
            else:
                await process_message_object(user_id, msg)

        for _, album_msgs in sorted(grouped_map_10.items(), key=lambda item: min(m.id for m in item[1])):
            await process_album_object(user_id, album_msgs)

        logger.info("Latest %s startup recheck completed for user %s", LATEST_RECHECK_LIMIT, user_id)
    except Exception:
        logger.exception("Failed latest startup recheck for user %s", user_id)


# =========================================================
# COMMAND FLOW
# =========================================================
async def send_self_reply(chat_id: int, text: str) -> None:
    try:
        await safe_send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send self reply to chat %s", chat_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check$"))
async def manual_check_handler(event):
    global owner_id

    if owner_id is None or event.sender_id != owner_id:
        return

    user_id = owner_id
    await send_self_reply(event.chat_id, "Checking Channel A for missed updates...")

    try:
        resolved = await resolve_user_channels(user_id)
        if not resolved:
            await send_self_reply(event.chat_id, "Channels are not properly configured.")
            return

        await startup_scan_for_user(user_id)
        await send_self_reply(event.chat_id, "Check completed.")
        logger.info("Manual /check executed by user %s", user_id)
    except Exception as e:
        logger.exception("Error during /check command")
        await send_self_reply(event.chat_id, f"Error during check: {e}")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/status$"))
async def status_handler(event):
    global owner_id

    if owner_id is None or event.sender_id != owner_id:
        return

    record = await ensure_user_record(owner_id)
    text = (
        "Status\n\n"
        f"Channel A: {record.get('channel_a', '')}\n"
        f"Channel B: {record.get('channel_b', '')}\n"
        f"last_processed_id: {record.get('last_processed_id', 0)}\n"
        f"recent_sent_ids_count: {len(record.get('recent_sent_ids', []))}"
    )
    await send_self_reply(event.chat_id, text)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/start$"))
async def start_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await ensure_user_record(owner_id)
    pending_input_state[event.chat_id] = {"mode": "await_channel_a", "user_id": owner_id}
    await send_self_reply(event.chat_id, "Please send the link for Channel A.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editA$"))
async def edit_a_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await ensure_user_record(owner_id)
    pending_input_state[event.chat_id] = {"mode": "edit_channel_a", "user_id": owner_id}
    await send_self_reply(event.chat_id, "Please send the new link for Channel A.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editB$"))
async def edit_b_command_handler(event):
    global owner_id
    if owner_id is None or event.sender_id != owner_id:
        return

    await ensure_user_record(owner_id)
    pending_input_state[event.chat_id] = {"mode": "edit_channel_b", "user_id": owner_id}
    await send_self_reply(event.chat_id, "Please send the new link for Channel B.")


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
    record = await ensure_user_record(user_id)
    mode = state["mode"]

    try:
        if mode in ("await_channel_a", "edit_channel_a"):
            await safe_get_entity(text)
            record["channel_a"] = text
            record["last_processed_id"] = 0
            record["recent_sent_ids"] = []
            await save_user_record(user_id)

            if mode == "await_channel_a":
                pending_input_state[event.chat_id] = {"mode": "await_channel_b", "user_id": user_id}
                await send_self_reply(event.chat_id, "Please send the link for Channel B.")
            else:
                pending_input_state.pop(event.chat_id, None)
                resolved = await resolve_user_channels(user_id)
                await send_self_reply(
                    event.chat_id,
                    "Channel A updated successfully.\n\n"
                    f"Channel A: {record['channel_a']}\n"
                    f"Channel B: {record.get('channel_b', '')}\n\n"
                    "If you want to edit the links, use /editA or /editB."
                )
                logger.info("Channel A updated by user %s", user_id)
                if resolved:
                    await startup_scan_for_user(user_id)

        elif mode in ("await_channel_b", "edit_channel_b"):
            await safe_get_entity(text)
            record["channel_b"] = text
            await save_user_record(user_id)

            pending_input_state.pop(event.chat_id, None)
            resolved = await resolve_user_channels(user_id)

            reply_text = (
                "Configuration saved successfully.\n\n"
                f"Channel A: {record['channel_a']}\n"
                f"Channel B: {record['channel_b']}\n\n"
                "If you want to edit the links, use /editA or /editB."
            )
            await send_self_reply(event.chat_id, reply_text)
            logger.info("Channel B updated by user %s", user_id)

            if resolved:
                await startup_scan_for_user(user_id)

    except Exception:
        logger.exception("Invalid or inaccessible channel link provided: %s", text)
        await send_self_reply(
            event.chat_id,
            "That channel link looks invalid or inaccessible.\nPlease send a valid public channel link or username."
        )


# =========================================================
# LIVE MONITORING EVENTS
# =========================================================
@client.on(events.NewMessage())
async def live_new_message_handler(event):
    global owner_id
    if owner_id is None:
        return

    if event.out:
        return

    runtime = get_runtime(owner_id)
    source_entity = runtime.get("source_entity")
    source_chat_id = runtime.get("source_chat_id")

    if not source_entity or not source_chat_id:
        return

    msg = event.message

    event_channel_id = None
    try:
        if getattr(msg, "peer_id", None) and hasattr(msg.peer_id, "channel_id"):
            event_channel_id = msg.peer_id.channel_id
    except Exception:
        pass

    logger.info(
        "LIVE MSG DETECTED | event.chat_id=%s | peer.channel_id=%s | source_chat_id=%s | msg_id=%s",
        getattr(event, "chat_id", None),
        event_channel_id,
        source_chat_id,
        getattr(msg, "id", None),
    )

    if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
        return

    logger.info(
        "SOURCE MATCHED | msg_id=%s | grouped_id=%s | has_media=%s | is_video=%s",
        msg.id,
        getattr(msg, "grouped_id", None),
        bool(getattr(msg, "media", None)),
        is_video_message(msg),
    )

    if getattr(msg, "grouped_id", None):
        return

    await process_message_object(owner_id, msg)


@client.on(events.Album())
async def live_album_handler(event):
    global owner_id
    if owner_id is None:
        return

    runtime = get_runtime(owner_id)
    source_chat_id = runtime.get("source_chat_id")

    if not source_chat_id:
        return

    album_messages = list(event.messages or [])
    if not album_messages:
        return

    first_msg = album_messages[0]
    event_channel_id = None
    try:
        if getattr(first_msg, "peer_id", None) and hasattr(first_msg.peer_id, "channel_id"):
            event_channel_id = first_msg.peer_id.channel_id
    except Exception:
        pass

    logger.info(
        "ALBUM DETECTED | event.chat_id=%s | peer.channel_id=%s | source_chat_id=%s | ids=%s",
        getattr(event, "chat_id", None),
        event_channel_id,
        source_chat_id,
        [m.id for m in album_messages],
    )

    if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
        return

    await process_album_object(owner_id, album_messages)


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

            record = await ensure_user_record(owner_id)
            runtime = get_runtime(owner_id)
            source_entity = runtime.get("source_entity")
            dest_entity = runtime.get("dest_entity")

            if not source_entity or not dest_entity:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            last_processed_id = int(record.get("last_processed_id", 0))
            grouped_map: Dict[int, List[Any]] = {}

            async for msg in client.iter_messages(source_entity, min_id=last_processed_id, reverse=True):
                grouped_id = getattr(msg, "grouped_id", None)
                if grouped_id:
                    grouped_map.setdefault(grouped_id, []).append(msg)
                else:
                    await process_message_object(owner_id, msg)

            for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
                await process_album_object(owner_id, album_msgs)

        except Exception:
            logger.exception("Polling loop error")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# =========================================================
# LOGIN / STARTUP
# =========================================================
async def login_flow() -> None:
    if SESSION_STRING:
        if await client.is_user_authorized():
            logger.info("Using SESSION_STRING authorization.")
            return
        raise RuntimeError("SESSION_STRING is set but authorization failed. Regenerate SESSION_STRING.")

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
    global user_data, owner_id

    await init_db()
    user_data = await load_all_user_data()

    me = await client.get_me()
    owner_id = me.id
    logger.info("Logged in as user_id=%s", owner_id)

    await ensure_user_record(owner_id)

    resolved = await resolve_user_channels(owner_id)
    if resolved:
        await startup_scan_for_user(owner_id)
    else:
        logger.info("Channels are not fully configured yet. Send /start to configure.")


async def main():
    logger.info("Bot startup initiated.")

    await client.connect()
    await login_flow()
    await initialize_runtime()

    asyncio.create_task(poll_new_updates_loop())

    logger.info("User-bot is now running.")
    print("User-bot is running. Send /start in your own Telegram chat to configure Channel A and Channel B.")

    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception:
        logger.exception("Fatal error in main.")
