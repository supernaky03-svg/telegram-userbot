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
from telethon.errors import FloodWaitError, PasswordHashInvalidError, SessionPasswordNeededError
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo

# =========================================================
# TOP-LEVEL CONFIG
# =========================================================
load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SESSION_NAME = os.getenv("SESSION_NAME", "userbot_session")
SESSION_STRING = (os.getenv("SESSION_STRING") or "").strip()
LOG_FILE = os.getenv("LOG_FILE", "bot_log.txt")
PORT = int(os.getenv("PORT", "10000"))

# Anti-ban defaults required by prompt
DELAY_MIN_SECONDS = int(os.getenv("DELAY_MIN_SECONDS", "20"))
DELAY_MAX_SECONDS = int(os.getenv("DELAY_MAX_SECONDS", "50"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "3600"))
INITIAL_SCAN_LIMIT = int(os.getenv("INITIAL_SCAN_LIMIT", "50"))
LATEST_RECHECK_LIMIT = int(os.getenv("LATEST_RECHECK_LIMIT", "10"))
RECENT_IDS_LIMIT = int(os.getenv("RECENT_IDS_LIMIT", "100"))

# Simple keyword list for post_rule=ON

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
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =========================================================
# OPTIONAL KEEP-ALIVE FOR RENDER
# =========================================================
app = Flask(__name__)


@app.route("/")
def home():
    return "Bot is alive"


@app.route("/healthz")
def healthz():
    return "ok"


def run_web() -> None:
    app.run(host="0.0.0.0", port=PORT)


def keep_alive() -> None:
    Thread(target=run_web, daemon=True).start()


# =========================================================
# GLOBALS
# =========================================================
if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

account_user_id: int = 0
user_data: Dict[str, Dict[str, Any]] = {}
runtime_cache: Dict[str, Dict[str, Any]] = {}


def clean_and_add_ads(text: str, ads_text: str = None) -> str:
    if not text:
        return ""
    
    # URL များကို ရှာဖွေဖျက်ဆီးမည့် Regex (t.me လင့်ခ်များပါဝင်သည်)
    url_pattern = r'https?://\S+|www\.\S+|t\.me/\S+'
    clean_text = re.sub(url_pattern, '', text)
    clean_text = clean_text.strip()
    
    # Ads link ရှိလျှင် အောက်ဆုံးတွင် ထပ်ပေါင်းမည်
    if ads_text and ads_text.strip():
        if clean_text:
            clean_text += f"\n\n{ads_text.strip()}"
        else:
            clean_text = ads_text.strip()
            
    return clean_text

# =========================================================
# DB HELPERS (NEON / POSTGRES)
# =========================================================
def get_db_connection():
    return psycopg.connect(DATABASE_URL, sslmode="require", row_factory=dict_row)


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # ads_link intentionally kept as the LAST column for pair rows
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pairs (
                    owner_user_id BIGINT NOT NULL,
                    pair_id INTEGER NOT NULL,
                    source_id TEXT NOT NULL DEFAULT '',
                    target_id TEXT NOT NULL DEFAULT '',
                    last_processed_id BIGINT NOT NULL DEFAULT 0,
                    recent_sent_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                    forward_rule BOOLEAN NOT NULL DEFAULT FALSE,
                    post_rule BOOLEAN NOT NULL DEFAULT TRUE,
                    ads_link TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (owner_user_id, pair_id)
                )
                """
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS forward_rule BOOLEAN NOT NULL DEFAULT FALSE"
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS post_rule BOOLEAN NOT NULL DEFAULT TRUE"
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS ads_link TEXT NOT NULL DEFAULT ''"
            )
        conn.commit()


def load_all_user_data() -> Dict[str, Dict[str, Any]]:
    data: Dict[str, Dict[str, Any]] = {}
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT owner_user_id, pair_id, source_id, target_id,
                       last_processed_id, recent_sent_ids,
                       forward_rule, post_rule, ads_link
                FROM pairs
                ORDER BY owner_user_id, pair_id
                """
            )
            rows = cur.fetchall()

    for row in rows:
        user_key = str(row["owner_user_id"])
        data.setdefault(user_key, {"pairs": []})
        recent_ids = row["recent_sent_ids"] or []
        if isinstance(recent_ids, str):
            try:
                recent_ids = json.loads(recent_ids)
            except Exception:
                recent_ids = []
        data[user_key]["pairs"].append(
            {
                "pair_id": int(row["pair_id"]),
                "source_id": row["source_id"],
                "target_id": row["target_id"],
                "last_processed_id": int(row["last_processed_id"] or 0),
                "recent_sent_ids": list(recent_ids)[-RECENT_IDS_LIMIT:],
                "forward_rule": bool(row["forward_rule"]),
                "post_rule": bool(row["post_rule"]),
                "ads_link": (row["ads_link"] or "").strip(),
            }
        )
    return data


def save_pair(owner_user_id: int, pair: Dict[str, Any]) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pairs (
                    owner_user_id, pair_id, source_id, target_id,
                    last_processed_id, recent_sent_ids,
                    forward_rule, post_rule, ads_link
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                ON CONFLICT (owner_user_id, pair_id)
                DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    target_id = EXCLUDED.target_id,
                    last_processed_id = EXCLUDED.last_processed_id,
                    recent_sent_ids = EXCLUDED.recent_sent_ids,
                    forward_rule = EXCLUDED.forward_rule,
                    post_rule = EXCLUDED.post_rule,
                    ads_link = EXCLUDED.ads_link
                """,
                (
                    owner_user_id,
                    int(pair["pair_id"]),
                    pair.get("source_id", ""),
                    pair.get("target_id", ""),
                    int(pair.get("last_processed_id", 0)),
                    json.dumps(pair.get("recent_sent_ids", [])[-RECENT_IDS_LIMIT:]),
                    bool(pair.get("forward_rule", False)),
                    bool(pair.get("post_rule", True)),
                    (pair.get("ads_link") or "").strip(),
                ),
            )
        conn.commit()


def delete_pair_from_db(owner_user_id: int, pair_id: int) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM pairs WHERE owner_user_id = %s AND pair_id = %s",
                (owner_user_id, int(pair_id)),
            )
        conn.commit()


def ensure_user_record(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    if key not in user_data:
        user_data[key] = {"pairs": []}
    if "pairs" not in user_data[key] or not isinstance(user_data[key]["pairs"], list):
        user_data[key]["pairs"] = []
    return user_data[key]


def next_pair_id(record: Dict[str, Any]) -> int:
    pairs = record.get("pairs", [])
    used_ids = {int(p.get("pair_id", 0)) for p in pairs if int(p.get("pair_id", 0)) > 0}

    new_id = 1
    while new_id in used_ids:
        new_id += 1
    return new_id


def get_pair_by_id(user_id: int, pair_id: int) -> Optional[Dict[str, Any]]:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        if int(pair.get("pair_id", 0)) == int(pair_id):
            return pair
    return None


def trim_recent_ids(ids_list: List[int], limit: int = RECENT_IDS_LIMIT) -> List[int]:
    return ids_list[-limit:]


# =========================================================
# TEXT / FILTER HELPERS
# =========================================================
def clean_urls(text: Optional[str]) -> str:
    if not text:
        return ""
    cleaned = URL_REGEX.sub("", text)
    cleaned = re.sub(r"\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\[\s*\]", "", cleaned)
    cleaned = re.sub(r"<\s*>", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def get_clean_text_from_message(msg) -> str:
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    return clean_urls(raw)


def message_text_for_filter(msg) -> str:
    parts = []
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    if raw:
        parts.append(raw)
    return "\n".join(parts).strip().lower()


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


def pair_matches_filters(pair: Dict[str, Any], msg) -> bool:
    # forward_rule ON => skip forwarded
    if pair.get("forward_rule", False) and is_forwarded(msg):
        return False

    # post_rule OFF => allow everything else
    if not pair.get("post_rule", True):
        return True

    # post_rule ON => only video messages pass directly
    return is_video_message(msg)


def pair_album_matches_filters(pair: Dict[str, Any], album_messages: List[Any]) -> bool:
    # forward_rule ON => skip forwarded albums
    if pair.get("forward_rule", False) and any(is_forwarded(m) for m in album_messages):
        return False

    # post_rule OFF => allow every album
    if not pair.get("post_rule", True):
        return True

    # post_rule ON => only albums containing video
    return any(is_video_message(m) for m in album_messages)


def should_skip_forwarded(pair: Dict[str, Any], msg) -> bool:
    return bool(pair.get("forward_rule", False)) and is_forwarded(msg)


def should_skip_album_forwarded(pair: Dict[str, Any], album_messages: List[Any]) -> bool:
    return bool(pair.get("forward_rule", False)) and any(is_forwarded(m) for m in album_messages)


def get_ads_link(pair: Dict[str, Any]) -> str:
    return (pair.get("ads_link") or "").strip()


def build_single_text(pair: Dict[str, Any], msg) -> Optional[str]:
    base = get_clean_text_from_message(msg)
    final_text = clean_and_add_ads(base, get_ads_link(pair))
    return final_text if final_text else None


def build_single_caption(pair: Dict[str, Any], msg) -> Optional[str]:
    base = get_clean_text_from_message(msg)
    final_caption = clean_and_add_ads(base, get_ads_link(pair))
    return final_caption if final_caption else None


def build_album_captions(pair: Dict[str, Any], album_messages: List[Any]) -> List[str]:
    captions = []
    ads_link = get_ads_link(pair)
    first_video_index = None

    for idx, m in enumerate(album_messages):
        if is_video_message(m):
            first_video_index = idx
            break

    if first_video_index is None and album_messages:
        first_video_index = 0

    for idx, m in enumerate(album_messages):
        base_caption = get_clean_text_from_message(m)
        if ads_link and first_video_index is not None and idx == first_video_index:
            captions.append(clean_and_add_ads(base_caption, ads_link))
        else:
            captions.append(base_caption)

    return captions


# =========================================================
# SAFE TELEGRAM CALLS
# =========================================================
async def run_with_floodwait(coro_factory, *args, **kwargs):
    while True:
        try:
            return await coro_factory(*args, **kwargs)
        except FloodWaitError as e:
            seconds = int(getattr(e, "seconds", 0))
            logger.warning("FloodWait detected. Sleeping for %s seconds.", seconds)
            await asyncio.sleep(seconds)
        except Exception:
            raise


async def safe_get_entity(link_or_username: str):
    return await run_with_floodwait(client.get_entity, link_or_username)


async def safe_get_message(entity, ids):
    return await run_with_floodwait(client.get_messages, entity, ids=ids)


async def safe_get_messages(entity, ids: List[int]):
    return await run_with_floodwait(client.get_messages, entity, ids=ids)


async def safe_send_message(entity, text: str):
    if not text or not text.strip():
        return None
    return await run_with_floodwait(client.send_message, entity, text)


async def safe_send_file(entity, file, caption: Optional[str] = None):
    kwargs = {}
    if caption is not None:
        kwargs["caption"] = caption
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
            "target_entity": None,
            "source_chat_id": None,
            "target_chat_id": None,
            "lock": asyncio.Lock(),
            "last_action_time": 0.0,
            "processed_grouped_ids_live": set(),
        }
    return runtime_cache[user_key][pair_key]


def reset_pair_runtime_state(user_id: int, pair_id: int, *, clear_entities: bool = False) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["last_action_time"] = 0.0
    runtime["processed_grouped_ids_live"].clear()
    if clear_entities:
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None


async def resolve_pair_entities(user_id: int, pair: Dict[str, Any]) -> bool:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_id = (pair.get("source_id") or "").strip()
    target_id = (pair.get("target_id") or "").strip()

    if not source_id or not target_id:
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None
        return False

    try:
        source_entity = await safe_get_entity(source_id)
        target_entity = await safe_get_entity(target_id)
        runtime["source_entity"] = source_entity
        runtime["target_entity"] = target_entity
        runtime["source_chat_id"] = getattr(source_entity, "id", None)
        runtime["target_chat_id"] = getattr(target_entity, "id", None)
        logger.info("Resolved pair %s | source=%s | target=%s", pair_id, source_id, target_id)
        return True
    except Exception:
        logger.exception("Failed to resolve entities for pair %s", pair_id)
        return False


async def resolve_all_pairs(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        await resolve_pair_entities(user_id, pair)


# =========================================================
# STATE HELPERS
# =========================================================
def is_duplicate(pair: Dict[str, Any], source_msg_id: int) -> bool:
    return source_msg_id in pair.get("recent_sent_ids", [])


def mark_sent_ids(user_id: int, pair: Dict[str, Any], source_ids: List[int]) -> None:
    recent_ids = pair.get("recent_sent_ids", [])
    for sid in source_ids:
        if sid not in recent_ids:
            recent_ids.append(sid)
    pair["recent_sent_ids"] = trim_recent_ids(recent_ids)
    save_pair(user_id, pair)


def update_last_processed(user_id: int, pair: Dict[str, Any], msg_id: int) -> None:
    if msg_id > int(pair.get("last_processed_id", 0)):
        pair["last_processed_id"] = msg_id
        save_pair(user_id, pair)


async def apply_human_delay(user_id: int, pair_id: int) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    last_action_time = runtime.get("last_action_time", 0.0)
    if last_action_time <= 0:
        return
    target_delay = random.randint(DELAY_MIN_SECONDS, DELAY_MAX_SECONDS)
    elapsed = time.time() - last_action_time
    if elapsed < target_delay:
        wait_for = target_delay - elapsed
        logger.info("Human-like delay for pair %s: %.2f seconds", pair_id, wait_for)
        await asyncio.sleep(wait_for)


def mark_action_done(user_id: int, pair_id: int) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["last_action_time"] = time.time()


# =========================================================
# REPOST HELPERS
# =========================================================
async def repost_single_message(
    user_id: int,
    pair: Dict[str, Any],
    msg,
    *,
    ignore_post_rule: bool = False,
) -> List[int]:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    target_entity = runtime["target_entity"]

    if should_skip_forwarded(pair, msg):
        return []
    if not ignore_post_rule and not pair_matches_filters(pair, msg):
        return []
    if is_duplicate(pair, msg.id):
        return []

    final_text = build_single_text(pair, msg)
    final_caption = build_single_caption(pair, msg)

    try:
        if msg.media:
            await safe_send_file(target_entity, msg.media, caption=final_caption)
        elif final_text:
            await safe_send_message(target_entity, final_text)
        else:
            return []

        return [msg.id]
    except Exception:
        logger.exception("Failed to repost message %s", msg.id)
        return []

async def repost_album(
    user_id: int,
    pair: Dict[str, Any],
    album_messages: List[Any],
    *,
    ignore_post_rule: bool = False,
) -> List[int]:
    if not album_messages:
        return []

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    target_entity = runtime["target_entity"]

    album_messages = sorted(album_messages, key=lambda x: x.id)
    grouped_id = getattr(album_messages[0], "grouped_id", None)

    if should_skip_album_forwarded(pair, album_messages):
        logger.info("Skipping album %s for pair %s due to forward_rule", grouped_id, pair_id)
        return []

    if not ignore_post_rule and not pair_album_matches_filters(pair, album_messages):
        logger.info("Skipping album %s for pair %s due to post_rule filter", grouped_id, pair_id)
        return []

    album_ids = [m.id for m in album_messages]
    if any(is_duplicate(pair, mid) for mid in album_ids):
        logger.info("Skipping album %s for pair %s because one or more items already sent", grouped_id, pair_id)
        return []

    files: List[Any] = []
    valid_messages: List[Any] = []
    for m in album_messages:
        if m.media:
            files.append(m.media)
            valid_messages.append(m)

    if not files:
        return []

    captions = build_album_captions(pair, valid_messages)

    try:
        await safe_send_album(target_entity, files, captions)
        logger.info("Reposted album %s for pair %s", grouped_id, pair_id)
        return album_ids
    except Exception:
        logger.exception("Failed to repost album %s for pair %s", grouped_id, pair_id)
        return []


async def collect_grouped_album_messages(entity, anchor_msg_id: int, grouped_id: int, window: int = 20) -> List[Any]:
    start_id = max(1, anchor_msg_id - window + 1)
    candidate_ids = list(range(start_id, anchor_msg_id + 1))
    messages = await safe_get_messages(entity, candidate_ids)

    if not messages:
        return []

    grouped_messages = [
        m for m in messages
        if m and getattr(m, "grouped_id", None) == grouped_id
    ]
    grouped_messages.sort(key=lambda x: x.id)
    return grouped_messages


async def repost_preview_for_video(user_id: int, pair: Dict[str, Any], msg) -> List[int]:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    prev_msg_id = msg.id - 1

    if prev_msg_id <= 0:
        return []

    try:
        prev_msg = await safe_get_message(runtime["source_entity"], prev_msg_id)
        if not prev_msg:
            return []

        if getattr(prev_msg, "grouped_id", None):
            album_messages = await collect_grouped_album_messages(
                runtime["source_entity"],
                prev_msg.id,
                prev_msg.grouped_id,
            )
            if not album_messages:
                return []
            return await repost_album(
                user_id,
                pair,
                album_messages,
                ignore_post_rule=True,
            )

        return await repost_single_message(
            user_id,
            pair,
            prev_msg,
            ignore_post_rule=True,
        )
    except Exception as e:
        logger.debug("Previous preview before message %s not found or skipped: %s", msg.id, e)
        return []


# =========================================================
# PROCESSING
# =========================================================
async def process_message_object(user_id: int, pair: Dict[str, Any], msg) -> None:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    if runtime["source_entity"] is None or runtime["target_entity"] is None:
        return

    async with runtime["lock"]:
        try:
            if getattr(msg, "grouped_id", None):
                update_last_processed(user_id, pair, msg.id)
                return

            is_video = is_video_message(msg)
            should_process = not pair.get("post_rule", True) or is_video

            if should_process:
                await apply_human_delay(user_id, pair_id)

                sent_ids: List[int] = []

                if pair.get("post_rule", True) and is_video:
                    preview_sent_ids = await repost_preview_for_video(user_id, pair, msg)
                    if preview_sent_ids:
                        sent_ids.extend(preview_sent_ids)

                current_sent_ids = await repost_single_message(user_id, pair, msg)
                if current_sent_ids:
                    sent_ids.extend(current_sent_ids)

                if sent_ids:
                    mark_sent_ids(user_id, pair, sent_ids)
                    mark_action_done(user_id, pair_id)

            update_last_processed(user_id, pair, msg.id)

        except Exception:
            logger.exception("Error in process_message_object for pair %s and msg %s", pair_id, getattr(msg, "id", None))


async def process_album_object(user_id: int, pair: Dict[str, Any], album_messages: List[Any]) -> None:
    if not album_messages:
        return

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    album_messages = sorted(album_messages, key=lambda x: x.id)
    highest_id = album_messages[-1].id

    async with runtime["lock"]:
        try:
            grouped_id = getattr(album_messages[0], "grouped_id", None)
            if grouped_id is not None:
                seen_groups = runtime["processed_grouped_ids_live"]
                if grouped_id in seen_groups:
                    update_last_processed(user_id, pair, highest_id)
                    return
                seen_groups.add(grouped_id)

            # post_rule OFF => allow every album
            if not pair.get("post_rule", True):
                await apply_human_delay(user_id, pair_id)

                sent_ids = await repost_album(user_id, pair, album_messages)
                if sent_ids:
                    mark_sent_ids(user_id, pair, sent_ids)
                    mark_action_done(user_id, pair_id)

                update_last_processed(user_id, pair, highest_id)
                return

            # post_rule ON => only albums containing video
            has_video = any(is_video_message(m) for m in album_messages)
            if not has_video:
                update_last_processed(user_id, pair, highest_id)
                return

            await apply_human_delay(user_id, pair_id)

            sent_ids = await repost_album(user_id, pair, album_messages)
            if sent_ids:
                mark_sent_ids(user_id, pair, sent_ids)
                mark_action_done(user_id, pair_id)

            update_last_processed(user_id, pair, highest_id)

        except Exception:
            logger.exception(
                "Unexpected error while processing album grouped_id=%s pair %s",
                getattr(album_messages[0], "grouped_id", None),
                pair_id,
            )
            update_last_processed(user_id, pair, highest_id)


# =========================================================
# SCAN / STARTUP / POLLING
# =========================================================
async def fetch_latest_message_id(source_entity) -> int:
    try:
        latest = await run_with_floodwait(client.get_messages, source_entity, limit=1)
        if latest:
            return latest[0].id
    except Exception:
        logger.exception("Failed to fetch latest message id")
    return 0


async def scan_pair(user_id: int, pair: Dict[str, Any]) -> None:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_entity = runtime["source_entity"]
    if not source_entity:
        return

    last_processed_id = int(pair.get("last_processed_id", 0))
    logger.info("Scan begin for pair %s | last_processed_id=%s", pair_id, last_processed_id)

    try:
        grouped_map: Dict[int, List[Any]] = {}
        if last_processed_id == 0:
            latest_msgs = await run_with_floodwait(client.get_messages, source_entity, limit=INITIAL_SCAN_LIMIT)
            latest_msgs = sorted(latest_msgs, key=lambda x: x.id)
            for msg in latest_msgs:
                grouped_id = getattr(msg, "grouped_id", None)
                if grouped_id:
                    grouped_map.setdefault(grouped_id, []).append(msg)
                else:
                    await process_message_object(user_id, pair, msg)
        else:
            current_latest_id = await fetch_latest_message_id(source_entity)
            if current_latest_id > last_processed_id:
                async for msg in client.iter_messages(source_entity, min_id=last_processed_id, reverse=True):
                    grouped_id = getattr(msg, "grouped_id", None)
                    if grouped_id:
                        grouped_map.setdefault(grouped_id, []).append(msg)
                    else:
                        await process_message_object(user_id, pair, msg)

        for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
            await process_album_object(user_id, pair, album_msgs)

        # small safety recheck of latest few posts
        latest_msgs = await run_with_floodwait(client.get_messages, source_entity, limit=LATEST_RECHECK_LIMIT)
        latest_msgs = sorted(latest_msgs, key=lambda x: x.id)
        grouped_map_2: Dict[int, List[Any]] = {}
        for msg in latest_msgs:
            grouped_id = getattr(msg, "grouped_id", None)
            if grouped_id:
                grouped_map_2.setdefault(grouped_id, []).append(msg)
            else:
                await process_message_object(user_id, pair, msg)
        for _, album_msgs in sorted(grouped_map_2.items(), key=lambda item: min(m.id for m in item[1])):
            await process_album_object(user_id, pair, album_msgs)

    except Exception:
        logger.exception("Scan failed for pair %s", pair_id)


async def scan_all_pairs(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        resolved = await resolve_pair_entities(user_id, pair)
        if resolved:
            await scan_pair(user_id, pair)


async def run_poll_once(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        resolved = await resolve_pair_entities(user_id, pair)
        if resolved:
            await scan_pair(user_id, pair)


async def poll_new_updates_loop() -> None:
    global account_user_id
    while True:
        try:
            if not account_user_id:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue
            await run_poll_once(account_user_id)
        except Exception:
            logger.exception("Polling loop error")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# =========================================================
# COMMAND REPLIES
# =========================================================
async def send_self_reply(chat_id: int, text: str) -> None:
    try:
        await safe_send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send self reply")


def format_status(user_id: int) -> str:
    record = ensure_user_record(user_id)
    pairs = record.get("pairs", [])
    if not pairs:
        return "No pairs configured. Use /addpair <source> <target>"

    lines = ["Status:"]
    for pair in pairs:
        ads_text = pair.get("ads_link", "") or "[not set]"
        lines.append(
            f"\nPair {pair['pair_id']}"
            f"\nSource: {pair.get('source_id', '')}"
            f"\nTarget: {pair.get('target_id', '')}"
            f"\nforward_rule: {'ON' if pair.get('forward_rule', False) else 'OFF'}"
            f"\npost_rule: {'ON' if pair.get('post_rule', True) else 'OFF'}"
            f"\nads_link: {ads_text}"
            f"\nlast_processed_id: {pair.get('last_processed_id', 0)}"
            f"\nrecent_sent_ids_count: {len(pair.get('recent_sent_ids', []))}"
        )
    return "\n".join(lines)


# =========================================================
# COMMANDS
# =========================================================


@client.on(events.NewMessage(outgoing=True, pattern=r"^/start$"))
async def start_command_handler(event):
    await send_self_reply(
        event.chat_id,
        "Multi-pair userbot is running.\n\n"
        "Use /help for usage."
    )

@client.on(events.NewMessage(outgoing=True, pattern=r"^/help$"))
async def help_command_handler(event):
    await send_self_reply(
        event.chat_id,
        "Available commands:\n\n"
        "/start - short bot status message\n"
        "/help - show this help message\n"
        "/addpair <source> <target> - add a new source/target pair\n"
        "/editA <id> <new_source> - change source channel for a pair\n"
        "/editB <id> <new_target> - change target channel for a pair\n"
        "/listpairs - show all configured pairs\n"
        "/status - show pair status summary\n"
        "/deletepair <id> - delete one pair\n"
        "/check - scan all pairs now\n"
        "/check <id> - scan one pair now\n"
        "/poll - run one manual poll cycle for all pairs\n"
        "/ads <id> <link> - set ads link for a pair\n"
        "/showads <id> - show current ads link\n"
        "/clearads <id> - clear ads link\n"
        "/forward_rule <on/off> <id> - forwarded message rule\n"
        "/post_rule <on/off> <id> - on = video + previous preview post, off = post everything\n"
        "/addpair https://t.me/source1 https://t.me/target1\n"
        "/editA 2 https://t.me/newsource\n"
        "/editB 2 https://t.me/newtarget\n"
        "/ads 1 https://example.com\n"
        "/forward_rule on 1\n"
        "/post_rule off 1\n"
        "/check 1\n"
        "/poll"
    )

@client.on(events.NewMessage(outgoing=True, pattern=r"^/addpair\s+(\S+)\s+(\S+)$"))
async def addpair_handler(event):
    source_id = event.pattern_match.group(1).strip()
    target_id = event.pattern_match.group(2).strip()

    record = ensure_user_record(account_user_id)
    pair_id = next_pair_id(record)
    pair = {
        "pair_id": pair_id,
        "source_id": source_id,
        "target_id": target_id,
        "last_processed_id": 0,
        "recent_sent_ids": [],
        "forward_rule": False,
        "post_rule": True,
        "ads_link": "",
    }
    record["pairs"].append(pair)
    save_pair(account_user_id, pair)

    resolved = await resolve_pair_entities(account_user_id, pair)
    if resolved:
        await send_self_reply(event.chat_id, f"Pair {pair_id} added.\nSource: {source_id}\nTarget: {target_id}")
        await scan_pair(account_user_id, pair)
    else:
        await send_self_reply(event.chat_id, f"Pair {pair_id} saved, but source/target could not be resolved yet.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/listpairs$"))
async def listpairs_handler(event):
    await send_self_reply(event.chat_id, format_status(account_user_id))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/deletepair\s+(\d+)$"))
async def deletepair_handler(event):
    pair_id = int(event.pattern_match.group(1))
    record = ensure_user_record(account_user_id)
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    record["pairs"] = [p for p in record["pairs"] if int(p["pair_id"]) != pair_id]
    delete_pair_from_db(account_user_id, pair_id)
    runtime_cache.get(str(account_user_id), {}).pop(str(pair_id), None)
    await send_self_reply(event.chat_id, f"Pair {pair_id} deleted.")

@client.on(events.NewMessage(outgoing=True, pattern=r"^/editA\s+(\d+)\s+(\S+)$"))
async def edit_a_command_handler(event):
    pair_id = int(event.pattern_match.group(1))
    new_source = event.pattern_match.group(2).strip()

    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    try:
        await safe_get_entity(new_source)
    except Exception:
        await send_self_reply(event.chat_id, "Invalid or inaccessible Channel A / source link.")
        return

    pair["source_id"] = new_source
    pair["last_processed_id"] = 0
    pair["recent_sent_ids"] = []
    save_pair(account_user_id, pair)
    reset_pair_runtime_state(account_user_id, pair_id, clear_entities=True)

    resolved = await resolve_pair_entities(account_user_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Pair {pair_id} saved, but the new source could not be resolved.")
        return

    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} Channel A updated.\n\nNew source: {new_source}\nRescanning latest posts now..."
    )
    await scan_pair(account_user_id, pair)

@client.on(events.NewMessage(outgoing=True, pattern=r"^/editB\s+(\d+)\s+(\S+)$"))
async def edit_b_command_handler(event):
    pair_id = int(event.pattern_match.group(1))
    new_target = event.pattern_match.group(2).strip()

    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    try:
        await safe_get_entity(new_target)
    except Exception:
        await send_self_reply(event.chat_id, "Invalid or inaccessible Channel B / target link.")
        return

    pair["target_id"] = new_target
    save_pair(account_user_id, pair)
    reset_pair_runtime_state(account_user_id, pair_id, clear_entities=True)

    resolved = await resolve_pair_entities(account_user_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Pair {pair_id} saved, but the new target could not be resolved.")
        return

    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} Channel B updated.\n\nNew target: {new_target}"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ads\s+(\d+)\s+(.+)$"))
async def ads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    ads_link = event.pattern_match.group(2).strip()
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["ads_link"] = ads_link
    save_pair(account_user_id, pair)
    await send_self_reply(event.chat_id, f"Ads link saved for pair {pair_id}.\n\n{ads_link}")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/showads\s+(\d+)$"))
async def showads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    ads_link = get_ads_link(pair)
    if ads_link:
        await send_self_reply(event.chat_id, f"Pair {pair_id} ads link:\n{ads_link}")
    else:
        await send_self_reply(event.chat_id, f"Pair {pair_id} has no ads link set.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/clearads\s+(\d+)$"))
async def clearads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["ads_link"] = ""
    save_pair(account_user_id, pair)
    await send_self_reply(event.chat_id, f"Ads link cleared for pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/forward_rule\s+(on|off)\s+(\d+)$"))
async def forward_rule_handler(event):
    state = event.pattern_match.group(1).lower()
    pair_id = int(event.pattern_match.group(2))
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["forward_rule"] = (state == "on")
    save_pair(account_user_id, pair)
    await send_self_reply(event.chat_id, f"forward_rule for pair {pair_id} is now {'ON' if pair['forward_rule'] else 'OFF' }.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/post_rule\s+(on|off)\s+(\d+)$"))
async def post_rule_handler(event):
    state = event.pattern_match.group(1).lower()
    pair_id = int(event.pattern_match.group(2))
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["post_rule"] = (state == "on")
    save_pair(account_user_id, pair)
    await send_self_reply(event.chat_id, f"post_rule for pair {pair_id} is now {'ON' if pair['post_rule'] else 'OFF' }.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check$"))
async def check_all_handler(event):
    await send_self_reply(event.chat_id, "Checking all pairs...")
    await scan_all_pairs(account_user_id)
    await send_self_reply(event.chat_id, "Check completed.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check\s+(\d+)$"))
async def check_pair_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    resolved = await resolve_pair_entities(account_user_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Could not resolve pair {pair_id}.")
        return
    await send_self_reply(event.chat_id, f"Checking pair {pair_id}...")
    await scan_pair(account_user_id, pair)
    await send_self_reply(event.chat_id, f"Check completed for pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/poll$"))
async def poll_now_handler(event):
    await send_self_reply(event.chat_id, "Running one manual poll cycle for all pairs...")
    await run_poll_once(account_user_id)
    await send_self_reply(event.chat_id, "Manual poll cycle completed.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/status$"))
async def status_handler(event):
    await send_self_reply(event.chat_id, format_status(account_user_id))


# =========================================================
# LIVE EVENTS
# =========================================================
@client.on(events.NewMessage())
async def live_new_message_handler(event):
    global account_user_id
    if not account_user_id:
        return
    if event.out:
        return

    msg = event.message
    record = ensure_user_record(account_user_id)
    event_channel_id = None
    try:
        if getattr(msg, "peer_id", None) and hasattr(msg.peer_id, "channel_id"):
            event_channel_id = msg.peer_id.channel_id
    except Exception:
        pass

    for pair in record["pairs"]:
        pair_id = int(pair["pair_id"])
        runtime = get_pair_runtime(account_user_id, pair_id)
        source_chat_id = runtime.get("source_chat_id")
        if not source_chat_id:
            continue

        if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
            continue

        await process_message_object(account_user_id, pair, msg)


@client.on(events.Album())
async def live_album_handler(event):
    global account_user_id
    if not account_user_id:
        return
    album_messages = list(event.messages or [])
    if not album_messages:
        return

    first_msg = album_messages[0]
    record = ensure_user_record(account_user_id)
    event_channel_id = None
    try:
        if getattr(first_msg, "peer_id", None) and hasattr(first_msg.peer_id, "channel_id"):
            event_channel_id = first_msg.peer_id.channel_id
    except Exception:
        pass

    for pair in record["pairs"]:
        pair_id = int(pair["pair_id"])
        runtime = get_pair_runtime(account_user_id, pair_id)
        source_chat_id = runtime.get("source_chat_id")
        if not source_chat_id:
            continue

        if event_channel_id != source_chat_id and getattr(event, "chat_id", None) != source_chat_id:
            continue

        await process_album_object(account_user_id, pair, album_messages)


# =========================================================
# LOGIN / INIT
# =========================================================
async def login_flow() -> None:
    if await client.is_user_authorized():
        logger.info("Using existing authorized session.")
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
                print("Wrong password. Try again.")


async def initialize_runtime() -> None:
    global account_user_id, user_data
    init_db()
    user_data = load_all_user_data()

    me = await client.get_me()
    if not account_user_id:
        account_user_id = me.id
    logger.info("Logged in as user_id=%s | account_user_id=%s", me.id, account_user_id)

    ensure_user_record(account_user_id)
    await resolve_all_pairs(account_user_id)

    record = ensure_user_record(account_user_id)
    if record["pairs"]:
        for pair in record["pairs"]:
            await scan_pair(account_user_id, pair)
    else:
        logger.info("No pairs configured yet. Use /addpair <source> <target>")


# =========================================================
# MAIN
# =========================================================
async def main() -> None:
    logger.info("Bot startup initiated.")
    await client.connect()
    keep_alive()
    await login_flow()
    await initialize_runtime()
    asyncio.create_task(poll_new_updates_loop())
    logger.info("User-bot is now running.")
    print("User-bot is running. Use /start in Saved Messages.")
    await client.run_until_disconnected()


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
            break
        except KeyboardInterrupt:
            logger.info("Bot stopped by user.")
            break
        except Exception:
            logger.exception("Fatal error in main loop. Restarting in 5 seconds...")
            time.sleep(5)
