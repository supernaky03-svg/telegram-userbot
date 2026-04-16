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
    InviteHashEmptyError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    InviteRequestSentError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.messages import ImportChatInviteRequest
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
INITIAL_SCAN_LIMIT = int(os.getenv("INITIAL_SCAN_LIMIT", "100"))
LATEST_RECHECK_LIMIT = int(os.getenv("LATEST_RECHECK_LIMIT", "10"))
RECENT_IDS_LIMIT = int(os.getenv("RECENT_IDS_LIMIT", "100"))

DEFAULT_SCAN_COUNT = 100
SCAN_COUNT_ALL = -1

# Simple keyword list for post_rule=ON

URL_REGEX = re.compile(r'(?i)\b(?:https?://|www\.|t\.me/)[^\s<>"\'\]\)]+' )
INVITE_LINK_RE = re.compile(
    r"(?i)(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|\+)([A-Za-z0-9_-]+)"
)
TG_JOIN_INVITE_RE = re.compile(r"(?i)tg://join\?invite=([A-Za-z0-9_-]+)")

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

    link_pattern = r'(https?://\S+|www\.\S+|(?:https?://)?(?:t\.me|telegram\.me)/\S+|tg://\S+)'
    username_pattern = r'(?<!\w)@[A-Za-z][A-Za-z0-9_]{4,31}\b'

    clean_text = re.sub(link_pattern, '', text, flags=re.IGNORECASE)
    clean_text = re.sub(username_pattern, '', clean_text)

    clean_text = clean_text.strip()

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


def normalize_scan_count_value(value: Any) -> int:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "all":
            return SCAN_COUNT_ALL
    try:
        ivalue = int(value)
    except Exception:
        ivalue = DEFAULT_SCAN_COUNT

    if ivalue == SCAN_COUNT_ALL:
        return SCAN_COUNT_ALL

    return max(1, ivalue)


def format_scan_count_value(value: Any) -> str:
    normalized = normalize_scan_count_value(value)
    return "all" if normalized == SCAN_COUNT_ALL else str(normalized)


def get_pair_initial_scan_limit(pair: Dict[str, Any]) -> Optional[int]:
    normalized = normalize_scan_count_value(pair.get("scan_count", DEFAULT_SCAN_COUNT))
    return None if normalized == SCAN_COUNT_ALL else normalized


def init_db() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
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
                    scan_count INTEGER NOT NULL DEFAULT 100,
                    keyword_mode TEXT NOT NULL DEFAULT 'off',
                    keyword_value TEXT NOT NULL DEFAULT '',
                    initial_scan_pending BOOLEAN NOT NULL DEFAULT FALSE,
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
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS scan_count INTEGER NOT NULL DEFAULT 100"
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS keyword_mode TEXT NOT NULL DEFAULT 'off'"
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS keyword_value TEXT NOT NULL DEFAULT ''"
            )
            cur.execute(
                "ALTER TABLE pairs ADD COLUMN IF NOT EXISTS initial_scan_pending BOOLEAN NOT NULL DEFAULT FALSE"
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
                       forward_rule, post_rule, scan_count,
                       keyword_mode, keyword_value, initial_scan_pending,
                       ads_link
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
                        "scan_count": normalize_scan_count_value(
                            row.get("scan_count", DEFAULT_SCAN_COUNT)
                        ),
                        "keyword_mode": normalize_keyword_mode(row.get("keyword_mode", "off")),
                        "keyword_value": normalize_keyword_value(row.get("keyword_value", "")),
                        "initial_scan_pending": bool(row.get("initial_scan_pending", False)),
                        "ads_link": (row["ads_link"] or "").strip(),
                    }
                )

    return data


def save_pair(owner_user_id: int, pair: Dict[str, Any]) -> None:
    scan_count = normalize_scan_count_value(pair.get("scan_count", DEFAULT_SCAN_COUNT))
    keyword_mode = normalize_keyword_mode(pair.get("keyword_mode", "off"))
    keyword_value = normalize_keyword_value(pair.get("keyword_value", ""))
    initial_scan_pending = bool(pair.get("initial_scan_pending", False))

    pair["scan_count"] = scan_count
    pair["keyword_mode"] = keyword_mode
    pair["keyword_value"] = keyword_value
    pair["initial_scan_pending"] = initial_scan_pending

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pairs (
                    owner_user_id, pair_id, source_id, target_id,
                    last_processed_id, recent_sent_ids,
                    forward_rule, post_rule, scan_count,
                    keyword_mode, keyword_value, initial_scan_pending,
                    ads_link
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (owner_user_id, pair_id)
                DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    target_id = EXCLUDED.target_id,
                    last_processed_id = EXCLUDED.last_processed_id,
                    recent_sent_ids = EXCLUDED.recent_sent_ids,
                    forward_rule = EXCLUDED.forward_rule,
                    post_rule = EXCLUDED.post_rule,
                    scan_count = EXCLUDED.scan_count,
                    keyword_mode = EXCLUDED.keyword_mode,
                    keyword_value = EXCLUDED.keyword_value,
                    initial_scan_pending = EXCLUDED.initial_scan_pending,
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
                    scan_count,
                    keyword_mode,
                    keyword_value,
                    initial_scan_pending,
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
    max_pair_id = max((int(p.get("pair_id", 0)) for p in pairs), default=0)
    return max_pair_id + 1


def get_pair_by_id(user_id: int, pair_id: int) -> Optional[Dict[str, Any]]:
    record = ensure_user_record(user_id)
    for pair in record["pairs"]:
        if int(pair.get("pair_id", 0)) == int(pair_id):
            return pair
    return None


def trim_recent_ids(ids_list: List[int], limit: int = RECENT_IDS_LIMIT) -> List[int]:
    return ids_list[-limit:]


def normalize_keyword_mode(value: Any) -> str:
    lowered = (value or "off").strip().lower()
    if lowered in {"off", "ban", "post"}:
        return lowered
    return "off"


def normalize_keyword_value(value: Any) -> str:
    return (value or "").strip()


def get_pair_keyword_mode(pair: Dict[str, Any]) -> str:
    return normalize_keyword_mode(pair.get("keyword_mode", "off"))


def get_pair_keyword_value(pair: Dict[str, Any]) -> str:
    return normalize_keyword_value(pair.get("keyword_value", ""))


def set_pair_keyword_state(pair: Dict[str, Any], mode: str, keyword: str = "") -> None:
    normalized_mode = normalize_keyword_mode(mode)
    normalized_keyword = normalize_keyword_value(keyword)
    if normalized_mode == "off":
        normalized_keyword = ""
    pair["keyword_mode"] = normalized_mode
    pair["keyword_value"] = normalized_keyword


def pair_has_pending_initial_scan(pair: Dict[str, Any]) -> bool:
    return bool(pair.get("initial_scan_pending", False))


def get_pending_initial_scan_pairs(user_id: int) -> List[Dict[str, Any]]:
    record = ensure_user_record(user_id)
    return [
        pair for pair in sorted(record.get("pairs", []), key=lambda item: int(item.get("pair_id", 0)))
        if pair_has_pending_initial_scan(pair)
    ]


class CommandUsageError(Exception):
    pass


class EntityResolutionError(Exception):
    pass


ADDPAIR_USAGE = (
    "Usage: /addpair [pair_number] <source> <target> [scan_count]\n"
    "scan_count = positive number or all"
)

EDITA_USAGE = (
    "Usage: /editA <pair_number> <source> [scan_count]\n"
    "scan_count = positive number or all"
)


def extract_invite_hash(value: str) -> Optional[str]:
    text = (value or "").strip()
    match = INVITE_LINK_RE.search(text)
    if match:
        return match.group(1)

    match = TG_JOIN_INVITE_RE.search(text)
    if match:
        return match.group(1)

    return None


def parse_scan_count_token(token: Optional[str]) -> int:
    if token is None:
        return DEFAULT_SCAN_COUNT

    value = token.strip().lower()
    if value == "all":
        return SCAN_COUNT_ALL

    if not re.fullmatch(r"\d+", value):
        raise CommandUsageError("Invalid scan count. Use a positive number or all.")

    count = int(value)
    if count <= 0:
        raise CommandUsageError("Invalid scan count. Use a positive number or all.")

    return count


def parse_addpair_command_args(text: str) -> Dict[str, Any]:
    parts = (text or "").split()
    args = parts[1:]

    if not args:
        raise CommandUsageError(ADDPAIR_USAGE)

    explicit_pair_id: Optional[int] = None

    if len(args) >= 3 and re.fullmatch(r"\d+", args[0]):
        explicit_pair_id = int(args[0])
        if explicit_pair_id <= 0:
            raise CommandUsageError(
                "Invalid pair number. It must be a positive integer.\n" + ADDPAIR_USAGE
            )
        args = args[1:]

    if len(args) < 2:
        raise CommandUsageError("Missing required arguments.\n" + ADDPAIR_USAGE)

    if len(args) > 3:
        raise CommandUsageError("Too many arguments.\n" + ADDPAIR_USAGE)

    source_id = args[0].strip()
    target_id = args[1].strip()
    scan_count = parse_scan_count_token(args[2]) if len(args) == 3 else DEFAULT_SCAN_COUNT

    return {
        "pair_id": explicit_pair_id,
        "source_id": source_id,
        "target_id": target_id,
        "scan_count": scan_count,
    }


def parse_edita_command_args(text: str) -> Dict[str, Any]:
    parts = (text or "").split()
    args = parts[1:]

    if len(args) < 2:
        raise CommandUsageError("Missing required arguments.\n" + EDITA_USAGE)

    if len(args) > 3:
        raise CommandUsageError("Too many arguments.\n" + EDITA_USAGE)

    if not re.fullmatch(r"\d+", args[0]):
        raise CommandUsageError(
            "Invalid pair number. It must be a positive integer.\n" + EDITA_USAGE
        )

    pair_id = int(args[0])
    if pair_id <= 0:
        raise CommandUsageError(
            "Invalid pair number. It must be a positive integer.\n" + EDITA_USAGE
        )

    source_id = args[1].strip()
    scan_count = parse_scan_count_token(args[2]) if len(args) == 3 else DEFAULT_SCAN_COUNT

    return {
        "pair_id": pair_id,
        "source_id": source_id,
        "scan_count": scan_count,
    }


def cache_pair_entities(user_id: int, pair_id: int, source_entity, target_entity) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["source_entity"] = source_entity
    runtime["target_entity"] = target_entity
    runtime["source_chat_id"] = getattr(source_entity, "id", None) if source_entity else None
    runtime["target_chat_id"] = getattr(target_entity, "id", None) if target_entity else None


async def resolve_entity_from_numeric_id(raw_id: str):
    entity_id = int(raw_id)

    try:
        return await safe_get_entity(entity_id)
    except Exception:
        pass

    try:
        dialogs = await run_with_floodwait(client.get_dialogs, limit=None)
        wanted_id = abs(entity_id)

        for dialog in dialogs:
            entity = getattr(dialog, "entity", None)
            if entity and getattr(entity, "id", None) == wanted_id:
                return entity
    except Exception:
        logger.exception("Failed dialog-based entity lookup for %s", raw_id)

    raise EntityResolutionError(
        "Channel ID could not be resolved. Make sure this account can access that chat."
    )


async def resolve_entity_reference(raw_ref: str, *, allow_join_via_invite: bool = True):
    ref = (raw_ref or "").strip()
    if not ref:
        raise EntityResolutionError("Empty channel reference.")

    if re.fullmatch(r"-?\d+", ref):
        return await resolve_entity_from_numeric_id(ref)

    invite_hash = extract_invite_hash(ref)
    if invite_hash:
        try:
            return await safe_get_entity(ref)
        except Exception:
            if not allow_join_via_invite:
                raise EntityResolutionError("Private invite link could not be resolved.")

            try:
                result = await run_with_floodwait(client, ImportChatInviteRequest(invite_hash))
                chats = list(getattr(result, "chats", []) or [])
                if chats:
                    return chats[0]
            except UserAlreadyParticipantError:
                pass
            except InviteRequestSentError as e:
                raise EntityResolutionError(
                    "Join request was sent for this invite link. Approve it first, then try again."
                ) from e
            except (InviteHashEmptyError, InviteHashExpiredError, InviteHashInvalidError) as e:
                raise EntityResolutionError("Invite link is invalid or expired.") from e
            except Exception as e:
                raise EntityResolutionError(
                    "Could not join or resolve this private invite link."
                ) from e

            try:
                return await safe_get_entity(ref)
            except Exception as e:
                raise EntityResolutionError("Private invite link could not be resolved.") from e

    try:
        return await safe_get_entity(ref)
    except Exception as e:
        raise EntityResolutionError(
            "Channel link, username, or ID could not be resolved."
        ) from e


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
    if pair.get("forward_rule", False) and is_forwarded(msg):
        return False

    if not pair_keyword_allows_message(pair, msg):
        return False

    if not pair.get("post_rule", True):
        return True

    return is_video_message(msg)


def pair_album_matches_filters(pair: Dict[str, Any], album_messages: List[Any]) -> bool:
    if pair.get("forward_rule", False) and any(is_forwarded(m) for m in album_messages):
        return False

    if not pair_keyword_allows_album(pair, album_messages):
        return False

    if not pair.get("post_rule", True):
        return True

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
        source_entity = await resolve_entity_reference(source_id, allow_join_via_invite=True)
        target_entity = await resolve_entity_reference(target_id, allow_join_via_invite=True)

        cache_pair_entities(user_id, pair_id, source_entity, target_entity)

        logger.info("Resolved pair %s | source=%s | target=%s", pair_id, source_id, target_id)
        return True

    except Exception:
        logger.exception("Failed to resolve entities for pair %s", pair_id)
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None
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
    if not pair_keyword_allows_message(pair, msg):
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

    if not pair_keyword_allows_album(pair, album_messages):
        logger.info("Skipping album %s for pair %s due to keyword filter", grouped_id, pair_id)
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

    if pair_has_pending_initial_scan(pair):
        return

    if runtime["source_entity"] is None or runtime["target_entity"] is None:
        return

    async with runtime["lock"]:
        try:
            if getattr(msg, "grouped_id", None):
                update_last_processed(user_id, pair, msg.id)
                return

            is_video = is_video_message(msg)
            should_process = pair_matches_filters(pair, msg)

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

    if pair_has_pending_initial_scan(pair):
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

            if not pair_album_matches_filters(pair, album_messages):
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

    if pair_has_pending_initial_scan(pair):
        logger.info("Pair %s initial scan is pending. Skipping scan for now.", pair_id)
        return

    if not source_entity:
        return

    last_processed_id = int(pair.get("last_processed_id", 0))
    initial_scan_limit = get_pair_initial_scan_limit(pair)

    logger.info(
        "Scan begin for pair %s | last_processed_id=%s | initial_scan=%s",
        pair_id,
        last_processed_id,
        "all" if initial_scan_limit is None else initial_scan_limit,
    )

    try:
        grouped_map: Dict[int, List[Any]] = {}

        if last_processed_id == 0:
            if initial_scan_limit is None:
                async for msg in client.iter_messages(source_entity, reverse=True):
                    grouped_id = getattr(msg, "grouped_id", None)
                    if grouped_id:
                        grouped_map.setdefault(grouped_id, []).append(msg)
                    else:
                        await process_message_object(user_id, pair, msg)
            else:
                latest_msgs = await run_with_floodwait(
                    client.get_messages,
                    source_entity,
                    limit=initial_scan_limit,
                )
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
                async for msg in client.iter_messages(
                    source_entity,
                    min_id=last_processed_id,
                    reverse=True,
                ):
                    grouped_id = getattr(msg, "grouped_id", None)
                    if grouped_id:
                        grouped_map.setdefault(grouped_id, []).append(msg)
                    else:
                        await process_message_object(user_id, pair, msg)

        for _, album_msgs in sorted(grouped_map.items(), key=lambda item: min(m.id for m in item[1])):
            await process_album_object(user_id, pair, album_msgs)

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


def build_keyword_setup_prompt(pair: Dict[str, Any]) -> str:
    pair_id = int(pair["pair_id"])
    return (
        f"Pair {pair_id} added.\n"
        f"Source: {pair.get('source_id', '')}\n"
        f"Target: {pair.get('target_id', '')}\n"
        f"Scan count: {format_scan_count_value(pair.get('scan_count', DEFAULT_SCAN_COUNT))}\n"
        f"Keyword mode: OFF\n\n"
        f"Initial scan is paused for pair {pair_id}.\n"
        f"If you do not want keyword filtering, reply with: no\n"
        f"If you want keyword filtering, use:\n"
        f"/BanKeyword {pair_id} <word>\n"
        f"/PostKeyword {pair_id} <word>"
    )


async def start_pending_initial_scan_for_pair(user_id: int, pair: Dict[str, Any], chat_id: int) -> bool:
    pair_id = int(pair["pair_id"])

    if not pair_has_pending_initial_scan(pair):
        await send_self_reply(chat_id, f"Pair {pair_id} is not waiting for an initial scan.")
        return False

    pair["initial_scan_pending"] = False
    save_pair(user_id, pair)

    resolved = await resolve_pair_entities(user_id, pair)
    if not resolved:
        pair["initial_scan_pending"] = True
        save_pair(user_id, pair)
        await send_self_reply(
            chat_id,
            f"Pair {pair_id} is still waiting because source/target could not be resolved right now."
        )
        return False

    keyword_suffix = ""
    if get_pair_keyword_mode(pair) != "off" and get_pair_keyword_value(pair):
        keyword_suffix = f" | keyword: {get_pair_keyword_value(pair)}"

    await send_self_reply(
        chat_id,
        f"Starting initial scan for pair {pair_id}.\n"
        f"Keyword mode: {get_pair_keyword_mode(pair).upper()}{keyword_suffix}"
    )
    await scan_pair(user_id, pair)
    return True


def format_status(user_id: int) -> str:
    record = ensure_user_record(user_id)
    pairs = record.get("pairs", [])

    if not pairs:
        return "No pairs configured. Use /addpair [pair_number] <source> <target> [scan_count]"

    lines = ["Status:"]

    for pair in pairs:
        ads_text = pair.get("ads_link", "") or "[not set]"
        lines.append(
            f"\nPair {pair['pair_id']}"
            f"\nSource: {pair.get('source_id', '')}"
            f"\nTarget: {pair.get('target_id', '')}"
            f"\nforward_rule: {'ON' if pair.get('forward_rule', False) else 'OFF'}"
            f"\npost_rule: {'ON' if pair.get('post_rule', True) else 'OFF'}"
            f"\nscan_count: {format_scan_count_value(pair.get('scan_count', DEFAULT_SCAN_COUNT))}"
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
        "/addpair [pair_number] <source> <target> [scan_count] - add a new source/target pair\n"
        "/editA <id> <new_source> [scan_count] - change source channel for a pair\n"
        "/editB <id> <new_target> - change target channel for a pair\n"
        "/BanKeyword <id> <word> - skip posts containing the keyword for that pair\n"
        "/PostKeyword <id> <word> - only repost posts containing the keyword for that pair\n"
        "/ClearKeyword <id> - clear keyword filter for that pair\n"
        "/ShowKeyword <id> - show keyword filter state for that pair\n"
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
        "/post_rule <on/off> <id> - on = video + previous preview post, off = post everything\n\n"
        "Examples:\n"
        "/addpair https://t.me/source1 https://t.me/target1\n"
        "/addpair 7 https://t.me/source1 https://t.me/target1\n"
        "/addpair 8 https://t.me/+privateInviteLink https://t.me/target1 300\n"
        "/addpair 9 -1001234567890 https://t.me/target1 all\n"
        "/editA 2 https://t.me/newsource\n"
        "/editA 3 https://t.me/+privateInviteLink\n"
        "/editA 4 -1001234567890 all\n"
        "/editB 2 https://t.me/newtarget\n"
        "/BanKeyword 3 spam\n"
        "/PostKeyword 3 movie\n"
        "/ClearKeyword 3\n"
        "/ShowKeyword 3\n"
        "/ads 1 https://example.com\n"
        "/forward_rule on 1\n"
        "/post_rule off 1\n"
        "/check 1\n"
        "/poll"
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/addpair(?:\s+.+)?$"))
async def addpair_handler(event):
    try:
        parsed = parse_addpair_command_args(event.raw_text or "")
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    record = ensure_user_record(account_user_id)

    if parsed["pair_id"] is None:
        pair_id = next_pair_id(record)
    else:
        pair_id = parsed["pair_id"]
        if get_pair_by_id(account_user_id, pair_id):
            await send_self_reply(
                event.chat_id,
                f"Pair {pair_id} already exists. Use a different pair number."
            )
            return

    try:
        source_entity = await resolve_entity_reference(
            parsed["source_id"],
            allow_join_via_invite=True,
        )
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid source.\n{e}")
        return

    try:
        target_entity = await resolve_entity_reference(
            parsed["target_id"],
            allow_join_via_invite=True,
        )
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid target.\n{e}")
        return

    pair = {
        "pair_id": pair_id,
        "source_id": parsed["source_id"],
        "target_id": parsed["target_id"],
        "last_processed_id": 0,
        "recent_sent_ids": [],
        "forward_rule": False,
        "post_rule": True,
        "scan_count": parsed["scan_count"],
        "ads_link": "",
    }

    record["pairs"].append(pair)
    record["pairs"].sort(key=lambda p: int(p["pair_id"]))

    save_pair(account_user_id, pair)
    reset_pair_runtime_state(account_user_id, pair_id, clear_entities=True)
    cache_pair_entities(account_user_id, pair_id, source_entity, target_entity)

    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} added.\n"
        f"Source: {pair['source_id']}\n"
        f"Target: {pair['target_id']}\n"
        f"Scan count: {format_scan_count_value(pair['scan_count'])}"
    )

    await scan_pair(account_user_id, pair)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/BanKeyword(?:\s+.+)?$"))
async def ban_keyword_handler(event):
    try:
        parsed = parse_keyword_mode_command_args(event.raw_text or "", BAN_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    keyword = parsed["keyword"]
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "ban", keyword)
    save_pair(account_user_id, pair)
    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} keyword mode: BAN | keyword: {get_pair_keyword_value(pair)}"
    )

    if pair_has_pending_initial_scan(pair):
        await start_pending_initial_scan_for_pair(account_user_id, pair, event.chat_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/PostKeyword(?:\s+.+)?$"))
async def post_keyword_handler(event):
    try:
        parsed = parse_keyword_mode_command_args(event.raw_text or "", POST_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    keyword = parsed["keyword"]
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "post", keyword)
    save_pair(account_user_id, pair)
    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} keyword mode: POST | keyword: {get_pair_keyword_value(pair)}"
    )

    if pair_has_pending_initial_scan(pair):
        await start_pending_initial_scan_for_pair(account_user_id, pair, event.chat_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ClearKeyword(?:\s+.*)?$"))
async def clear_keyword_handler(event):
    try:
        parsed = parse_keyword_pair_only_command_args(event.raw_text or "", CLEAR_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "off", "")
    save_pair(account_user_id, pair)

    response = f"Pair {pair_id} keyword mode: OFF"
    if pair_has_pending_initial_scan(pair):
        response += "\nInitial scan is still paused. Reply with: no"
    await send_self_reply(event.chat_id, response)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ShowKeyword(?:\s+.*)?$"))
async def show_keyword_handler(event):
    try:
        parsed = parse_keyword_pair_only_command_args(event.raw_text or "", SHOW_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    mode = get_pair_keyword_mode(pair).upper()
    keyword = get_pair_keyword_value(pair)
    response = f"Pair {pair_id} keyword mode: {mode}"
    if keyword:
        response += f" | keyword: {keyword}"
    if pair_has_pending_initial_scan(pair):
        response += "\nInitial scan pending: YES"
    await send_self_reply(event.chat_id, response)


@client.on(events.NewMessage(outgoing=True, pattern=r"(?i)^no$"))
async def no_keyword_handler(event):
    pending_pairs = get_pending_initial_scan_pairs(account_user_id)
    if not pending_pairs:
        await send_self_reply(event.chat_id, "No pending initial scan found.")
        return

    pair = pending_pairs[0]
    started = await start_pending_initial_scan_for_pair(account_user_id, pair, event.chat_id)
    if started:
        remaining = len(get_pending_initial_scan_pairs(account_user_id))
        if remaining > 0:
            await send_self_reply(
                event.chat_id,
                f"There {'is' if remaining == 1 else 'are'} still {remaining} pending pair{'s' if remaining != 1 else ''}."
            )


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

@client.on(events.NewMessage(outgoing=True, pattern=r"^/editA(?:\s+.+)?$"))
async def edit_a_command_handler(event):
    try:
        parsed = parse_edita_command_args(event.raw_text or "")
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    new_source = parsed["source_id"]
    new_scan_count = parsed["scan_count"]

    pair = get_pair_by_id(account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    try:
        await resolve_entity_reference(new_source, allow_join_via_invite=True)
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid source.\n{e}")
        return

    pair["source_id"] = new_source
    pair["scan_count"] = new_scan_count
    pair["last_processed_id"] = 0
    pair["recent_sent_ids"] = []

    save_pair(account_user_id, pair)
    reset_pair_runtime_state(account_user_id, pair_id, clear_entities=True)

    resolved = await resolve_pair_entities(account_user_id, pair)
    if not resolved:
        await send_self_reply(
            event.chat_id,
            f"Pair {pair_id} source saved, but source/target could not be fully resolved yet."
        )
        return

    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} Channel A updated.\n\n"
        f"New source: {new_source}\n"
        f"Scan count: {format_scan_count_value(new_scan_count)}\n"
        f"Rescanning now..."
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
    if pair_has_pending_initial_scan(pair):
        await send_self_reply(
            event.chat_id,
            f"Pair {pair_id} is waiting for keyword setup. Use /BanKeyword, /PostKeyword, or reply with no first."
        )
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
