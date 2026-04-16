import json
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row

from .config import DATABASE_URL, DEFAULT_SCAN_COUNT, RECENT_IDS_LIMIT, SCAN_COUNT_ALL
from .utils.filters import normalize_keyword_mode, normalize_keyword_values


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


def get_db_connection():
    return psycopg.connect(DATABASE_URL, sslmode="require", row_factory=dict_row)


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
                    keyword_values JSONB NOT NULL DEFAULT '[]'::jsonb,
                    initial_scan_pending BOOLEAN NOT NULL DEFAULT FALSE,
                    ads_link TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (owner_user_id, pair_id)
                )
                """
            )
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS forward_rule BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS post_rule BOOLEAN NOT NULL DEFAULT TRUE")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS scan_count INTEGER NOT NULL DEFAULT 100")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS keyword_mode TEXT NOT NULL DEFAULT 'off'")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS keyword_value TEXT NOT NULL DEFAULT ''")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS keyword_values JSONB NOT NULL DEFAULT '[]'::jsonb")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS initial_scan_pending BOOLEAN NOT NULL DEFAULT FALSE")
            cur.execute("ALTER TABLE pairs ADD COLUMN IF NOT EXISTS ads_link TEXT NOT NULL DEFAULT ''")
            cur.execute(
                """
                UPDATE pairs
                SET keyword_values = jsonb_build_array(keyword_value)
                WHERE COALESCE(keyword_value, '') <> ''
                  AND (
                    keyword_values IS NULL
                    OR keyword_values = '[]'::jsonb
                  )
                """
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
                       keyword_mode, keyword_value, keyword_values, initial_scan_pending,
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

                keyword_values = row.get("keyword_values")
                if isinstance(keyword_values, str):
                    try:
                        keyword_values = json.loads(keyword_values)
                    except Exception:
                        keyword_values = []
                if not keyword_values and row.get("keyword_value"):
                    keyword_values = [row.get("keyword_value")]

                data[user_key]["pairs"].append(
                    {
                        "pair_id": int(row["pair_id"]),
                        "source_id": row["source_id"],
                        "target_id": row["target_id"],
                        "last_processed_id": int(row["last_processed_id"] or 0),
                        "recent_sent_ids": list(recent_ids)[-RECENT_IDS_LIMIT:],
                        "forward_rule": bool(row["forward_rule"]),
                        "post_rule": bool(row["post_rule"]),
                        "scan_count": normalize_scan_count_value(row.get("scan_count", DEFAULT_SCAN_COUNT)),
                        "keyword_mode": normalize_keyword_mode(row.get("keyword_mode", "off")),
                        "keyword_values": normalize_keyword_values(keyword_values),
                        "keyword_value": normalize_keyword_values(keyword_values)[0] if normalize_keyword_values(keyword_values) else "",
                        "initial_scan_pending": bool(row.get("initial_scan_pending", False)),
                        "ads_link": (row["ads_link"] or "").strip(),
                    }
                )

    return data


def save_pair(owner_user_id: int, pair: Dict[str, Any]) -> None:
    scan_count = normalize_scan_count_value(pair.get("scan_count", DEFAULT_SCAN_COUNT))
    keyword_values = normalize_keyword_values(pair.get("keyword_values") or pair.get("keyword_value"))

    pair["scan_count"] = scan_count
    pair["keyword_mode"] = normalize_keyword_mode(pair.get("keyword_mode", "off"))
    pair["keyword_values"] = keyword_values
    pair["keyword_value"] = keyword_values[0] if keyword_values else ""
    pair["initial_scan_pending"] = bool(pair.get("initial_scan_pending", False))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pairs (
                    owner_user_id, pair_id, source_id, target_id,
                    last_processed_id, recent_sent_ids,
                    forward_rule, post_rule, scan_count,
                    keyword_mode, keyword_value, keyword_values, initial_scan_pending,
                    ads_link
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
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
                    keyword_values = EXCLUDED.keyword_values,
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
                    pair.get("keyword_mode", "off"),
                    pair.get("keyword_value", ""),
                    json.dumps(keyword_values),
                    bool(pair.get("initial_scan_pending", False)),
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
