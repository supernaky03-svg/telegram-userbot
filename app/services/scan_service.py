from typing import Any, Dict, List

from ..config import LATEST_RECHECK_LIMIT, client
from ..db import get_pair_initial_scan_limit
from ..logging_setup import logger
from ..runtime import get_pair_runtime
from ..utils.filters import pair_has_pending_initial_scan
from ..utils.telethon_helpers import run_with_floodwait, safe_get_messages
from .pair_service import ensure_user_record
from .repost_service import process_album_object, process_message_object
from .entity_service import resolve_pair_entities


async def fetch_latest_message_id(source_entity) -> int:
    latest = await safe_get_messages(source_entity, limit=1)
    if latest and len(latest) > 0:
        return int(latest[0].id)
    return 0


async def scan_pair(user_id: int, pair: Dict[str, Any]) -> None:
    if pair_has_pending_initial_scan(pair):
        return

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    source_entity = runtime["source_entity"]

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
    for pair in record.get("pairs", []):
        await resolve_pair_entities(user_id, pair)
        await scan_pair(user_id, pair)
