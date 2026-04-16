import asyncio
import random
from typing import Any, Dict, List

from ..config import DELAY_MAX_SECONDS, DELAY_MIN_SECONDS
from ..db import save_pair
from ..logging_setup import logger
from ..runtime import get_pair_runtime
from ..services.pair_service import trim_recent_ids
from ..utils.filters import (
    is_video_message,
    pair_album_matches_filters,
    pair_has_pending_initial_scan,
    pair_matches_filters,
    should_skip_album_forwarded,
    should_skip_forwarded,
)
from ..utils.telethon_helpers import (
    safe_get_message,
    safe_get_messages,
    safe_send_album,
    safe_send_file,
    safe_send_message,
)
from ..utils.text import build_album_captions, build_single_caption, build_single_text
from .entity_service import resolve_pair_entities


def is_duplicate(pair: Dict[str, Any], msg_id: int) -> bool:
    return msg_id in set(pair.get("recent_sent_ids", []))


def mark_sent_ids(user_id: int, pair: Dict[str, Any], msg_ids: List[int]) -> None:
    recent = list(pair.get("recent_sent_ids", []))
    recent.extend(msg_ids)
    pair["recent_sent_ids"] = trim_recent_ids(recent)
    save_pair(user_id, pair)


def update_last_processed(user_id: int, pair: Dict[str, Any], msg_id: int) -> None:
    if msg_id > int(pair.get("last_processed_id", 0)):
        pair["last_processed_id"] = int(msg_id)
        save_pair(user_id, pair)


async def apply_human_delay() -> None:
    low = min(DELAY_MIN_SECONDS, DELAY_MAX_SECONDS)
    high = max(DELAY_MIN_SECONDS, DELAY_MAX_SECONDS)
    if high <= 0:
        return
    delay = random.randint(low, high) if high > low else high
    if delay > 0:
        logger.info("Sleeping %s seconds before send", delay)
        await asyncio.sleep(delay)


def mark_action_done(user_id: int, pair: Dict[str, Any], msg_ids: List[int]) -> None:
    mark_sent_ids(user_id, pair, msg_ids)
    update_last_processed(user_id, pair, max(msg_ids))


async def repost_single_message(user_id: int, pair: Dict[str, Any], msg) -> bool:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    target_entity = runtime["target_entity"]

    if not target_entity:
        return False

    msg_id = int(msg.id)
    if is_duplicate(pair, msg_id):
        logger.info("Skipping duplicate message %s for pair %s", msg_id, pair_id)
        update_last_processed(user_id, pair, msg_id)
        return False

    if not pair_matches_filters(pair, msg):
        logger.info("Skipping message %s for pair %s due to filters", msg_id, pair_id)
        update_last_processed(user_id, pair, msg_id)
        return False

    await apply_human_delay()

    if msg.media:
        caption = build_single_caption(pair, msg)
        await safe_send_file(target_entity, msg.media, caption=caption)
    else:
        text = build_single_text(pair, msg)
        if not text:
            logger.info("Skipping empty text message %s for pair %s", msg_id, pair_id)
            update_last_processed(user_id, pair, msg_id)
            return False
        await safe_send_message(target_entity, text)

    logger.info("Reposted single message %s for pair %s", msg_id, pair_id)
    mark_action_done(user_id, pair, [msg_id])
    return True


async def repost_album(user_id: int, pair: Dict[str, Any], album_messages: List[Any]) -> bool:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    target_entity = runtime["target_entity"]

    if not target_entity or not album_messages:
        return False

    grouped_id = getattr(album_messages[0], "grouped_id", None)
    msg_ids = [int(m.id) for m in album_messages]

    if grouped_id in runtime["last_sent_grouped_ids"]:
        logger.info("Skipping duplicate runtime album %s for pair %s", grouped_id, pair_id)
        update_last_processed(user_id, pair, max(msg_ids))
        return False

    if any(is_duplicate(pair, mid) for mid in msg_ids):
        logger.info("Skipping album %s for pair %s because one or more items already sent", grouped_id, pair_id)
        update_last_processed(user_id, pair, max(msg_ids))
        return False

    if not pair_album_matches_filters(pair, album_messages):
        logger.info("Skipping album %s for pair %s due to filters", grouped_id, pair_id)
        update_last_processed(user_id, pair, max(msg_ids))
        return False

    if pair.get("post_rule", True):
        files = []
        captions = []
        for msg, caption in zip(album_messages, build_album_captions(pair, album_messages)):
            if is_video_message(msg) and msg.media:
                files.append(msg.media)
                captions.append(caption or "")
        if not files:
            logger.info("Skipping album %s for pair %s because post_rule=ON and no video items found", grouped_id, pair_id)
            update_last_processed(user_id, pair, max(msg_ids))
            return False
    else:
        files = [m.media for m in album_messages if m.media]
        captions = build_album_captions(pair, album_messages)
        if not files:
            text_parts = [c for c in captions if c]
            text = "\n\n".join(text_parts).strip()
            if not text:
                logger.info("Skipping media-less album %s for pair %s", grouped_id, pair_id)
                update_last_processed(user_id, pair, max(msg_ids))
                return False
            await apply_human_delay()
            await safe_send_message(target_entity, text)
            runtime["last_sent_grouped_ids"].add(grouped_id)
            mark_action_done(user_id, pair, msg_ids)
            return True

    await apply_human_delay()
    await safe_send_album(target_entity, files, captions)
    if grouped_id is not None:
        runtime["last_sent_grouped_ids"].add(grouped_id)
    logger.info("Reposted album %s for pair %s", grouped_id, pair_id)
    mark_action_done(user_id, pair, msg_ids)
    return True


async def collect_grouped_album_messages(source_entity, msg) -> List[Any]:
    grouped_id = getattr(msg, "grouped_id", None)
    if not grouped_id:
        return [msg]

    batch = await safe_get_messages(source_entity, limit=30)
    items = [m for m in batch if getattr(m, "grouped_id", None) == grouped_id]
    items = sorted(items, key=lambda x: x.id)

    if msg.id not in {m.id for m in items}:
        items.append(msg)
        items = sorted(items, key=lambda x: x.id)

    return items


async def repost_preview_for_video(user_id: int, pair: Dict[str, Any], msg) -> None:
    if not pair.get("post_rule", True):
        return
    if not is_video_message(msg):
        return

    runtime = get_pair_runtime(user_id, int(pair["pair_id"]))
    source_entity = runtime["source_entity"]
    if not source_entity:
        return

    previous_id = int(msg.id) - 1
    if previous_id <= 0:
        return

    previous_msg = await safe_get_message(source_entity, previous_id)
    if not previous_msg:
        return

    if getattr(previous_msg, "grouped_id", None):
        album = await collect_grouped_album_messages(source_entity, previous_msg)
        if should_skip_album_forwarded(pair, album):
            return
        if any(is_duplicate(pair, int(m.id)) for m in album):
            return
        if not any(not is_video_message(m) for m in album):
            return
        await repost_album(user_id, pair, album)
        return

    if should_skip_forwarded(pair, previous_msg):
        return
    if is_duplicate(pair, int(previous_msg.id)):
        return
    if is_video_message(previous_msg):
        return
    await repost_single_message(user_id, pair, previous_msg)


async def process_message_object(user_id: int, pair: Dict[str, Any], msg) -> None:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    async with runtime["lock"]:
        if pair_has_pending_initial_scan(pair):
            return
        resolved = runtime["source_entity"] is not None and runtime["target_entity"] is not None
        if not resolved:
            resolved = await resolve_pair_entities(user_id, pair)
            if not resolved:
                return

        if should_skip_forwarded(pair, msg):
            update_last_processed(user_id, pair, int(msg.id))
            return

        if pair.get("post_rule", True) and is_video_message(msg):
            await repost_preview_for_video(user_id, pair, msg)

        await repost_single_message(user_id, pair, msg)


async def process_album_object(user_id: int, pair: Dict[str, Any], album_messages: List[Any]) -> None:
    if not album_messages:
        return

    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)
    album_messages = sorted(album_messages, key=lambda x: x.id)

    async with runtime["lock"]:
        if pair_has_pending_initial_scan(pair):
            return
        resolved = runtime["source_entity"] is not None and runtime["target_entity"] is not None
        if not resolved:
            resolved = await resolve_pair_entities(user_id, pair)
            if not resolved:
                return

        if should_skip_album_forwarded(pair, album_messages):
            update_last_processed(user_id, pair, max(int(m.id) for m in album_messages))
            return

        if pair.get("post_rule", True):
            first_video = next((m for m in album_messages if is_video_message(m)), None)
            if first_video:
                prev_id = int(min(m.id for m in album_messages)) - 1
                if prev_id > 0:
                    prev_msg = await safe_get_message(runtime["source_entity"], prev_id)
                    if prev_msg and not getattr(prev_msg, "grouped_id", None):
                        if not should_skip_forwarded(pair, prev_msg) and not is_video_message(prev_msg):
                            if not is_duplicate(pair, int(prev_msg.id)):
                                await repost_single_message(user_id, pair, prev_msg)

        await repost_album(user_id, pair, album_messages)
