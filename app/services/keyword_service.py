from typing import Dict

from ..config import DEFAULT_SCAN_COUNT
from ..db import format_scan_count_value, save_pair
from ..utils.filters import format_pair_keywords, get_pair_keyword_mode, pair_has_pending_initial_scan
from ..utils.telethon_helpers import send_self_reply
from .entity_service import resolve_pair_entities
from .scan_service import scan_pair


def build_keyword_setup_prompt(pair: Dict) -> str:
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
        f"/BanKeyword {pair_id} <word1,word2,...>\n"
        f"/PostKeyword {pair_id} <word1,word2,...>"
    )


async def start_pending_initial_scan_for_pair(user_id: int, pair: Dict, chat_id: int) -> bool:
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
    if get_pair_keyword_mode(pair) != "off":
        keywords_text = format_pair_keywords(pair)
        if keywords_text:
            keyword_suffix = f" | keywords: {keywords_text}"

    await send_self_reply(
        chat_id,
        f"Starting initial scan for pair {pair_id}.\n"
        f"Keyword mode: {get_pair_keyword_mode(pair).upper()}{keyword_suffix}"
    )
    await scan_pair(user_id, pair)
    return True
