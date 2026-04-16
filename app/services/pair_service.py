from typing import Any, Dict, List, Optional

from ..config import DEFAULT_SCAN_COUNT, RECENT_IDS_LIMIT
from ..db import format_scan_count_value
from ..runtime import state
from ..utils.filters import format_pair_keywords, get_pair_keyword_mode, pair_has_pending_initial_scan


def ensure_user_record(user_id: int) -> Dict[str, Any]:
    key = str(user_id)
    if key not in state.user_data:
        state.user_data[key] = {"pairs": []}
    if "pairs" not in state.user_data[key] or not isinstance(state.user_data[key]["pairs"], list):
        state.user_data[key]["pairs"] = []
    return state.user_data[key]


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


def get_pending_initial_scan_pairs(user_id: int) -> List[Dict[str, Any]]:
    record = ensure_user_record(user_id)
    return [
        pair
        for pair in sorted(record.get("pairs", []), key=lambda item: int(item.get("pair_id", 0)))
        if pair_has_pending_initial_scan(pair)
    ]


def format_status(user_id: int) -> str:
    record = ensure_user_record(user_id)
    pairs = record.get("pairs", [])

    if not pairs:
        return "No pairs configured. Use /addpair [pair_number] <source> <target> [scan_count]"

    lines = ["Status:"]

    for pair in pairs:
        ads_text = pair.get("ads_link", "") or "[not set]"
        keywords_text = format_pair_keywords(pair) or "[none]"
        lines.append(
            f"\nPair {pair['pair_id']}"
            f"\nSource: {pair.get('source_id', '')}"
            f"\nTarget: {pair.get('target_id', '')}"
            f"\nforward_rule: {'ON' if pair.get('forward_rule', False) else 'OFF'}"
            f"\npost_rule: {'ON' if pair.get('post_rule', True) else 'OFF'}"
            f"\nscan_count: {format_scan_count_value(pair.get('scan_count', DEFAULT_SCAN_COUNT))}"
            f"\nkeyword_mode: {get_pair_keyword_mode(pair).upper()}"
            f"\nkeyword_values: {keywords_text}"
            f"\ninitial_scan_pending: {'YES' if pair_has_pending_initial_scan(pair) else 'NO'}"
            f"\nads_link: {ads_text}"
            f"\nlast_processed_id: {pair.get('last_processed_id', 0)}"
            f"\nrecent_sent_ids_count: {len(pair.get('recent_sent_ids', []))}"
        )

    return "\n".join(lines)
