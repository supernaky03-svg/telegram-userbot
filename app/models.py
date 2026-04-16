from typing import Any, Dict, List

PairDict = Dict[str, Any]
KEYWORD_MODES = {"off", "ban", "post"}
DEFAULT_KEYWORD_MODE = "off"


def build_pair(*, pair_id: int, source_id: str, target_id: str, scan_count: int) -> PairDict:
    return {
        "pair_id": int(pair_id),
        "source_id": source_id,
        "target_id": target_id,
        "last_processed_id": 0,
        "recent_sent_ids": [],
        "forward_rule": False,
        "post_rule": True,
        "scan_count": scan_count,
        "keyword_mode": DEFAULT_KEYWORD_MODE,
        "keyword_values": [],
        "initial_scan_pending": True,
        "ads_link": "",
    }
