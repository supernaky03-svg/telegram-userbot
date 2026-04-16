from typing import Any, Dict, List

from telethon.tl.types import DocumentAttributeVideo

from ..models import KEYWORD_MODES


def normalize_keyword_mode(value: Any) -> str:
    mode = str(value or "off").strip().lower()
    if mode not in KEYWORD_MODES:
        return "off"
    return mode


def normalize_keyword_values(value: Any) -> List[str]:
    if value is None:
        return []

    raw_items: List[str] = []
    if isinstance(value, (list, tuple)):
        raw_items = [str(item) for item in value]
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        raw_items = text.split(",") if "," in text else text.split()
    else:
        raw_items = [str(value)]

    normalized: List[str] = []
    seen = set()
    for item in raw_items:
        keyword = str(item or "").strip().lower()
        if not keyword or keyword in seen:
            continue
        normalized.append(keyword)
        seen.add(keyword)
    return normalized


def get_pair_keyword_mode(pair: Dict[str, Any]) -> str:
    return normalize_keyword_mode(pair.get("keyword_mode", "off"))


def get_pair_keyword_values(pair: Dict[str, Any]) -> List[str]:
    values = pair.get("keyword_values")
    if values is None and pair.get("keyword_value"):
        values = [pair.get("keyword_value")]
    return normalize_keyword_values(values)


def get_pair_keyword_value(pair: Dict[str, Any]) -> str:
    values = get_pair_keyword_values(pair)
    return values[0] if values else ""


def format_pair_keywords(pair: Dict[str, Any]) -> str:
    return ", ".join(get_pair_keyword_values(pair))


def set_pair_keyword_state(pair: Dict[str, Any], mode: str, keywords=None) -> None:
    normalized_mode = normalize_keyword_mode(mode)
    normalized_keywords = normalize_keyword_values(keywords)
    if normalized_mode == "off":
        normalized_keywords = []
    pair["keyword_mode"] = normalized_mode
    pair["keyword_values"] = normalized_keywords
    pair["keyword_value"] = normalized_keywords[0] if normalized_keywords else ""


def pair_has_pending_initial_scan(pair: Dict[str, Any]) -> bool:
    return bool(pair.get("initial_scan_pending", False))


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


def message_text_for_filter(msg) -> str:
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    return (raw or "").strip().lower()


def pair_keyword_allows_text(pair: Dict[str, Any], text: str) -> bool:
    mode = get_pair_keyword_mode(pair)
    keywords = get_pair_keyword_values(pair)
    if mode == "off" or not keywords:
        return True
    haystack = (text or "").lower()
    matched = any(keyword in haystack for keyword in keywords) if haystack else False
    if mode == "ban":
        return not matched
    if mode == "post":
        return matched
    return True


def pair_keyword_allows_message(pair: Dict[str, Any], msg) -> bool:
    return pair_keyword_allows_text(pair, message_text_for_filter(msg))


def pair_keyword_allows_album(pair: Dict[str, Any], album_messages: List[Any]) -> bool:
    text = "\n".join(message_text_for_filter(m) for m in album_messages if m)
    return pair_keyword_allows_text(pair, text)


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
