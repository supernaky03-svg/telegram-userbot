
import re
from typing import Any, Dict, List, Optional

from .filters import get_ads_link, is_video_message
from ..config import URL_REGEX


def clean_and_add_ads(text: str, ads_text: str = None) -> str:
    if not text:
        return ""

    link_pattern = r'(https?://\S+|www\.\S+|(?:https?://)?(?:t\.me|telegram\.me)/\S+|tg://\S+)'
    username_pattern = r'(?<!\w)@([A-Za-z0-9_]{3,})'

    cleaned = re.sub(link_pattern, '', text)
    cleaned = re.sub(username_pattern, '', cleaned)
    cleaned = re.sub(r'[ 	]{2,}', ' ', cleaned)
    cleaned = re.sub(r' *\n *', '\n', cleaned)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = cleaned.strip()

    if ads_text:
        ads_text = ads_text.strip()
        if cleaned:
            return f"{cleaned}\n\n{ads_text}"
        return ads_text

    return cleaned


def clean_urls(text: Optional[str]) -> str:
    if not text:
        return ""
    cleaned = URL_REGEX.sub("", text)
    cleaned = re.sub(r"\(\s*\)", "", cleaned)
    cleaned = re.sub(r"\[\s*\]", "", cleaned)
    cleaned = re.sub(r"<\s*>", "", cleaned)
    cleaned = re.sub(r"[ 	]{2,}", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def get_clean_text_from_message(msg) -> str:
    raw = getattr(msg, "raw_text", None) or getattr(msg, "message", None) or ""
    return clean_urls(raw)


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
