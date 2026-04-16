import re
from typing import Any, Dict, List, Optional

from ..config import DEFAULT_SCAN_COUNT, INVITE_LINK_RE, SCAN_COUNT_ALL, TG_JOIN_INVITE_RE


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
BAN_KEYWORD_USAGE = "Usage: /BanKeyword <pair_id> <word1,word2,...>"
POST_KEYWORD_USAGE = "Usage: /PostKeyword <pair_id> <word1,word2,...>"
CLEAR_KEYWORD_USAGE = "Usage: /ClearKeyword <pair_id>"
SHOW_KEYWORD_USAGE = "Usage: /ShowKeyword <pair_id>"


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


def split_keyword_input(keyword_text: str) -> List[str]:
    text = (keyword_text or "").strip()
    if not text:
        return []
    items = text.split(",") if "," in text else text.split()

    normalized: List[str] = []
    seen = set()
    for item in items:
        value = item.strip().lower()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def parse_keyword_mode_command_args(text: str, usage: str) -> Dict[str, Any]:
    parts = (text or "").split()
    args = parts[1:]
    if len(args) < 2:
        raise CommandUsageError("Missing required arguments.\n" + usage)
    if not re.fullmatch(r"\d+", args[0]):
        raise CommandUsageError("Invalid pair id.\n" + usage)
    pair_id = int(args[0])
    if pair_id <= 0:
        raise CommandUsageError("Invalid pair id.\n" + usage)
    keywords_text = " ".join(args[1:]).strip()
    keywords = split_keyword_input(keywords_text)
    if not keywords:
        raise CommandUsageError("Missing keyword word.\n" + usage)
    return {"pair_id": pair_id, "keywords": keywords}


def parse_keyword_pair_only_command_args(text: str, usage: str) -> Dict[str, Any]:
    parts = (text or "").split()
    args = parts[1:]
    if len(args) != 1:
        raise CommandUsageError(usage)
    if not re.fullmatch(r"\d+", args[0]):
        raise CommandUsageError("Invalid pair id.\n" + usage)
    pair_id = int(args[0])
    if pair_id <= 0:
        raise CommandUsageError("Invalid pair id.\n" + usage)
    return {"pair_id": pair_id}
