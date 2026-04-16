from telethon import events

from ..config import client
from ..db import save_pair
from ..runtime import state
from ..services.keyword_service import start_pending_initial_scan_for_pair
from ..services.pair_service import get_pair_by_id, get_pending_initial_scan_pairs
from ..utils.filters import format_pair_keywords, get_pair_keyword_mode, pair_has_pending_initial_scan, set_pair_keyword_state
from ..utils.parsing import (
    BAN_KEYWORD_USAGE,
    CLEAR_KEYWORD_USAGE,
    POST_KEYWORD_USAGE,
    SHOW_KEYWORD_USAGE,
    CommandUsageError,
    parse_keyword_mode_command_args,
    parse_keyword_pair_only_command_args,
)
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/BanKeyword(?:\s+.+)?$"))
async def ban_keyword_handler(event):
    try:
        parsed = parse_keyword_mode_command_args(event.raw_text or "", BAN_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    keywords = parsed["keywords"]
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "ban", keywords)
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"Pair {pair_id} keyword mode: BAN | keywords: {format_pair_keywords(pair)}")

    if pair_has_pending_initial_scan(pair):
        await start_pending_initial_scan_for_pair(state.account_user_id, pair, event.chat_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/PostKeyword(?:\s+.+)?$"))
async def post_keyword_handler(event):
    try:
        parsed = parse_keyword_mode_command_args(event.raw_text or "", POST_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    keywords = parsed["keywords"]
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "post", keywords)
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"Pair {pair_id} keyword mode: POST | keywords: {format_pair_keywords(pair)}")

    if pair_has_pending_initial_scan(pair):
        await start_pending_initial_scan_for_pair(state.account_user_id, pair, event.chat_id)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ClearKeyword(?:\s+.+)?$"))
async def clear_keyword_handler(event):
    try:
        parsed = parse_keyword_pair_only_command_args(event.raw_text or "", CLEAR_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    set_pair_keyword_state(pair, "off", [])
    save_pair(state.account_user_id, pair)

    response = f"Pair {pair_id} keyword mode: OFF"
    if pair_has_pending_initial_scan(pair):
        response += "\nInitial scan is still paused. Reply with: no"
    await send_self_reply(event.chat_id, response)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ShowKeyword(?:\s+.+)?$"))
async def show_keyword_handler(event):
    try:
        parsed = parse_keyword_pair_only_command_args(event.raw_text or "", SHOW_KEYWORD_USAGE)
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    mode = get_pair_keyword_mode(pair).upper()
    keywords_text = format_pair_keywords(pair)
    response = f"Pair {pair_id} keyword mode: {mode}"
    if keywords_text:
        response += f" | keywords: {keywords_text}"
    if pair_has_pending_initial_scan(pair):
        response += "\nInitial scan pending: YES"
    await send_self_reply(event.chat_id, response)


@client.on(events.NewMessage(outgoing=True, pattern=r"^no$"))
async def no_keyword_handler(event):
    pending_pairs = get_pending_initial_scan_pairs(state.account_user_id)
    if not pending_pairs:
        await send_self_reply(event.chat_id, "No pending initial scan found.")
        return

    pair = pending_pairs[0]
    started = await start_pending_initial_scan_for_pair(state.account_user_id, pair, event.chat_id)
    if started:
        remaining = len(get_pending_initial_scan_pairs(state.account_user_id))
        if remaining > 0:
            await send_self_reply(
                event.chat_id,
                f"There {'is' if remaining == 1 else 'are'} still {remaining} pending pair{'s' if remaining != 1 else ''}.",
            )
