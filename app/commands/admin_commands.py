from telethon import events

from ..config import client
from ..runtime import state, get_pair_runtime
from ..services.entity_service import resolve_pair_entities
from ..services.pair_service import ensure_user_record, format_status, get_pair_by_id
from ..services.poll_service import run_poll_once
from ..services.repost_service import process_album_object, process_message_object
from ..services.scan_service import scan_all_pairs, scan_pair
from ..utils.filters import pair_has_pending_initial_scan
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check$"))
async def check_all_handler(event):
    await send_self_reply(event.chat_id, "Checking all pairs...")
    await scan_all_pairs(state.account_user_id)
    await send_self_reply(event.chat_id, "Check completed.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/check\s+(\d+)$"))
async def check_pair_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    if pair_has_pending_initial_scan(pair):
        await send_self_reply(
            event.chat_id,
            f"Pair {pair_id} is waiting for keyword setup. Use /BanKeyword, /PostKeyword, or reply with no first.",
        )
        return
    resolved = await resolve_pair_entities(state.account_user_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Could not resolve pair {pair_id}.")
        return
    await send_self_reply(event.chat_id, f"Checking pair {pair_id}...")
    await scan_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"Check completed for pair {pair_id}.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/poll$"))
async def poll_now_handler(event):
    await send_self_reply(event.chat_id, "Running manual poll...")
    await run_poll_once(state.account_user_id)
    await send_self_reply(event.chat_id, "Manual poll completed.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/status$"))
async def status_handler(event):
    await send_self_reply(event.chat_id, format_status(state.account_user_id))


@client.on(events.NewMessage(incoming=True))
async def live_new_message_handler(event):
    if not state.account_user_id:
        return
    if event.out:
        return
    msg = event.message
    source_chat_id = getattr(event.chat, "id", None) or getattr(msg, "chat_id", None)
    if not source_chat_id:
        return

    record = ensure_user_record(state.account_user_id)
    for pair in record.get("pairs", []):
        if pair_has_pending_initial_scan(pair):
            continue
        runtime = get_pair_runtime(state.account_user_id, int(pair["pair_id"]))
        if runtime.get("source_chat_id") == source_chat_id and not getattr(msg, "grouped_id", None):
            await process_message_object(state.account_user_id, pair, msg)


@client.on(events.Album(incoming=True))
async def live_album_handler(event):
    if not state.account_user_id:
        return
    if event.out:
        return
    messages = list(event.messages or [])
    if not messages:
        return
    source_chat_id = getattr(event.chat, "id", None) or getattr(messages[0], "chat_id", None)
    if not source_chat_id:
        return

    record = ensure_user_record(state.account_user_id)
    for pair in record.get("pairs", []):
        if pair_has_pending_initial_scan(pair):
            continue
        runtime = get_pair_runtime(state.account_user_id, int(pair["pair_id"]))
        if runtime.get("source_chat_id") == source_chat_id:
            await process_album_object(state.account_user_id, pair, messages)
