from telethon import events

from ..config import client
from ..db import delete_pair_from_db, format_scan_count_value, save_pair
from ..models import build_pair
from ..runtime import state, cache_pair_entities, reset_pair_runtime_state
from ..services.entity_service import resolve_entity_reference, resolve_pair_entities
from ..services.keyword_service import build_keyword_setup_prompt
from ..services.pair_service import ensure_user_record, format_status, get_pair_by_id, next_pair_id
from ..services.scan_service import scan_pair
from ..utils.parsing import CommandUsageError, EntityResolutionError, parse_addpair_command_args, parse_edita_command_args
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/addpair(?:\s+.+)?$"))
async def addpair_handler(event):
    try:
        parsed = parse_addpair_command_args(event.raw_text or "")
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    record = ensure_user_record(state.account_user_id)

    if parsed["pair_id"] is None:
        pair_id = next_pair_id(record)
    else:
        pair_id = parsed["pair_id"]
        if get_pair_by_id(state.account_user_id, pair_id):
            await send_self_reply(event.chat_id, f"Pair {pair_id} already exists. Use a different pair number.")
            return

    try:
        source_entity = await resolve_entity_reference(parsed["source_id"], allow_join_via_invite=True)
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid source.\n{e}")
        return

    try:
        target_entity = await resolve_entity_reference(parsed["target_id"], allow_join_via_invite=True)
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid target.\n{e}")
        return

    pair = build_pair(
        pair_id=pair_id,
        source_id=parsed["source_id"],
        target_id=parsed["target_id"],
        scan_count=parsed["scan_count"],
    )
    record["pairs"].append(pair)
    record["pairs"].sort(key=lambda p: int(p["pair_id"]))

    save_pair(state.account_user_id, pair)
    reset_pair_runtime_state(state.account_user_id, pair_id, clear_entities=True)
    cache_pair_entities(state.account_user_id, pair_id, source_entity, target_entity)

    await send_self_reply(event.chat_id, build_keyword_setup_prompt(pair))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/listpairs$"))
async def listpairs_handler(event):
    await send_self_reply(event.chat_id, format_status(state.account_user_id))


@client.on(events.NewMessage(outgoing=True, pattern=r"^/deletepair\s+(\d+)$"))
async def deletepair_handler(event):
    pair_id = int(event.pattern_match.group(1))
    record = ensure_user_record(state.account_user_id)
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    record["pairs"] = [p for p in record["pairs"] if int(p["pair_id"]) != pair_id]
    delete_pair_from_db(state.account_user_id, pair_id)
    await send_self_reply(event.chat_id, f"Pair {pair_id} deleted.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editA(?:\s+.+)?$"))
async def edit_a_command_handler(event):
    try:
        parsed = parse_edita_command_args(event.raw_text or "")
    except CommandUsageError as e:
        await send_self_reply(event.chat_id, str(e))
        return

    pair_id = parsed["pair_id"]
    new_source = parsed["source_id"]
    new_scan_count = parsed["scan_count"]

    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return

    try:
        await resolve_entity_reference(new_source, allow_join_via_invite=True)
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid source.\n{e}")
        return

    pair["source_id"] = new_source
    pair["scan_count"] = new_scan_count
    pair["last_processed_id"] = 0
    pair["recent_sent_ids"] = []

    save_pair(state.account_user_id, pair)
    reset_pair_runtime_state(state.account_user_id, pair_id, clear_entities=True)

    resolved = await resolve_pair_entities(state.account_user_id, pair)
    if not resolved:
        await send_self_reply(
            event.chat_id,
            f"Pair {pair_id} source saved, but source/target could not be fully resolved yet.",
        )
        return

    await send_self_reply(
        event.chat_id,
        f"Pair {pair_id} Channel A updated.\n\n"
        f"New source: {new_source}\n"
        f"Scan count: {format_scan_count_value(new_scan_count)}\n"
        f"Rescanning now...",
    )

    await scan_pair(state.account_user_id, pair)


@client.on(events.NewMessage(outgoing=True, pattern=r"^/editB\s+(\d+)\s+(\S+)$"))
async def edit_b_command_handler(event):
    pair_id = int(event.pattern_match.group(1))
    new_target = event.pattern_match.group(2).strip()
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    try:
        await resolve_entity_reference(new_target, allow_join_via_invite=True)
    except EntityResolutionError as e:
        await send_self_reply(event.chat_id, f"Invalid target.\n{e}")
        return
    pair["target_id"] = new_target
    save_pair(state.account_user_id, pair)
    reset_pair_runtime_state(state.account_user_id, pair_id, clear_entities=True)
    resolved = await resolve_pair_entities(state.account_user_id, pair)
    if not resolved:
        await send_self_reply(event.chat_id, f"Could not fully resolve pair {pair_id} after update.")
        return
    await send_self_reply(event.chat_id, f"Pair {pair_id} Channel B updated to: {new_target}")
