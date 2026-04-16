from telethon import events

from ..config import client
from ..db import save_pair
from ..runtime import state
from ..services.pair_service import get_pair_by_id
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/ads\s+(\d+)\s+(.+)$"))
async def ads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    ads_link = event.pattern_match.group(2).strip()
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["ads_link"] = ads_link
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"ads_link set for pair {pair_id}: {ads_link}")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/showads\s+(\d+)$"))
async def showads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    ads_link = pair.get("ads_link", "") or "[not set]"
    await send_self_reply(event.chat_id, f"Pair {pair_id} ads_link: {ads_link}")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/clearads\s+(\d+)$"))
async def clearads_handler(event):
    pair_id = int(event.pattern_match.group(1))
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["ads_link"] = ""
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"ads_link cleared for pair {pair_id}.")
