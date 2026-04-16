from telethon import events

from ..config import client
from ..db import save_pair
from ..runtime import state
from ..services.pair_service import get_pair_by_id
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/forward_rule\s+(on|off)\s+(\d+)$"))
async def forward_rule_handler(event):
    new_value = event.pattern_match.group(1).lower() == "on"
    pair_id = int(event.pattern_match.group(2))
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["forward_rule"] = new_value
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"forward_rule for pair {pair_id} is now {'ON' if pair['forward_rule'] else 'OFF' }.")


@client.on(events.NewMessage(outgoing=True, pattern=r"^/post_rule\s+(on|off)\s+(\d+)$"))
async def post_rule_handler(event):
    new_value = event.pattern_match.group(1).lower() == "on"
    pair_id = int(event.pattern_match.group(2))
    pair = get_pair_by_id(state.account_user_id, pair_id)
    if not pair:
        await send_self_reply(event.chat_id, f"Pair {pair_id} not found.")
        return
    pair["post_rule"] = new_value
    save_pair(state.account_user_id, pair)
    await send_self_reply(event.chat_id, f"post_rule for pair {pair_id} is now {'ON' if pair['post_rule'] else 'OFF' }.")
