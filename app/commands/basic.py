from telethon import events

from ..config import client
from ..utils.telethon_helpers import send_self_reply


@client.on(events.NewMessage(outgoing=True, pattern=r"^/start$"))
async def start_command_handler(event):
    await send_self_reply(
        event.chat_id,
        "Multi-pair userbot is running.\n\nUse /help for usage.",
    )


@client.on(events.NewMessage(outgoing=True, pattern=r"^/help$"))
async def help_command_handler(event):
    await send_self_reply(
        event.chat_id,
        "Available commands:\n\n"
        "/start - short bot status message\n"
        "/help - show this help message\n"
        "/addpair [pair_number] <source> <target> [scan_count] - add a new source/target pair\n"
        "/editA <id> <new_source> [scan_count] - change source channel for a pair\n"
        "/editB <id> <new_target> - change target channel for a pair\n"
        "/BanKeyword <id> <word1,word2,...> - skip posts containing any listed keyword for that pair\n"
        "/PostKeyword <id> <word1,word2,...> - only repost posts containing any listed keyword for that pair\n"
        "/ClearKeyword <id> - clear keyword filter for that pair\n"
        "/ShowKeyword <id> - show keyword filter state for that pair\n"
        "/listpairs - show all configured pairs\n"
        "/status - show pair status summary\n"
        "/deletepair <id> - delete one pair\n"
        "/check - scan all pairs now\n"
        "/check <id> - scan one pair now\n"
        "/poll - run one manual poll cycle for all pairs\n"
        "/ads <id> <link> - set ads link for a pair\n"
        "/showads <id> - show current ads link\n"
        "/clearads <id> - clear ads link\n"
        "/forward_rule <on/off> <id> - forwarded message rule\n"
        "/post_rule <on/off> <id> - on = video + previous preview post, off = post everything\n\n"
        "Examples:\n"
        "/addpair https://t.me/source1 https://t.me/target1\n"
        "/addpair 7 https://t.me/source1 https://t.me/target1\n"
        "/addpair 8 https://t.me/+privateInviteLink https://t.me/target1 300\n"
        "/addpair 9 -1001234567890 https://t.me/target1 all\n"
        "/BanKeyword 3 spam,scam,ads\n"
        "/PostKeyword 3 movie,anime,series\n"
        "/ClearKeyword 3\n"
        "/ShowKeyword 3\n"
        "/editA 2 https://t.me/newsource\n"
        "/editA 3 https://t.me/+privateInviteLink\n"
        "/editA 4 -1001234567890 all\n"
        "/editB 2 https://t.me/newtarget\n"
        "/ads 1 https://example.com\n"
        "/forward_rule on 1\n"
        "/post_rule off 1\n"
        "/check 1\n"
        "/poll"
    )
