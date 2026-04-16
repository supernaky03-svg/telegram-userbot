import asyncio

from telethon.errors import FloodWaitError

from ..config import client
from ..logging_setup import logger


async def run_with_floodwait(coro_factory, *args, **kwargs):
    while True:
        try:
            return await coro_factory(*args, **kwargs)
        except FloodWaitError as e:
            seconds = int(getattr(e, "seconds", 0))
            logger.warning("FloodWait detected. Sleeping %s seconds.", seconds)
            await asyncio.sleep(seconds + 1)


async def safe_get_entity(entity_like):
    return await run_with_floodwait(client.get_entity, entity_like)


async def safe_get_message(chat, msg_id: int):
    return await run_with_floodwait(client.get_messages, chat, ids=msg_id)


async def safe_get_messages(chat, limit: int):
    return await run_with_floodwait(client.get_messages, chat, limit=limit)


async def safe_send_message(chat, message: str):
    return await run_with_floodwait(client.send_message, chat, message=message)


async def safe_send_file(chat, file, caption=None, force_document=False):
    return await run_with_floodwait(
        client.send_file,
        chat,
        file=file,
        caption=caption,
        force_document=force_document,
    )


async def safe_send_album(chat, files, captions=None):
    return await run_with_floodwait(client.send_file, chat, file=files, caption=captions)


async def send_self_reply(chat_id: int, text: str) -> None:
    try:
        await safe_send_message(chat_id, text)
    except Exception:
        logger.exception("Failed to send self reply")
