import asyncio
import time

from .config import client
from .logging_setup import logger
from .commands import register_handlers  # noqa: F401
from .web.health import keep_alive
from .services.entity_service import resolve_all_pairs
from .services.scan_service import scan_pair
from .services.poll_service import poll_new_updates_loop
from .services.pair_service import ensure_user_record
from .runtime import state, replace_user_data
from .db import init_db, load_all_user_data
from telethon.errors import PasswordHashInvalidError, SessionPasswordNeededError


async def login_flow() -> None:
    if await client.is_user_authorized():
        logger.info("Using existing authorized session.")
        return

    logger.info("No valid session found. Starting login flow.")
    phone = input("Enter your phone number (with country code, e.g. +959...): ").strip()
    await client.send_code_request(phone)
    code = input("Enter the login code you received: ").strip()

    try:
        await client.sign_in(phone=phone, code=code)
    except SessionPasswordNeededError:
        logger.info("2FA password required.")
        while True:
            password = input("Enter your 2FA password: ").strip()
            try:
                await client.sign_in(password=password)
                break
            except PasswordHashInvalidError:
                print("Wrong password. Try again.")


async def initialize_runtime() -> None:
    init_db()
    replace_user_data(load_all_user_data())

    me = await client.get_me()
    if not state.account_user_id:
        state.account_user_id = me.id
    logger.info("Logged in as user_id=%s | account_user_id=%s", me.id, state.account_user_id)

    ensure_user_record(state.account_user_id)
    await resolve_all_pairs(state.account_user_id)

    record = ensure_user_record(state.account_user_id)
    if record["pairs"]:
        for pair in record["pairs"]:
            await scan_pair(state.account_user_id, pair)
    else:
        logger.info("No pairs configured yet. Use /addpair <source> <target>")


async def main() -> None:
    logger.info("Bot startup initiated.")
    await client.connect()
    keep_alive()
    await login_flow()
    await initialize_runtime()
    asyncio.create_task(poll_new_updates_loop())
    logger.info("User-bot is now running.")
    print("User-bot is running. Use /start in Saved Messages.")
    await client.run_until_disconnected()


def run_forever() -> None:
    while True:
        try:
            asyncio.run(main())
            break
        except KeyboardInterrupt:
            logger.info("Bot stopped by user.")
            break
        except Exception:
            logger.exception("Fatal error in main loop. Restarting in 5 seconds...")
            time.sleep(5)
