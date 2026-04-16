import asyncio

from ..config import POLL_INTERVAL_SECONDS
from ..logging_setup import logger
from ..runtime import state
from .pair_service import ensure_user_record
from .scan_service import scan_pair
from .entity_service import resolve_pair_entities


async def run_poll_once(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record.get("pairs", []):
        await resolve_pair_entities(user_id, pair)
        await scan_pair(user_id, pair)


async def poll_new_updates_loop() -> None:
    while True:
        try:
            if state.account_user_id:
                await run_poll_once(state.account_user_id)
        except Exception:
            logger.exception("Polling loop error")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
