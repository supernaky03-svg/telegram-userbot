import re
from typing import Any, Dict

from telethon.errors import (
    InviteHashEmptyError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    InviteRequestSentError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.messages import ImportChatInviteRequest

from ..config import client
from ..logging_setup import logger
from ..runtime import cache_pair_entities, get_pair_runtime
from ..services.pair_service import ensure_user_record
from ..utils.parsing import EntityResolutionError, extract_invite_hash
from ..utils.telethon_helpers import run_with_floodwait, safe_get_entity


async def resolve_entity_from_numeric_id(raw_id: str):
    entity_id = int(raw_id)

    try:
        return await safe_get_entity(entity_id)
    except Exception:
        pass

    try:
        dialogs = await run_with_floodwait(client.get_dialogs, limit=None)
        wanted_id = abs(entity_id)

        for dialog in dialogs:
            entity = getattr(dialog, "entity", None)
            if entity and getattr(entity, "id", None) == wanted_id:
                return entity
    except Exception:
        logger.exception("Failed dialog-based entity lookup for %s", raw_id)

    raise EntityResolutionError(
        "Channel ID could not be resolved. Make sure this account can access that chat."
    )


async def resolve_entity_reference(raw_ref: str, *, allow_join_via_invite: bool = True):
    ref = (raw_ref or "").strip()
    if not ref:
        raise EntityResolutionError("Empty channel reference.")

    if re.fullmatch(r"-?\d+", ref):
        return await resolve_entity_from_numeric_id(ref)

    invite_hash = extract_invite_hash(ref)
    if invite_hash:
        try:
            return await safe_get_entity(ref)
        except Exception:
            if not allow_join_via_invite:
                raise EntityResolutionError("Private invite link could not be resolved.")

            try:
                result = await run_with_floodwait(client, ImportChatInviteRequest(invite_hash))
                chats = list(getattr(result, "chats", []) or [])
                if chats:
                    return chats[0]
            except UserAlreadyParticipantError:
                pass
            except InviteRequestSentError as e:
                raise EntityResolutionError(
                    "Join request was sent for this invite link. Approve it first, then try again."
                ) from e
            except (InviteHashEmptyError, InviteHashExpiredError, InviteHashInvalidError) as e:
                raise EntityResolutionError("Invite link is invalid or expired.") from e
            except Exception as e:
                raise EntityResolutionError(
                    "Could not join or resolve this private invite link."
                ) from e

            try:
                return await safe_get_entity(ref)
            except Exception as e:
                raise EntityResolutionError("Private invite link could not be resolved.") from e

    try:
        return await safe_get_entity(ref)
    except Exception as e:
        raise EntityResolutionError(
            "Channel link, username, or ID could not be resolved."
        ) from e


async def resolve_pair_entities(user_id: int, pair: Dict[str, Any]) -> bool:
    pair_id = int(pair["pair_id"])
    runtime = get_pair_runtime(user_id, pair_id)

    source_id = (pair.get("source_id") or "").strip()
    target_id = (pair.get("target_id") or "").strip()

    if not source_id or not target_id:
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None
        return False

    try:
        source_entity = await resolve_entity_reference(source_id, allow_join_via_invite=True)
        target_entity = await resolve_entity_reference(target_id, allow_join_via_invite=True)
        cache_pair_entities(user_id, pair_id, source_entity, target_entity)
        logger.info("Resolved pair %s | source=%s | target=%s", pair_id, source_id, target_id)
        return True
    except Exception:
        logger.exception("Failed to resolve entities for pair %s", pair_id)
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None
        return False


async def resolve_all_pairs(user_id: int) -> None:
    record = ensure_user_record(user_id)
    for pair in record.get("pairs", []):
        await resolve_pair_entities(user_id, pair)
