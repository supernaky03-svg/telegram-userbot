import asyncio
from types import SimpleNamespace
from typing import Any, Dict

state = SimpleNamespace(
    account_user_id=0,
    user_data={},
    runtime_cache={},
)


def replace_user_data(new_data: Dict[str, Dict[str, Any]]) -> None:
    state.user_data.clear()
    state.user_data.update(new_data)


def get_pair_runtime(user_id: int, pair_id: int) -> Dict[str, Any]:
    user_key = str(user_id)
    if user_key not in state.runtime_cache:
        state.runtime_cache[user_key] = {}
    pair_key = str(pair_id)
    if pair_key not in state.runtime_cache[user_key]:
        state.runtime_cache[user_key][pair_key] = {
            "lock": asyncio.Lock(),
            "source_entity": None,
            "target_entity": None,
            "source_chat_id": None,
            "target_chat_id": None,
            "last_sent_grouped_ids": set(),
        }
    return state.runtime_cache[user_key][pair_key]


def reset_pair_runtime_state(user_id: int, pair_id: int, *, clear_entities: bool = False) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["last_sent_grouped_ids"] = set()
    if clear_entities:
        runtime["source_entity"] = None
        runtime["target_entity"] = None
        runtime["source_chat_id"] = None
        runtime["target_chat_id"] = None


def cache_pair_entities(user_id: int, pair_id: int, source_entity, target_entity) -> None:
    runtime = get_pair_runtime(user_id, pair_id)
    runtime["source_entity"] = source_entity
    runtime["target_entity"] = target_entity
    runtime["source_chat_id"] = getattr(source_entity, "id", None) if source_entity else None
    runtime["target_chat_id"] = getattr(target_entity, "id", None) if target_entity else None
