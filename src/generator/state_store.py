"""Persistencia local do estado do gerador (fronteira de I/O)."""

import json
from pathlib import Path

from src.transforms.cdc_entities import GeneratorState

from . import config


def load_state(path: Path = config.STATE_FILE) -> GeneratorState | None:
    if not path.exists():
        return None
    return GeneratorState.from_dict(json.loads(path.read_text(encoding="utf-8")))


def save_state(state: GeneratorState, path: Path = config.STATE_FILE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
