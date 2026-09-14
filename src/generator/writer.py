"""Escreve os eventos gerados como JSON Lines, particionado por data, no Volume
do Unity Catalog (ou localmente em modo --dry-run). Fronteira de I/O."""

import io
import json
from datetime import date

from databricks.sdk import WorkspaceClient

from . import config


def _to_jsonl_bytes(events: list[dict]) -> bytes:
    lines = (json.dumps(event, ensure_ascii=False) for event in events)
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_events(
    entity: str,
    events: list[dict],
    run_id: str,
    run_date: date,
    *,
    dry_run: bool = False,
) -> str | None:
    """Grava um arquivo `<entity>_<run_id>.json` sob `<entity>/date=YYYY-MM-DD/`.
    Retorna o caminho gravado, ou None se nao havia eventos.
    """
    if not events:
        return None

    partition = f"date={run_date.isoformat()}"
    filename = f"{entity}_{run_id}.json"
    payload = _to_jsonl_bytes(events)

    if dry_run:
        out_dir = config.LOCAL_OUTPUT_DIR / entity / partition
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename
        out_path.write_bytes(payload)
        return str(out_path)

    volume_path = f"{config.VOLUME_ROOT}/{entity}/{partition}/{filename}"
    w = WorkspaceClient()
    w.files.upload(volume_path, io.BytesIO(payload), overwrite=True)
    return volume_path
