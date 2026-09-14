from pathlib import Path

CATALOG = "cdc_lakehouse"
SCHEMA = "bronze"
VOLUME = "raw"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

ENTITIES = ("customers", "orders", "order_items")

STATE_DIR = Path(".generator_state")
STATE_FILE = STATE_DIR / "state.json"

LOCAL_OUTPUT_DIR = Path(".generator_output")
