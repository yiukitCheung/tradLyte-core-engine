"""Pytest path setup for cloud package imports."""

import sys
from pathlib import Path

CLOUD_ROOT = Path(__file__).resolve().parents[1]
SHARED_ROOT = CLOUD_ROOT / "shared"
BATCH_ROOT = CLOUD_ROOT / "batch_layer"

for path in (str(CLOUD_ROOT), str(SHARED_ROOT), str(BATCH_ROOT)):
    if path not in sys.path:
        sys.path.insert(0, path)
