from __future__ import annotations

"""
Config + threshold helpers (modeled after NotazoSystemAlerts).

Pipeline:
1) Config load (YAML → Python dict)
   - config() reads `config/config.yaml` and caches it (LRU cache).
2) Threshold access helpers (centralized)
   - get_thresholds(alert_key) pulls cfg['alerts'][alert_key]['thresholds']
   - threshold_mode(alert_key) pulls cfg['alerts'][alert_key]['thresholds']['mode']
"""

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"


@lru_cache(maxsize=1)
def config() -> Dict[str, Any]:
    """Load `config/config.yaml` once per process."""
    if not CONFIG_PATH.exists():
        return {}
    raw = CONFIG_PATH.read_text(encoding="utf-8")
    data = yaml.safe_load(raw)
    return data or {}


def get_thresholds(alert_key: str) -> Dict[str, Any]:
    """Return the thresholds block for a given alert key."""
    cfg = config()
    return (
        cfg.get("alerts", {})
        .get(alert_key, {})
        .get("thresholds", {})
        or {}
    )


def threshold_mode(alert_key: str, default: str = "static") -> str:
    """Return thresholds.mode for an alert key (e.g. 'static' or 'dynamic')."""
    thresholds = get_thresholds(alert_key)
    mode = thresholds.get("mode")
    return str(mode) if mode is not None else default


def get_threshold_value(alert_key: str, name: str, default: Any = None) -> Any:
    """
    Convenience helper:
    Looks up thresholds[mode][name] using the configured mode.
    """
    thresholds = get_thresholds(alert_key)
    mode = threshold_mode(alert_key)
    mode_cfg = thresholds.get(mode) or {}
    return mode_cfg.get(name, default)

