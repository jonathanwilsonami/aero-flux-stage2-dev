"""Configuration — plain YAML into small dataclasses.

Deliberately simple (no Jinja2/ConfigManager yet): one YAML file drives which
channels run, window sizes, the airframe key, and the model. Env-var templating
can wrap this later without changing callers. Keeping it simple now matches the
"don't over-engineer early" principle.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class ModelConfig:
    path: str = ""                 # trained model file (xgboost json/ubj)
    kind: str = "xgboost"
    version: str = "unversioned"
    target: str = "arr_del15"      # informational; inference needs only features
    # If empty, expected features are read from the model itself.
    features: list[str] = field(default_factory=list)


@dataclass
class FeatureConfig:
    feature_version: str = "1.0"
    window_minutes: int = 60
    airframe_key: str = "hex"      # 'hex' (live) or 'tail_number' (GA) or 'TAIL_NUM' via BTS adapter
    channels: dict[str, bool] = field(default_factory=lambda: {
        "flight": True, "rotation": True, "airport_state": True,
        "flow": False, "weather": False,
    })

    def enabled_channels(self) -> list[str]:
        return [name for name, on in self.channels.items() if on]


@dataclass
class Config:
    features: FeatureConfig = field(default_factory=FeatureConfig)
    model: ModelConfig = field(default_factory=ModelConfig)


def load_config(path: str | Path) -> Config:
    data: dict[str, Any] = yaml.safe_load(Path(path).read_text()) or {}
    return Config(
        features=FeatureConfig(**(data.get("features") or {})),
        model=ModelConfig(**(data.get("model") or {})),
    )
