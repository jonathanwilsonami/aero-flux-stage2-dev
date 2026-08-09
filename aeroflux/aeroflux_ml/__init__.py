"""AeroFlux ML — modular, config-driven feature engineering + inference.

Train/serve parity by construction: BTS and live both map into one canonical
frame (schema), features are computed once (channels/engineer), and the model
consumes only features (inference).
"""

from .config import Config, FeatureConfig, ModelConfig, load_config
from .schema import (
    from_bts, from_silver, add_base_delays, CANONICAL_COLUMNS,
)
from .channels import CHANNELS, CHANNEL_OUTPUTS, channel, WEATHER_OBS_COLUMNS
from .engineer import FeatureEngineer
from .inference import InferenceEngine
from .weather import fetch_metar_live, fetch_metar_history
from .weather import parse_ncei_records
from .io import (
    write_table, StateRepository, InMemoryStateRepository, SqliteStateRepository,
    PostgresStateRepository, DynamoDBStateRepository, state_backend_from_env,
    LakeStore, LocalLakeStore, S3LakeStore, lake_backend_from_env,
)

__version__ = "0.1.0"

__all__ = [
    "Config", "FeatureConfig", "ModelConfig", "load_config",
    "from_bts", "from_silver", "add_base_delays", "CANONICAL_COLUMNS",
    "CHANNELS", "CHANNEL_OUTPUTS", "channel", "WEATHER_OBS_COLUMNS",
    "FeatureEngineer", "InferenceEngine",
    "fetch_metar_live", "fetch_metar_history",
    "parse_ncei_records",
    "write_table", "StateRepository", "InMemoryStateRepository",
    "SqliteStateRepository", "PostgresStateRepository", "DynamoDBStateRepository",
    "state_backend_from_env",
    "LakeStore", "LocalLakeStore", "S3LakeStore", "lake_backend_from_env",
]
