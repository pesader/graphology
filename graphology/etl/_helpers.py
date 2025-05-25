from datetime import datetime
from pathlib import Path
from ._constants import DATA_DIRECTORY


def now() -> str:
    timestamp: str = datetime.now().isoformat(timespec="seconds").replace(":", "-")
    return timestamp


def raw_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "raw"


def processed_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "processed"


def merged_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "merged"


def neo4j_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "neo4j"
