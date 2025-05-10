from pathlib import Path
from ._constants import DATA_DIRECTORY


def raw_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "raw"


def processed_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "processed"


def merged_data_directory(timestamp: str) -> Path:
    return DATA_DIRECTORY / timestamp / "merged"
