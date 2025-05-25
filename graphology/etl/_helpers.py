from datetime import datetime
from pathlib import Path


def now() -> str:
    timestamp: str = datetime.now().isoformat(timespec="seconds").replace(":", "-")
    return timestamp


def output_directory_name(
    timestamp: str,
    start_year: int,
    end_year: int,
):
    return f"{timestamp}_{start_year}-{end_year}"


def output_directory_path(
    timestamp: str,
    start_year: int,
    end_year: int,
    data_directory: Path,
):
    return data_directory / output_directory_name(timestamp, start_year, end_year)


def raw_data_directory_path(
    timestamp: str,
    start_year: int,
    end_year: int,
    data_directory: Path,
) -> Path:
    return (
        output_directory_path(timestamp, start_year, end_year, data_directory) / "raw"
    )


def processed_data_directory(
    timestamp: str,
    start_year: int,
    end_year: int,
    data_directory: Path,
) -> Path:
    return (
        output_directory_path(timestamp, start_year, end_year, data_directory)
        / "processed"
    )


def merged_data_directory(
    timestamp: str,
    start_year: int,
    end_year: int,
    data_directory: Path,
) -> Path:
    return (
        output_directory_path(timestamp, start_year, end_year, data_directory)
        / "merged"
    )


def neo4j_data_directory(
    timestamp: str,
    start_year: int,
    end_year: int,
    data_directory: Path,
) -> Path:
    return (
        output_directory_path(timestamp, start_year, end_year, data_directory) / "neo4j"
    )


def is_empty(directory: Path):
    return not any(directory.iterdir())
