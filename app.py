from pathlib import Path

from graphology.etl import Pipeline
from graphology.etl._helpers import now


def main():
    DATA_DIRECTORY: Path = Path("data")

    START_YEAR = 2000
    END_YEAR = 2025

    # timestamp = now()
    timestamp = "2025-05-10T17-44-35"

    pipeline = Pipeline(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
        data_directory=DATA_DIRECTORY,
    )
    pipeline.run()


if __name__ == "__main__":
    main()
