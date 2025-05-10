from graphology import Fetcher, Processor
from graphology.database.relational.loader import RDBMSLoader


def main():
    START_YEAR = 2020
    END_YEAR = 2021

    fetcher = Fetcher(
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    timestamp: str = fetcher.fetch()

    processor = Processor(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    processor.process()
    processor.merge()

    loader = RDBMSLoader(timestamp)
    loader.load()


if __name__ == "__main__":
    main()
