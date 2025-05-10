from graphology.etl import Extractor, Transformer, RDBMSLoader


def main():
    START_YEAR = 2020
    END_YEAR = 2021

    extractor = Extractor(
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    timestamp: str = extractor.timestamp
    extractor.fetch()

    transformer = Transformer(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    transformer.process()
    transformer.merge()

    loader = RDBMSLoader(timestamp)
    loader.populate()


if __name__ == "__main__":
    main()
