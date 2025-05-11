from graphology.etl import Extractor, Transformer, RDBMSLoader


def main():
    START_YEAR = 2000
    END_YEAR = 2025

    extractor = Extractor(
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    timestamp: str = extractor.timestamp
    extractor.extract()

    transformer = Transformer(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    transformer.transform()

    loader = RDBMSLoader(timestamp)
    loader.load()


if __name__ == "__main__":
    main()
