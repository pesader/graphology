from graphology.etl import (
    Extractor,
    RDBMSLoader,
    GDBMSLoader,
)
from graphology.etl.transform.transformer import GDBMSTransformer, RDBMSTransformer


def main():
    START_YEAR = 2000
    END_YEAR = 2025

    # Extract the data
    extractor = Extractor(
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    timestamp: str = extractor.timestamp
    extractor.extract()

    # Transform and load into RDBMS
    rdbms_transformer = RDBMSTransformer(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    rdbms_transformer.transform()
    loader = RDBMSLoader(timestamp)
    loader.load()

    # Transform and load into GDBMS
    gdbms_transformer = GDBMSTransformer(
        timestamp=timestamp,
        start_year=START_YEAR,
        end_year=END_YEAR,
    )
    gdbms_transformer.transform()
    loader = GDBMSLoader(timestamp)
    loader.load()


if __name__ == "__main__":
    main()
