from pathlib import Path

from graphology.etl import (
    Extractor,
    RDBMSTransformer,
    GDBMSTransformer,
    RDBMSLoader,
    GDBMSLoader,
)


class Pipeline:
    def __init__(
        self,
        timestamp: str,
        start_year: int,
        end_year: int,
        data_directory: Path,
    ) -> None:
        self.timestamp = timestamp
        self.start_year = start_year
        self.end_year = end_year
        self.data_directory = data_directory

    def run(self):
        # Extract the data
        extractor = Extractor(
            timestamp=self.timestamp,
            start_year=self.start_year,
            end_year=self.end_year,
            data_directory=self.data_directory,
        )
        extractor.extract()

        # Transform and load into RDBMS
        rdbms_transformer = RDBMSTransformer(
            timestamp=self.timestamp,
            start_year=self.start_year,
            end_year=self.end_year,
            data_directory=self.data_directory,
        )
        rdbms_transformer.transform()
        rdbms_loader = RDBMSLoader(
            timestamp=self.timestamp,
            start_year=self.start_year,
            end_year=self.end_year,
            data_directory=self.data_directory,
        )
        rdbms_loader.load()

        # Transform and load into GDBMS
        gdbms_transformer = GDBMSTransformer(
            timestamp=self.timestamp,
            start_year=self.start_year,
            end_year=self.end_year,
            data_directory=self.data_directory,
        )
        gdbms_transformer.transform()
        gdbms_loader = GDBMSLoader(
            timestamp=self.timestamp,
            start_year=self.start_year,
            end_year=self.end_year,
            data_directory=self.data_directory,
        )
        gdbms_loader.load()
