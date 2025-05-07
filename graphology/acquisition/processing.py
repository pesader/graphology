import pandas as pd
from pathlib import Path

DATE: str = "2025-04-22T10-01-23"
DATA_DIRECTORY: Path = Path("data")
RESULTS_DIRECTORY: Path = DATA_DIRECTORY / Path(DATE) / Path("processed")
PROCESSED_DATA_DIRECTORY: Path = DATA_DIRECTORY / Path(DATE) / Path("merged")

# Ensure directories exists
PROCESSED_DATA_DIRECTORY.mkdir(parents=True, exist_ok=True)

TABLE_PREFIXES = ["documents", "affiliations", "authors", "authorships"]


def main():
    for prefix in TABLE_PREFIXES:
        # Find all matching authorship files
        tsv_files = sorted(RESULTS_DIRECTORY.glob(f"{prefix}_*.tsv"))

        # Load and concatenate all files
        df = pd.concat(
            (pd.read_csv(f, sep="\t", dtype=str) for f in tsv_files), ignore_index=True
        )

        if prefix == "authorships":
            df["affiliations"] = df["affiliations"].str.split(",")
            df = df.explode("affiliations").reset_index(drop=True)
            df = df.rename(columns={"affiliations": "affiliation_id"})

        # Save to a single merged file
        df.to_csv(PROCESSED_DATA_DIRECTORY / f"{prefix}.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
