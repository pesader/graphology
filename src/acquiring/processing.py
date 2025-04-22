import pandas as pd
from pathlib import Path

DATA_DIRECTORY: Path = Path("data")
RESULTS_DIRECTORY: Path = DATA_DIRECTORY / Path("2025-04-15T23-22-45") / Path("raw")
PROCESSED_DATA_DIRECTORY: Path = (
    DATA_DIRECTORY / Path("2025-04-15T23-22-45") / Path("processed")
)

TABLE_PREFIXES = ["affiliations", "authorships", "documents"]

for prefix in TABLE_PREFIXES:
    # Find all matching authorship files
    tsv_files = sorted(RESULTS_DIRECTORY.glob(f"{prefix}*.tsv"))

    # Load and concatenate all files
    df = pd.concat(
        (pd.read_csv(f, sep="\t", dtype=str) for f in tsv_files), ignore_index=True
    )

    if prefix == "authorships":
        df["affiliation"] = df["affiliations"].str.split(",")
        df = df.explode("affiliations").reset_index(drop=True)

    # Save to a single merged file
    df.to_csv(PROCESSED_DATA_DIRECTORY / f"{prefix}.tsv", sep="\t", index=False)
