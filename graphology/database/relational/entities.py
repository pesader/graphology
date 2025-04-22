from sqlmodel import SQLModel, Field, UniqueConstraint
from datetime import datetime


class Author(SQLModel):
    id: int | None = Field(default=None, primary_key=True)
    scopus_id: int = Field(unique=True)  # "author_id" in the .tsv files
    name: str = Field(index=True)


class Institution(SQLModel):
    id: int | None = Field(default=None, primary_key=True)
    scopus_id: int = Field(unique=True, index=True)
    name: str = Field(index=True)
    city: str
    country: str


class Document(SQLModel):
    id: int | None = Field(default=None, primary_key=True)
    eid: str = Field(unique=True, index=True)
    doi: str = Field(unique=True, index=True)
    title: str = Field(index=True)
    openaccess: bool
    date: datetime
    document_type: str
    document_type_description: str
    first_author: str
    volume: str
    issue: str
    page: str
    citedby_count: int
    funding_acronym: str
    funding_number: str
    funding_name: str
    source_name: str
    source_type: str
    source_id: str
    source_issn: str
    source_eissn: str


class Authorship(SQLModel):
    __table_args__ = (
        UniqueConstraint(
            "author_id",
            "institution_id",
            "document_id",
        ),
    )
    author_id: int = Field(default=None, foreign_key="author.id")
    institution_id: int = Field(default=None, foreign_key="institution.id")
    document_id: int = Field(default=None, foreign_key="document.id")
    first_author: bool = False
