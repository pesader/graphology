from sqlmodel import SQLModel, Field, Relationship
from datetime import datetime


class Authorship(SQLModel, table=True):
    key: int | None = Field(default=None, primary_key=True)

    author_id: str = Field(
        default=None,
        foreign_key="author.scopus_id",
    )
    document_id: str = Field(
        default=None,
        foreign_key="document.scopus_id",
    )
    institution_id: str | None = Field(
        default=None,
        foreign_key="institution.scopus_id",
    )

    first_author: bool = False


class Author(SQLModel, table=True):
    scopus_id: str = Field(primary_key=True)
    name: str = Field(index=True)

    institutions: list["Institution"] = Relationship(
        back_populates="authors",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )
    documents: list["Document"] = Relationship(
        back_populates="authors",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )


class Institution(SQLModel, table=True):
    scopus_id: str = Field(primary_key=True)
    name: str = Field(index=True)
    city: str | None = Field(default=None, index=True)
    country: str | None = Field(default=None, index=True)

    authors: list["Author"] = Relationship(
        back_populates="institutions",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )
    documents: list["Document"] = Relationship(
        back_populates="institutions",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )


class Document(SQLModel, table=True):
    scopus_id: str = Field(primary_key=True)
    eid: str = Field(index=True)
    doi: str | None = Field(default=None, index=True)
    title: str = Field(index=True)
    openaccess: bool
    date: datetime
    document_type: str
    document_type_description: str
    volume: str | None
    issue: str | None
    page: str | None
    citedby_count: int
    funding_acronym: str | None
    funding_number: str | None
    funding_name: str | None
    source_name: str
    source_type: str
    source_id: str
    source_issn: str | None
    source_eissn: str | None

    authors: list["Author"] = Relationship(
        back_populates="documents",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )
    institutions: list["Institution"] = Relationship(
        back_populates="documents",
        link_model=Authorship,
        sa_relationship_kwargs={"viewonly": True},
    )
