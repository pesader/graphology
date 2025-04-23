# NOTE: for SQLModel.create_all() to work, SQLModel and all models need to
#       be imported before the database engine object. Importing them here
#       guarantees that happens, as recommended by the documentation:
#       https://sqlmodel.tiangolo.com/tutorial/create-db-and-table/#calling-create_all


import keyring
from sqlalchemy.engine import URL
from sqlmodel import SQLModel, create_engine

from graphology.database.relational.entities import (
    Author,
    Document,
    Institution,
    Authorship,
)

DATABASE_NAME = "graphology"

# Infrastructure
DBMS_NAME = "postgresql"
DBMS_DRIVER = "psycopg2"

# Credentials
HOST = "localhost"
USERNAME = "admin"
PASSWORD = keyring.get_password(DATABASE_NAME, USERNAME)

url_kwargs = {
    "drivername": f"{DBMS_NAME}+{DBMS_DRIVER}",
    "username": USERNAME,
    "password": PASSWORD,
    "host": HOST,
    "database": DATABASE_NAME,
}

engine_kwargs = {
    "echo": False,
    "executemany_mode": "values_plus_batch",
    "insertmanyvalues_page_size": 10000,
    "executemany_batch_page_size": 2000,
}

DATABASE_URL = URL.create(**url_kwargs)
engine = create_engine(DATABASE_URL, **engine_kwargs)


def init_db():
    SQLModel.metadata.create_all(engine)
