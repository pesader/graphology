import keyring
from neo4j import GraphDatabase

PROJECT_NAME = "graphology"
GDBMS_NAME = "neo4j"
DATABASE_NAME = GDBMS_NAME
DATABASE_URL = "bolt://localhost:7687"

# Credentials
USERNAME = GDBMS_NAME
KEYRING_ENTRY = f"{PROJECT_NAME}-{GDBMS_NAME}"
PASSWORD = keyring.get_password(KEYRING_ENTRY, USERNAME)

driver = GraphDatabase.driver(DATABASE_URL, auth=(USERNAME, PASSWORD))
