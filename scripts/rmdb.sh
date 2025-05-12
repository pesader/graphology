#!/bin/sh

if [ "$EUID" -ne 0 ]; then
  echo "Please, run this script with privilege"
  exit 1
fi

rm -rf /var/lib/neo4j/data/databases/neo4j && rm -rf /var/lib/neo4j/data/transactions/neo4j
