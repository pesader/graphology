#!/bin/sh

if [ "$EUID" -ne 0 ]; then
  echo "Please, run this script with privilege"
  exit 1
fi

mkdir -p /var/lib/neo4j/import/$1
cp data/$1/merged/*.tsv /var/lib/neo4j/import/$1
