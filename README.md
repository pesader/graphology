![A banner, showing the project's logo, as well authors and institutions](assets/banner.png)

# `graphology`

An analysis of scientific collaboration networks at Unicamp using graphs.

> [!IMPORTANT]
> `graphology` will be featured in a presentation at NODES 2025, Neo4j's annual
> conference.
> <img src="assets/nodes2025.jpeg" width="400">


## Good news!


## Report Summary

### Introduction

### Methods

### Implementation

### Results

## Running

### Relational Database

#### Setup

Set your password with

```sh
keyring set graphology-postgres admin
```

To run the database, use this command:

```sh
podman run -d \
  --name graphology-postgres \
  -e POSTGRES_PASSWORD=$(keyring get graphology-postgres admin) \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=postgres \
  -v "$HOME/Documents/repos/pfg/.storage/" \
  -p 5432:5432 \
  --rm \
  --replace \
  postgres:latest
```

If you're running it inside an interactive container (i.e. a toolbx), run:

```sh
flatpak-spawn --host podman run -d \
  --name graphology-postgres \
  -e POSTGRES_PASSWORD=$(keyring get graphology-postgres admin) \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=postgres \
  -v "$HOME/Documents/repos/pfg/.storage/" \
  -p 5432:5432 \
  --rm \
  --replace \
  postgres:latest
```

### Graph Database

#### Setup

These instructions are for Fedora Workstation, and based on the official
installation instructions laid out
[here](https://neo4j.com/docs/operations-manual/current/installation/linux/rpm/).

1. Install neo4j

Import the PGP key:

```sh
rpm --import https://debian.neo4j.com/neotechnology.gpg.key
```

Manually add the RPM repository:

```sh
sudo tee /etc/yum.repos.d/neo4j.repo > /dev/null <<EOF
[neo4j]
name=Neo4j RPM Repository
baseurl=https://yum.neo4j.com/stable/latest
enabled=1
gpgcheck=1
EOF
```

Install neo4j Community Edition:

```sh
sudo dnf install neo4j -y
```

2. Set initial password

```sh
sudo neo4j-admin dbms set-initial-password $YOUR_PASSWORD
```

3. Start neo4j

```sh
sudo neo4j start
```

#### Moving data to Neo4j's import directory

Neo4j requires files to be placed in a certain directory to import data from
them. For convenience, there's a small shell script that does that in
`scripts/cptsv.sh`. You can run it like so:

```sh
sudo ./scripts/cptsv.sh $TIMESTAMP
```

Where `$TIMESTAMP` is timestamp string, like "2025-05-10T17-44-35".

#### Resetting the `neo4j` database

To reset the database (clear all data in it), run:

```sh
sudo ./scripts/rmdb.sh
```

## Additional Resources

- Full report (Brazilian Portuguese)
- Poster (Brazilian Portuguese)

## License

This work is licensed under the terms of the GPLv3.
