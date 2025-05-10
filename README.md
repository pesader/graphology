# `graphology`

An analysis of scientific collaboration at Unicamp using graphs.

## Relationa Database

Set your password with

```sh
keyring set graphology-postgres admin
```

To run the database, use this command:

```sh
podman run -d \
  --name graphology \
  -e POSTGRES_PASSWORD=$(keyring get graphology admin) \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=graphology \
  -v "$HOME/Documents/repos/pfg/.storage/" \
  -p 5432:5432 \
  --rm \
  --replace \
  postgres:latest
```

If you're running it inside an interactive container (i.e. a toolbx), run:

```sh
flatpak-spawn --host podman run -d \
  --name graphology \
  -e POSTGRES_PASSWORD=$(keyring get graphology admin) \
  -e POSTGRES_USER=admin \
  -e POSTGRES_DB=graphology \
  -v "$HOME/Documents/repos/pfg/.storage/" \
  -p 5432:5432 \
  --rm \
  --replace \
  postgres:latest
```

## Graph Database

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
