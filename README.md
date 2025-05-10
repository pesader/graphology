# `graphology`

An analysis of scientific collaboration at Unicamp using graphs.

## Relationa Database

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
