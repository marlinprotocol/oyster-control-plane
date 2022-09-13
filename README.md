# Marlin EPCP
`epcp` is marlin's nitro enclave provider control plane software.

## Building
Build the application using:
- `make build` or `make build -B` (force) to build `build/epcp`
- `make install` to install the built `eccp` into `/usr/local/bin/epcp` bringing it into $PATH.
- `make uninstall` removes `/usr/local/bin/epcp`

## Asserting version of a built artifact
`epcp` while building is stamped with build information such as git commit, CI process, time of build etc. so as to allow identification while in production.
```
$ epcp --version
epcp version  build master@d999385
persistence version 1
compiled at 13-09-2022 07:34:34 by heraos
using go version go1.19 linux/amd64
```

## EPCP running modes
TODO

## Development database
- Install docker via `paru -S docker` (arch linux) and run it using `sudo systemctl start docker`
- Install psycopg2 using `sudo pacman -S python-psycopg2`
- Install `pgcli` for interacting with a postgres instance via cli using `pip3 install pgcli`
- Install migrate using `./scripts/setup_migrate.sh`
- Setup a pgsql docker container using `./scripts/start_db.sh`. This will generate a new `pgdata/.pgdata_XXXX` directory (XXXXX being random for each invocation) which will be used by postgresql. Every time this script is invoked, the DB is launched anew with no data.
- To start a docker container with a previous directory, invoke using `./scripts/start_db.sh a6df1` if concerned data directory is `pgdata/.pgdata_a6df1`. In this mode, db migrations are not run.

You should now have two users:
- **devuser**: Accessible via `pgcli postgresql://devuser:devpass@localhost:5432/devdb` for DB superuser access.
- **proguser**: Accessible via `pgcli postgresql://proguser:progpass@localhost:5432/devdb` for insert only access to `blocks` and `pool_actions_geth` tables.
