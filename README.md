# dap_project - nypd_analysis

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Docker

Please ensure you have Docker installed on your system. If you want to run it using your own installations of MongoDB or PostgresSQL whether on docker or not then you will have to change the `mongodb_connection_string` or `postgres_connection_string` in `nypd_analysis/nypd_analysis/assets.py`

If you do not hav your own installations leave those strings unchanged and run the following commands.

### Make sure Docker is running 
If you have a fresh install of Docker and DO NOT HAVE POSTGRES INSTALLED, then follow the following steps:
```bash
cd nypd_analysis/nypd_analysis/postgres
docker-compose up -d
```

If you have a fresh install of Docker and DO NOT HAVE MONGODB INSTALLED, then follow the following steps:
```bash
cd nypd_analysis/nypd_analysis/mongodb
docker-compose up -d
```

If you have a fresh install of Docker and BUT HAVE POSTGRES INSTALLED, then you will have to kill all the running postgres processes on port:5432 (on UNIX or LINUX) using the following steps:
```bash
sudo lsof -i tcp:5432
# PROCESS_ID is the ID of the processes shown in the previous command, if there are no processes running, skip the next two commands
sudo kill -9 ${PROCESS_ID}
# make sure all processes are deleted, the following command should not return anything
sudo lsof -i tcp:5432
# create a postgres server on docker
cd nypd_analysis/nypd_analysis/postgres
docker-compose up -d
```

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
cd nypd_analysis
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

All the code for the assets created is in `nypd_analysis/nypd_analysis/assets.py`. The assets are automatically loaded into the Dagster.

You have to open the UI and click on `Materialize All`

