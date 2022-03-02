# Meilleurs Agents's BI test
BI repository about the technical test for candidates during the hiring process

## Setup requirements
1) Install [Docker Community Edition (CE)](https://docs.docker.com/engine/install/) on your workstation. Depending on the OS, you may need to configure your Docker instance to use 4.00 GB of memory for all containers to run properly. Please refer to the Resources section if using [Docker for Windows](https://docs.docker.com/desktop/windows/) or [Docker for Mac](https://docs.docker.com/desktop/mac/) for more information.
2) Install [Docker Compose](https://docs.docker.com/compose/install/) v1.29.1 and newer on your workstation.

*Default amount of memory available for Docker on MacOS is often not enough to get Airflow up and running. If enough memory is not allocated, it might lead to airflow webserver continuously restarting. You should at least allocate 4GB memory for the Docker Engine (ideally 8GB). You can check and change the amount of memory in Resources*

**You can also check if you have enough memory by running this command:**
```sql
docker run --rm "debian:buster-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'
```

## About [docker-compose.yaml](docker-compose.yaml)
This file contains several service definitions:
- airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
- airflow-webserver - The webserver is available at http://localhost:8080.
- airflow-worker - The worker that executes the tasks given by the scheduler.
- airflow-init - The initialization service.
- flower - The flower app for monitoring the environment. It is available at http://localhost:5555.
- postgres - The database.
- redis - The redis - broker that forwards messages from scheduler to worker.
- db-analytics - The analytic database which the candidate will use to perform analysis and pipeline worflows 

Some directories in the container are mounted, which means that their contents are synchronized between your computer and the container.
- ./dags - you can put your DAG files here.
- ./logs - contains logs from task execution and scheduler.
- ./plugins - you can put your custom plugins here.

This file uses the latest Airflow image (apache/airflow).

## Usage
*Every following commands should be perfomerd at the racine of this repository*

If you want some example of airflow dags set flag **AIRFLOW__CORE__LOAD_EXAMPLES** to **true** in [docker-compose.yaml](docker-compose.yaml)

Now you can start all services:
```bash
docker-compose up
```

Cleaning-up the environment
```bash
docker-compose down --volumes --remove-orphans
rm -rf logs/
```

Display state of containers
```bash
docker-compose ps
```

To stop and delete containers, delete volumes with database data and download images, run:
```
docker-compose down --volumes --rmi all
```

**Exploring Analytics Postgres database**

First get the name of the analytics db (container name ending like `*db-analytics_1`)

```shell
> docker ps
```

First enter in the PostgreSQL command prompt, by running the following command

```shell
> docker exec --user postgres -it <analytics_db_container> psql -U ma_user -d ma_db
```

Display tables 

```shell
> \dt+
```

(`+` add size and description columns)

Then you can run sql query

```shell
> SELECT * FROM pet;
```

**Accessing the web interface**

Once the cluster has started up, you can log in to the web interface and try to run some tasks.

The webserver is available at: http://localhost:8080.

The default account has the login **airflow** and the password **airflow**.

## About the test

We have provided to you in this repository an example of dag which uses the `db-analytics` throught the connection `local_db_analytics` (which is defined in the `docker-compose.yml` file thanks to the env variable `AIRFLOW_CONN_LOCAL_DB_ANALYTICS`)
