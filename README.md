# airflow-practice

A small **Apache Airflow + Postgres + MongoDB** playground project.

It contains two main pipelines:

1) **File → Mongo loader** (`mongo_dag.py`)
- Watches for CSV files in `data/input_data/`.
- Loads CSV rows into MongoDB collection `processed_data`.

2) **Mongo queries** (`mong_queries_dag.py`)
- Runs MongoDB aggregation queries (top-5 frequent comments, short comments, average score per day).
- Writes results to `data/output_data/`.
- Is designed to run **after** the loader completes (see “DAG ordering” below).

---

## Project structure

```
.
├── dags
│   ├── first_dag.py
│   ├── mongo_dag.py
│   ├── mong_queries_dag.py
│   └── utils
├── data
│   ├── input_data
│   └── output_data
├── docker-compose.yml
├── Dockerfile
├── logs
├── plugins
├── requirements.txt
└── Makefile
```

- `data/input_data/` — put your **source CSV** files here.
- `data/output_data/` — results produced by the Mongo queries DAG.

---

## Requirements

- Docker + Docker Compose
- (Optional) GNU Make

---

## Makefile shortcuts

If you have **GNU Make** installed, you can use the provided `Makefile` to run common Docker Compose commands.

- Start services:
  ```bash
  make up
  ```

- Stop services:
  ```bash
  make down
  ```

- Build images from scratch (no cache):
  ```bash
  make build
  ```

- Full restart (down + up):
  ```bash
  make restart
  ```

- Full rebuild and restart (down + build + up):
  ```bash
  make rebuild
  ```

- Tail Airflow logs:
  ```bash
  make logs
  ```

- Run linters via pre-commit:
  ```bash
  make lint
  ```

- List available Makefile targets:
  ```bash
  make help
  ```

> Note: the `logs` target follows logs for the Compose service named `webserver`. If your compose file uses `airflow-webserver` (as in this repo), update `Makefile` accordingly.

---

## Environment variables (.env)

Create a `.env` file in the project root.

```dotenv
# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL=30
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Warsaw

# Airflow UI admin
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=python3.8
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Mongo
MONGO_ROOT_USERNAME=root
MONGO_ROOT_PASSWORD=example
MONGO_DB=airflow

# File path used by Airflow Connection `fs_default`
# IMPORTANT: this must be an absolute path INSIDE the container.
FILE_PATH=/opt/airflow/data/input_data/
```

---

## Docker

The stack uses:

- `postgres:15` for Airflow metadata
- `mongo:7` for storing loaded data
- `apache/airflow:2.10.x` image built from the local `Dockerfile`

Airflow services:

- `airflow-init` — one-time DB migration + creates admin user + creates Airflow Connections
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-triggerer` (only needed for **deferrable operators/sensors**; safe to keep enabled)

---

## Airflow Connections created automatically

On first startup, `airflow-init` creates these connections:

- `mongo_default` — used by `MongoHook` in DAGs
- `fs_default` — a filesystem path connection used by file-related logic

### Important: correct `fs_default` JSON quoting

If you pass JSON via bash, **do not use single quotes** if you want `${FILE_PATH}` to expand.

```bash
airflow connections delete fs_default || true
airflow connections add fs_default \
  --conn-type fs \
  --conn-extra "{\"path\":\"${FILE_PATH}\"}"
```

---

## Run

### Start the stack

```bash
docker compose up -d --build
```

Or, using the Makefile:

```bash
make up
```

Open Airflow UI:

- http://localhost:8080

Login using `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`.

### Stop

```bash
docker compose down
```

Or:

```bash
make down
```

---

## Using the pipelines

1) Put a CSV into:

```
data/input_data/
```

2) Trigger the file/loader DAG (or wait for your sensor logic, depending on your DAG setup).

3) After the loader finishes, the queries DAG runs and writes results into:

```
data/output_data/
```

---

## DAG ordering

This project expects the **queries** DAG to run **after** the **loader** DAG.

Recommended approaches:

- **Dataset scheduling** (Airflow Datasets) — publish a dataset at the end of the loader DAG and subscribe to it in the queries DAG.
- **TriggerDagRunOperator** — explicitly trigger `mongo_queries_dag` from the last task of `mongo_dag`.

If you use `ExternalTaskSensor`, ensure both DAGs share compatible `logical_date` semantics (dataset-triggered runs often do not match the external DAG run’s logical date).

---

## Troubleshooting

### `airflow: command not found`

If you ever see `/entrypoint: line ... airflow: command not found`, it usually means the image accidentally installed a different PyPI package named `airflow` (not `apache-airflow`), or dependencies were installed without Airflow constraints.

Fix:
- Keep Airflow base image as `apache/airflow:...`.
- Install Python deps using Airflow constraints when adding providers.

### Mongo authentication failed

Ensure your `mongo_default` connection uses `authSource=admin` if you authenticate with root credentials.

Example URI:

```
mongodb://root:example@mongo:27017/airflow?authSource=admin
```

---

## Notes

This repository is meant for learning and experimentation. For production:

- Use a proper secrets backend for credentials
- Use a real rate-limit storage for Flask-Limiter
- Configure persistent volumes carefully