# Olist E-Commerce Data Engineering Pipeline

> **YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University**

An end-to-end, fully containerized data engineering pipeline built on the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The system covers raw CSV ingestion, multi-layer PostgreSQL storage, workflow orchestration, full-text indexing, and interactive dashboards — all launched with a single command.

---

## Team Members

| Name | Student ID | GitHub |
|---|---|---|
| Defne Yıldırım | 150230727 | [@itu-itis23-yildirimd20](https://github.com/itu-itis23-yildirimd20) |
| Mehmet Burak Koçoğlu | 150220738 | — |
| Doğa Fikir | 150230715 | — |
| Muhammet Ahmet Saydam | 150230720 | — |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Network: olist_network                 │
│                                                                       │
│  ┌──────────┐    ┌─────────────────────────────────────────────────┐ │
│  │          │    │              PostgreSQL (port 5432)              │ │
│  │  ./data  │───▶│  ┌─────────┐  ┌──────────┐  ┌───────────────┐  │ │
│  │  (CSVs)  │    │  │   raw   │─▶│ staging  │─▶│   warehouse   │  │ │
│  └──────────┘    │  │  layer  │  │  layer   │  │  (star schema)│  │ │
│       │          │  └─────────┘  └──────────┘  └───────────────┘  │ │
│       │          └─────────────────────────────────────────────────┘ │
│       │                         ▲              ▲                      │
│       ▼                         │              │                      │
│  ┌─────────┐              ┌─────────┐    ┌─────────┐                 │
│  │  Apache │──────────────▶  Apache │    │  pgAdmin│                 │
│  │   NiFi  │  (ingest)    │ Airflow │    │ (5050)  │                 │
│  │  (8443) │              │  (8080) │    └─────────┘                 │
│  └─────────┘              └─────────┘                                │
│                                 │                                     │
│                                 ▼                                     │
│                    ┌─────────────────────┐                           │
│                    │   Elasticsearch     │◀──── FastAPI (8000)       │
│                    │      (9200)         │                            │
│                    └─────────────────────┘                           │
│                                 │                                     │
│                                 ▼                                     │
│                    ┌─────────────────────┐                           │
│                    │       Kibana        │                            │
│                    │      (5601)         │                            │
│                    └─────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

1. **Ingest** — Apache NiFi reads CSV files from `./data/` and loads them into PostgreSQL's `raw` schema as plain TEXT.
2. **Transform** — Airflow DAGs trigger SQL transformations: `raw → staging` (type casting, cleaning) → `warehouse` (star schema, materialized views).
3. **Index** — Warehouse data is pushed to Elasticsearch for fast full-text and analytical search.
4. **Visualize** — Kibana dashboards display revenue by state, top product categories, and seller performance.
5. **Serve** — FastAPI exposes REST endpoints over the Elasticsearch index for downstream consumers.

---

## Dataset

Source: [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (open license)

| File | Table | Rows |
|---|---|---|
| olist_orders_dataset.csv | orders | 99,441 |
| olist_order_items_dataset.csv | order_items | 112,650 |
| olist_customers_dataset.csv | customers | 99,441 |
| olist_sellers_dataset.csv | sellers | 3,095 |
| olist_products_dataset.csv | products | 32,951 |
| olist_order_payments_dataset.csv | payments | 103,886 |
| product_category_name_translation.csv | category_translation | 71 |

> The `./data/` folder in the repository contains a **sampled subset** (≈1,000 rows per table) for quick local demo. Full dataset must be downloaded separately from Kaggle.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24.x
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.x (included in Docker Desktop)
- At least **16 GB RAM** and **20 GB free disk**
- No Python, Java, or other runtime required on the host machine

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/itu-itis23-yildirimd20/olist-data-pipeline.git
cd olist-data-pipeline
```

### 2. Create the environment file

Copy the example and set your credentials:

```bash
cp .env.example .env
```

The `.env` file must contain:

```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin_password123
POSTGRES_DB=olist_db

NIFI_USER=admin
NIFI_PASSWORD=admin_password1234

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

> ⚠️ Never commit the real `.env` file. It is listed in `.gitignore`.

### 3. (Optional) Download the full dataset

Place the Olist CSV files inside the `./data/` folder. The sample data already present is enough for a smoke test.

### 4. Launch the entire stack

```bash
docker compose up --build
```

> First build takes approximately 5–10 minutes due to image pulls. Subsequent starts are faster.

Wait until you see Airflow's webserver log line:
```
[INFO] Listening at: http://0.0.0.0:8080
```

---

## Service URLs

| Service | URL | Default Credentials |
|---|---|---|
| Apache NiFi | https://localhost:8443/nifi | admin / admin_password1234 |
| Apache Airflow | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | See `.env` |
| Kibana | http://localhost:5601 | — |
| Elasticsearch | http://localhost:9200 | — |
| FastAPI Docs | http://localhost:8000/docs | — |

---

## Repository Structure

```
olist-data-pipeline/
├── docker-compose.yml        # Single-command stack definition
├── .env.example              # Template for environment variables
├── .gitignore
├── LICENSE
├── README.md
│
├── data/                     # Sample CSV files (full dataset: download from Kaggle)
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── ...
│
├── sql/                      # PostgreSQL initialization scripts
│   ├── 01_raw_schema.sql     # Raw layer: TEXT columns, no transformation
│   ├── 02_staging_schema.sql # Staging: typed columns, cleaned data
│   ├── 03_warehouse_schema.sql  # Star schema: dim/fact tables
│   └── 04_views.sql          # Materialized views (revenue, categories, sellers)
│
├── dags/                     # Airflow DAG definitions
│   └── olist_pipeline_dag.py # Main orchestration DAG
│
├── nifi/                     # NiFi flow template
│   └── NiFi_Flow.json        # Exported flow (import via NiFi UI)
│
└── docs/                     # Technical report (PDF + LaTeX source)
    ├── report.tex
    └── report.pdf
```

---

## Pipeline Details

### PostgreSQL — Three-Layer Architecture

| Layer | Schema | Description |
|---|---|---|
| Raw | `raw` | Unmodified CSV data stored as `TEXT`. Loaded by NiFi. |
| Staging | `staging` | Type-cast, cleaned, and normalized data. |
| Warehouse | `warehouse` | Star schema optimized for analytics. |

**Star Schema Tables:**

- **Dimensions:** `dim_date`, `dim_customers`, `dim_sellers`, `dim_products`
- **Facts:** `fact_orders`, `fact_order_items`, `fact_payments`
- **Views:** `vw_revenue_by_state`, `vw_top_categories`, `vw_seller_performance`

### Apache NiFi — Ingestion

- Reads CSV files from the mounted `./data/` volume
- Uses `PutDatabaseRecord` processor to load raw data into PostgreSQL
- PostgreSQL JDBC driver (`postgresql-42.7.2.jar`) is included in the repository
- Import the flow: NiFi UI → Upload Template → select `NiFi_Flow.json`

### Apache Airflow — Orchestration

The main DAG (`olist_pipeline_dag`) orchestrates:

```
raw_to_staging >> staging_to_warehouse >> push_to_elasticsearch
```

- Runs daily (can be triggered manually)
- Uses `PostgresOperator` for SQL transformations
- Uses `PythonOperator` for Elasticsearch indexing

### Elasticsearch + Kibana — Search & Visualization

- Warehouse data is indexed into Elasticsearch after each DAG run
- Kibana dashboards (import from `./docs/kibana_export.ndjson`):
  - Monthly revenue by customer state
  - Top 10 product categories by revenue
  - Seller performance ranking

### FastAPI — REST API

- Available at `http://localhost:8000`
- Interactive docs: `http://localhost:8000/docs`
- Example endpoints:
  ```
  GET /orders?state=SP&limit=20
  GET /categories/top?n=10
  GET /sellers/{seller_id}/performance
  ```

---

## Example Commands

Trigger the Airflow DAG manually:
```bash
docker exec olist_airflow airflow dags trigger olist_pipeline_dag
```

Check DAG run status:
```bash
docker exec olist_airflow airflow dags list-runs -d olist_pipeline_dag
```

Query the warehouse directly:
```bash
docker exec -it olist_postgres psql -U admin -d olist_db \
  -c "SELECT * FROM warehouse.vw_top_categories LIMIT 10;"
```

Test the Elasticsearch index:
```bash
curl http://localhost:9200/olist_orders/_count
```

Stop all services:
```bash
docker compose down
```

Stop and remove all volumes (full reset):
```bash
docker compose down -v
```

---

## Troubleshooting

**Airflow fails to start / database connection error**
- Postgres may not be ready yet. Wait ~30 seconds and run `docker compose restart airflow`.
- Verify credentials match between `.env` and `docker-compose.yml`.

**NiFi UI not reachable**
- NiFi takes 2–3 minutes to fully initialize. Wait and refresh.
- Use `https://` (not `http://`) — NiFi uses a self-signed certificate.

**Elasticsearch out of memory**
- Increase Docker Desktop memory allocation to at least 4 GB.
- The compose file limits ES heap to 512 MB (`ES_JAVA_OPTS=-Xms512m -Xmx512m`).

**Port conflicts**
- If ports 5432, 8080, 8443, 9200, or 5601 are already in use, stop the conflicting service or change the port mapping in `docker-compose.yml`.

---

## Known Limitations

- **NiFi flow import is manual** — the flow must be imported through the NiFi UI after the container starts. Full automation via flow.json.gz mounting is a planned improvement.
- **Airflow uses standalone mode** — suitable for development/demo; a production deployment would use separate scheduler and webserver containers with a proper executor (Celery/Kubernetes).
- **No authentication on Elasticsearch** — the cluster runs without security enabled (`xpack.security.enabled=false`). Acceptable for local demo; not for production.
- **Sample data only in repo** — full Olist CSV files (~100 MB) are not committed. Download from Kaggle and place in `./data/` for full pipeline execution.

---

## AI Usage Declaration

| Tool | Version | Purpose | Validation Method |
|---|---|---|---|
| Claude (Anthropic) | claude-sonnet-4 | SQL schema suggestions, DAG skeleton, README drafting | All SQL executed and tested locally; DAG runs verified in Airflow UI |
| GitHub Copilot | — | Code completion for FastAPI endpoints | Each suggestion reviewed, tested with `pytest` |

All outputs generated by AI tools were reviewed, modified, and validated by team members before inclusion in the submission.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

The Olist dataset is made available under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
