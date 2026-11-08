# Currency Exchange Rate ETL Pipeline (Airflow + MongoDB)

An Apache Airflow 3 ETL pipeline built with the **Astro CLI** that fetches daily
currency exchange rates from the **Frankfurter API**, transforms them, and loads
them into **MongoDB**. **Mongo Express** provides a web UI for the database.

## Architecture
Frankfurter API → extract_exchange_rates → transform_exchange_rates → load_to_mongodb → MongoDB
(data passed between tasks using **named XComs**)

## Services
| Service | URL | Notes |
|---|---|---|
| Airflow UI | http://localhost:8080 | Astro local dev |
| Mongo Express | http://localhost:8082 | admin / admin123 |
| MongoDB | localhost:27017 | root / example (local dev only) |

- All services share the `airflow` Docker bridge network (Mongo reachable as `mongo`).
- MongoDB data persists in named volumes `mongo_data` and `mongo_config`.
- Containers, volumes and the Airflow image carry metadata labels.

## Output document format
```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.121,
  "rate_date": "2026-11-08",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-08T07:15:00"
}
```

## How to run
1. Install Docker Desktop and the Astro CLI.
2. `cp airflow_settings.example.yaml airflow_settings.yaml` and fill in credentials.
3. `astro dev start`
4. Trigger the `currency_exchange_etl` DAG in the Airflow UI.

## Project files
- `dags/currency_etl_dag.py` – the ETL DAG
- `docker-compose.override.yml` – MongoDB + Mongo Express services, network, volumes, labels
- `Dockerfile` – Astro Runtime image with metadata labels
- `requirements.txt` – `apache-airflow-providers-mongo`

Author: Sikander Atif Arain