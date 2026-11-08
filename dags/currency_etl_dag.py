"""
Currency Exchange Rate ETL Pipeline
-----------------------------------
Extract  : Fetch daily exchange rates from the Frankfurter API (v2)
Transform: Reshape each rate into the required document format
Load     : Upsert documents into MongoDB (currency_db.exchange_rates)

Data is passed between tasks using NAMED XComs (explicit keys).
"""

import logging
from datetime import datetime, timedelta, timezone

import pendulum
import requests
from airflow.sdk import dag, task

log = logging.getLogger(__name__)

# ---------------- Configuration ----------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY"]
SOURCE_NAME = "Frankfurter API"

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys
XCOM_RAW_KEY = "raw_exchange_rates"
XCOM_TRANSFORMED_KEY = "transformed_exchange_rates"
XCOM_LOAD_SUMMARY_KEY = "load_summary"

default_args = {
    "owner": "your_name",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="currency_exchange_etl",
    description="Daily ETL: Frankfurter exchange rates -> MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    # ---------------- 1. EXTRACT ----------------
    @task(task_id="extract_exchange_rates")
    def extract_exchange_rates(**context):
        params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
        log.info("Calling %s with params %s", API_URL, params)

        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        rates = response.json()

        if not isinstance(rates, list) or not rates:
            raise ValueError(f"Unexpected API response: {rates}")

        payload = {
            "retrieved_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "rates": rates,
        }
        log.info("Extracted %d rates: %s", len(rates), rates)

        # Named XCom push
        context["ti"].xcom_push(key=XCOM_RAW_KEY, value=payload)

    # ---------------- 2. TRANSFORM ----------------
    @task(task_id="transform_exchange_rates")
    def transform_exchange_rates(**context):
        ti = context["ti"]

        # Named XCom pull
        payload = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW_KEY)
        if not payload:
            raise ValueError("No raw data found in XCom")

        retrieved_at = payload["retrieved_at"]
        transformed = []

        for item in payload["rates"]:
            if item.get("rate") is None or not item.get("quote"):
                log.warning("Skipping invalid record: %s", item)
                continue

            transformed.append(
                {
                    "base_currency": item["base"],
                    "target_currency": item["quote"],
                    "exchange_rate": float(item["rate"]),
                    "rate_date": item["date"],
                    "source": SOURCE_NAME,
                    "retrieved_at": retrieved_at,
                }
            )

        if not transformed:
            raise ValueError("Transformation produced no valid records")

        for doc in transformed:
            log.info("Transformed: %s", doc)

        # Named XCom push
        ti.xcom_push(key=XCOM_TRANSFORMED_KEY, value=transformed)

    # ---------------- 3. LOAD ----------------
    @task(task_id="load_to_mongodb")
    def load_to_mongodb(**context):
        from airflow.providers.mongo.hooks.mongo import MongoHook
        from pymongo import ASCENDING, UpdateOne

        ti = context["ti"]

        # Named XCom pull
        records = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED_KEY)
        if not records:
            raise ValueError("No transformed data found in XCom")

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        client = hook.get_conn()
        collection = client[MONGO_DB][MONGO_COLLECTION]

        # Unique index -> re-running the DAG never creates duplicates
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="uniq_pair_per_day",
        )

        operations = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(operations)

        summary = {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "matched": result.matched_count,
            "total_in_collection": collection.count_documents({}),
        }
        log.info("Load summary: %s", summary)

        # Named XCom push (useful for monitoring / screenshot)
        ti.xcom_push(key=XCOM_LOAD_SUMMARY_KEY, value=summary)

    # ---------------- Task dependencies ----------------
    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()