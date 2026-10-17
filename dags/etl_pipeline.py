
from datetime import datetime
from pathlib import Path
import csv
import json

import yaml
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")

DB_CONFIG = {
    "host": "postgres",
    "database": "data430",
    "user": "data430",
    "password": "data430pass",
    "port": 5432,
}


def validate_files():
    required = [
        "customers.csv", "customers.json", "customers.yaml",
        "products.csv", "products.json", "products.yaml",
        "orders.csv", "orders.json", "orders.yaml",
    ]

    missing = [name for name in required if not (DATA_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing data files: {missing}")

    for name in required:
        path = DATA_DIR / name
        if path.suffix == ".csv":
            with path.open(encoding="utf-8") as file:
                if sum(1 for _ in csv.DictReader(file)) != 1000:
                    raise ValueError(f"{name} must contain 1,000 records")
        elif path.suffix == ".json":
            if len(json.loads(path.read_text(encoding="utf-8"))) != 1000:
                raise ValueError(f"{name} must contain 1,000 records")
        elif path.suffix == ".yaml":
            if len(yaml.safe_load(path.read_text(encoding="utf-8"))) != 1000:
                raise ValueError(f"{name} must contain 1,000 records")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def database_setup():
    schema_path = Path("/opt/airflow/sql/schema.sql")
    sql = schema_path.read_text(encoding="utf-8")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def extract_dataset(entity, extension):
    path = DATA_DIR / f"{entity}.{extension}"

    if extension == "csv":
        with path.open(encoding="utf-8") as file:
            return list(csv.DictReader(file))

    if extension == "json":
        return json.loads(path.read_text(encoding="utf-8"))

    if extension == "yaml":
        return yaml.safe_load(path.read_text(encoding="utf-8"))

    raise ValueError(f"Unsupported format: {extension}")


def load_entity(entity):
    table_columns = {
        "customers": [
            "customer_id", "first_name", "last_name", "email",
            "phone", "city", "registration_date", "customer_type",
        ],
        "products": [
            "product_id", "product_name", "category", "brand",
            "unit_price", "stock_quantity", "is_active",
        ],
        "orders": [
            "order_id", "customer_id", "order_date", "order_status",
            "payment_method", "total_amount", "discount_amount",
            "shipping_cost",
        ],
    }

    columns = table_columns[entity]
    all_records = []

    # Use CSV as the canonical load source; validate all three formats.
    records = extract_dataset(entity, "csv")

    with get_connection() as conn:
        with conn.cursor() as cur:
            for record in records:
                values = []
                for column in columns:
                    value = record[column]
                    if column in {
                        "customer_id", "product_id", "order_id",
                        "stock_quantity",
                    }:
                        value = int(value)
                    elif column in {
                        "unit_price", "total_amount",
                        "discount_amount", "shipping_cost",
                    }:
                        value = float(value)
                    elif column == "is_active":
                        value = value.lower() == "true"
                    values.append(value)

                placeholders = ", ".join(["%s"] * len(columns))
                column_sql = ", ".join(columns)
                cur.execute(
                    f"INSERT INTO {entity} ({column_sql}) "
                    f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    values,
                )
                all_records.append(record)

            cur.execute(
                """
                INSERT INTO etl_audit
                    (pipeline_name, entity_name, status,
                     records_processed, message, completed_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (
                    "data430_etl_pipeline",
                    entity,
                    "SUCCESS",
                    len(all_records),
                    f"Loaded {len(all_records)} {entity} records from CSV",
                ),
            )


def load_customers():
    load_entity("customers")


def load_products():
    load_entity("products")


def load_orders():
    load_entity("orders")


def validate_database():
    expected = {"customers": 1000, "products": 1000, "orders": 1000}

    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, count in expected.items():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                actual = cur.fetchone()[0]
                if actual != count:
                    raise ValueError(
                        f"{table}: expected {count} rows, found {actual}"
                    )

            cur.execute(
                """
                INSERT INTO etl_audit
                    (pipeline_name, entity_name, status,
                     records_processed, message, completed_at)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (
                    "data430_etl_pipeline",
                    "all",
                    "SUCCESS",
                    sum(expected.values()),
                    "Database row-count validation passed",
                ),
            )


def completion():
    print("ETL pipeline completed successfully.")


with DAG(
    dag_id="data430_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["DATA430", "ETL"],
) as dag:
    file_validation = PythonOperator(
        task_id="file_validation", python_callable=validate_files
    )
    database_setup_task = PythonOperator(
        task_id="database_setup", python_callable=database_setup
    )
    extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=lambda: [extract_dataset(e, "csv")
                                 for e in ("customers", "products", "orders")],
    )
    extract_json = PythonOperator(
        task_id="extract_json",
        python_callable=lambda: [extract_dataset(e, "json")
                                 for e in ("customers", "products", "orders")],
    )
    extract_yaml = PythonOperator(
        task_id="extract_yaml",
        python_callable=lambda: [extract_dataset(e, "yaml")
                                 for e in ("customers", "products", "orders")],
    )
    load_customers_task = PythonOperator(
        task_id="load_customers", python_callable=load_customers
    )
    load_products_task = PythonOperator(
        task_id="load_products", python_callable=load_products
    )
    load_orders_task = PythonOperator(
        task_id="load_orders", python_callable=load_orders
    )
    data_validation = PythonOperator(
        task_id="data_validation", python_callable=validate_database
    )
    completion_task = PythonOperator(
        task_id="completion", python_callable=completion
    )

    file_validation >> database_setup_task
    database_setup_task >> [extract_csv, extract_json, extract_yaml]
    [extract_csv, extract_json, extract_yaml] >> load_customers_task
    load_customers_task >> load_products_task >> load_orders_task
    load_orders_task >> data_validation >> completion_task
