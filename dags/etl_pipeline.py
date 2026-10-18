
from datetime import datetime
from pathlib import Path

import pandas as pd
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


def extract_dataset(entity, extension):
    path = DATA_DIR / f"{entity}.{extension}"

    if extension == "csv":
        df = pd.read_csv(path)
    elif extension == "json":
        df = pd.read_json(path)
    elif extension == "yaml":
        records = yaml.safe_load(path.read_text(encoding="utf-8"))
        df = pd.DataFrame(records)
    else:
        raise ValueError(f"Unsupported format: {extension}")

    return df.to_dict(orient="records")


def validate_files():
    required = [
        "customers.csv", "customers.json", "customers.yaml",
        "products.csv", "products.json", "products.yaml",
        "orders.csv", "orders.json", "orders.yaml",
    ]

    missing = [
        name for name in required
        if not (DATA_DIR / name).exists()
    ]

    if missing:
        raise FileNotFoundError(f"Missing data files: {missing}")

    for name in required:
        entity, extension = name.rsplit(".", 1)
        records = extract_dataset(entity, extension)

        if len(records) != 1000:
            raise ValueError(
                f"{name} must contain 1,000 records; "
                f"found {len(records)}"
            )

        print(f"Validated {name}: {len(records)} records")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def database_setup():
    schema_path = Path("/opt/airflow/sql/schema.sql")
    sql = schema_path.read_text(encoding="utf-8")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


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
                        value = str(value).lower() == "true"

                    values.append(value)

                placeholders = ", ".join(["%s"] * len(columns))
                column_sql = ", ".join(columns)

                cur.execute(
                    f"INSERT INTO {entity} ({column_sql}) "
                    f"VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    values,
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
                    entity,
                    "SUCCESS",
                    len(records),
                    f"Loaded {len(records)} {entity} records from CSV",
                ),
            )

    print(f"Loaded {len(records)} records into {entity}")


def load_customers():
    load_entity("customers")


def load_products():
    load_entity("products")


def load_orders():
    load_entity("orders")


def validate_database():
    expected = {
        "customers": 1000,
        "products": 1000,
        "orders": 1000,
    }

    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, expected_count in expected.items():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                actual_count = cur.fetchone()[0]

                if actual_count != expected_count:
                    raise ValueError(
                        f"{table}: expected {expected_count} rows, "
                        f"found {actual_count}"
                    )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM orders o
                LEFT JOIN customers c
                    ON o.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
                """
            )

            orphan_orders = cur.fetchone()[0]

            if orphan_orders != 0:
                raise ValueError(
                    f"Found {orphan_orders} orders without customers"
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
                    "Row counts and customer relationships validated",
                ),
            )

    print("Database validation passed.")


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
        task_id="file_validation",
        python_callable=validate_files,
    )

    database_setup_task = PythonOperator(
        task_id="database_setup",
        python_callable=database_setup,
    )

    extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=lambda: [
            extract_dataset(entity, "csv")
            for entity in ("customers", "products", "orders")
        ],
    )

    extract_json = PythonOperator(
        task_id="extract_json",
        python_callable=lambda: [
            extract_dataset(entity, "json")
            for entity in ("customers", "products", "orders")
        ],
    )

    extract_yaml = PythonOperator(
        task_id="extract_yaml",
        python_callable=lambda: [
            extract_dataset(entity, "yaml")
            for entity in ("customers", "products", "orders")
        ],
    )

    load_customers_task = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers,
    )

    load_products_task = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )

    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    data_validation = PythonOperator(
        task_id="data_validation",
        python_callable=validate_database,
    )

    completion_task = PythonOperator(
        task_id="completion",
        python_callable=completion,
    )

    file_validation >> database_setup_task

    database_setup_task >> [
        extract_csv,
        extract_json,
        extract_yaml,
    ]

    [
        extract_csv,
        extract_json,
        extract_yaml,
    ] >> load_customers_task

    load_customers_task >> load_products_task
    load_products_task >> load_orders_task
    load_orders_task >> data_validation >> completion_task
