
# DATA-430 Project 1: Rapid ETL Pipeline Development

## 1. Project Overview

This project implements an automated Extract, Transform, Load (ETL) pipeline using Python, Apache Airflow, PostgreSQL, and Docker Compose.

The pipeline generates synthetic customer, product, and order datasets. Each dataset is stored in CSV, JSON, and YAML formats. The pipeline validates the files, loads the data into PostgreSQL, checks database results, and records execution details in an audit table.

Apache Airflow orchestrates the workflow, supports manual execution, and schedules the pipeline to run daily.

## 2. Technologies Used

- **Python:** Generates and processes data.
- **Pandas:** Reads and processes CSV, JSON, and YAML data.
- **Faker:** Generates synthetic sample data.
- **PyYAML:** Reads YAML files.
- **Apache Airflow:** Orchestrates and monitors ETL tasks.
- **PostgreSQL:** Stores customer, product, order, and audit data.
- **Docker Compose:** Runs the Airflow and PostgreSQL services.

## 3. Project Structure

```text
DATA430_Project1/
├── dags/
│   └── etl_pipeline.py
├── data/
│   ├── customers.csv
│   ├── customers.json
│   ├── customers.yaml
│   ├── products.csv
│   ├── products.json
│   ├── products.yaml
│   ├── orders.csv
│   ├── orders.json
│   └── orders.yaml
├── docs/
│   ├── Technical_Documentation.md
│   └── User_Guide.md
├── sql/
│   └── schema.sql
├── generate_data.py
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

## 4. Dataset Description

The project generates three datasets, each containing 1,000 records.

- **Customers:** Customer ID, name, email, phone, city, registration date, and customer type.
- **Products:** Product ID, product name, category, brand, unit price, stock quantity, and active status.
- **Orders:** Order ID, customer ID, order date, order status, payment method, total amount, discount, and shipping cost.

Each dataset is provided in CSV, JSON, and YAML formats, resulting in nine data files.

## 5. Prerequisites

Install and start Docker Desktop before running the project. Docker Compose is used to manage the services.

The project directory should contain the Python script, DAG, SQL schema, data files, and Docker Compose configuration described above.

## 6. Generate the Datasets

Open PowerShell in the project directory and run:

```powershell
docker run --rm -v "${PWD}:/app" -w /app python:3.12-slim sh -c "pip install -q Faker PyYAML && python generate_data.py"
```

The script creates the customer, product, and order datasets in CSV, JSON, and YAML formats inside the `data` directory.

## 7. Start the ETL Services

Start the services from the project directory:

```powershell
docker compose up -d
```

Check their status:

```powershell
docker compose ps
```

PostgreSQL should report a healthy status before the pipeline runs.

To inspect service logs, use:

```powershell
docker compose logs --tail 100 airflow
docker compose logs --tail 100 postgres
```

## 8. Access Airflow and Run the Pipeline

1. Open `http://localhost:8080` in a web browser.
2. Sign in to the Airflow interface.
3. Locate the DAG named `data430_etl_pipeline`.
4. Unpause the DAG if necessary.
5. Select **Trigger DAG** to run it manually.
6. Open the Grid or Graph view to monitor task progress.
7. Confirm that the tasks finish successfully.

For the standalone Airflow configuration used by this project, the generated administrator password can be retrieved from the running container with:

```powershell
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Use the Airflow administrator username shown during setup, typically `admin`, with the retrieved password.

The pipeline validates the files, prepares the database tables, executes the extraction tasks, loads the records, validates database results, and records execution details in the audit table.

## 9. Database Validation

Check the record count in each table:

```powershell
docker compose exec postgres psql -U data430 -d data430 -c "SELECT 'customers' AS table_name, COUNT(*) FROM customers UNION ALL SELECT 'products', COUNT(*) FROM products UNION ALL SELECT 'orders', COUNT(*) FROM orders;"
```

Expected results:

- Customers: 1,000 records.
- Products: 1,000 records.
- Orders: 1,000 records.

Check for orders without a matching customer:

```powershell
docker compose exec postgres psql -U data430 -d data430 -c "SELECT COUNT(*) AS orders_without_customer FROM orders o LEFT JOIN customers c ON o.customer_id = c.customer_id WHERE c.customer_id IS NULL;"
```

The expected result is zero.

## 10. Error Handling and Logging

The pipeline checks that required data files exist and contain the expected number of records. Database validation checks record counts and customer-order relationships.

Airflow provides task execution status and logs. The `etl_audit` table records successful entity loads and final validation results.

If a task fails, open that task's logs in Airflow to identify the error. Check that the input files exist, the services are running, and the database connection is available.

## 11. Troubleshooting

**Airflow page does not open**
- Check that Docker Desktop is running.
- Run `docker compose ps`.
- Inspect the Airflow logs with `docker compose logs --tail 100 airflow`.

**PostgreSQL is not healthy**
- Run `docker compose logs --tail 100 postgres`.
- Wait for the database health check to pass before running the DAG.

**DAG does not appear or has import errors**
- Check the DAG file at `dags/etl_pipeline.py`.
- Run `docker compose exec airflow airflow dags list-import-errors`.

**A pipeline task fails**
- Open the failed task's logs in Airflow.
- Check the required files, database connectivity, and error message before retrying.

## 12. Expected Results

A successful project run should produce:

- Nine data files across CSV, JSON, and YAML formats.
- Three PostgreSQL data tables containing 1,000 records each.
- Valid customer-order relationships.
- A working Airflow DAG with successful task execution.
- Audit records documenting pipeline activity.

## 13. Conclusion

This project demonstrates automated data generation, extraction, validation, and loading using Python, Pandas, Apache Airflow, and PostgreSQL. Docker Compose simplifies service startup, Airflow manages workflow dependencies, and PostgreSQL stores structured data. Database validation and audit records help verify the pipeline's results.
