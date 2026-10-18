# DATA-430 Project 1: Technical Documentation

## 1. Introduction

This project implements an automated ETL (Extract, Transform, Load) pipeline using Python, Apache Airflow, PostgreSQL, and Docker Compose. The pipeline processes customer, product, and order datasets and loads them into a relational database.

## 2. System Architecture

The system uses separate components for generating data, orchestrating the workflow, storing records, and running services.

### Architecture Diagram

```text
+-------------------------+
|   generate_data.py      |
| Generates sample data   |
+------------+------------+
             |
             v
+-------------------------+
|   Nine Data Files       |
| CSV, JSON, and YAML     |
| Customers, Products,    |
| Orders                  |
+------------+------------+
             |
             v
+-------------------------+
|     Apache Airflow      |
|  ETL Pipeline / DAG     |
+------------+------------+
             |
             v
+-------------------------+
|     PostgreSQL          |
| customers               |
| products                |
| orders                  |
| etl_audit               |
+-------------------------+

Docker Compose manages the Airflow and PostgreSQL services.
```

### Main Components

- **Data Generation:** `generate_data.py` creates synthetic customer, product, and order records.
- **Data Storage:** Each dataset is saved in CSV, JSON, and YAML formats in the `data` directory.
- **Workflow Orchestration:** Apache Airflow executes and monitors the ETL tasks.
- **Database:** PostgreSQL stores the records in relational tables.
- **Container Management:** Docker Compose starts and manages Airflow and PostgreSQL.

## 3. Dataset Design

The pipeline generates 1,000 records for each of the following datasets.

### Customers

The customers dataset contains customer ID, first name, last name, email, phone, city, registration date, and customer type.

### Products

The products dataset contains product ID, product name, category, brand, unit price, stock quantity, and active status.

### Orders

The orders dataset contains order ID, customer ID, order date, order status, payment method, total amount, discount amount, and shipping cost.

Each dataset is stored in CSV, JSON, and YAML formats, producing nine data files.

## 4. Database Design

The database schema is defined in `sql/schema.sql`.

- **customers:** Stores customer information. `customer_id` is the primary key, and email addresses must be unique.
- **products:** Stores product details. `product_id` is the primary key.
- **orders:** Stores order information. `order_id` is the primary key, and `customer_id` references the customers table.
- **etl_audit:** Stores ETL execution details, including pipeline name, entity, status, record count, message, and timestamps.

### Database Relationship Diagram

```text
+-----------------------+
|      customers        |
+-----------------------+
| PK customer_id        |
| first_name            |
| last_name             |
| email                 |
+-----------+-----------+
            |
            | One customer can have many orders
            |
            v
+-----------------------+
|        orders         |
+-----------------------+
| PK order_id           |
| FK customer_id        |
| order_date             |
| order_status           |
| total_amount           |
+-----------------------+

+-----------------------+
|       products        |
+-----------------------+
| PK product_id         |
| product_name          |
| category              |
| unit_price            |
+-----------------------+

+-----------------------+
|       etl_audit       |
+-----------------------+
| PK audit_id           |
| pipeline_name         |
| entity_name           |
| status                |
| records_processed     |
| timestamps            |
+-----------------------+
```

The primary keys identify individual records. The foreign key in `orders` maintains the relationship between orders and customers. The products table stores product information independently, and the audit table records pipeline activity.

## 5. ETL Workflow

The Airflow DAG is defined in `dags/etl_pipeline.py`.

### Data Flow

```text
Start
  |
  v
Validate Input Files
  |
  v
Set Up Database
  |
  v
Extract CSV, JSON, and YAML
  |
  v
Load Customers
  |
  v
Load Products
  |
  v
Load Orders
  |
  v
Validate Database Records
  |
  v
Complete Pipeline
```

The workflow includes these stages:

1. **File Validation:** Checks that the expected nine data files exist and contain 1,000 records each.
2. **Database Setup:** Creates the database tables using the SQL schema.
3. **Data Extraction:** Reads the input data in CSV, JSON, and YAML formats.
4. **Data Loading:** Loads customer, product, and order records into PostgreSQL.
5. **Data Validation:** Checks database record counts and validates the loaded data.
6. **Completion:** Marks the workflow as complete when the preceding tasks succeed.

Airflow manages task dependencies and provides task status and execution logs. The DAG is configured for daily scheduling and can also be triggered manually.

## 6. Error Handling and Audit Logging

The pipeline validates input files before loading records. Missing or invalid input files can cause the validation task to fail, preventing later dependent tasks from proceeding.

Airflow task logs provide information for investigating task failures. Database operations use connection and transaction handling so database errors can be investigated and unsuccessful transactions can be rolled back where applicable.

The `etl_audit` table stores pipeline activity, including processing status, record counts, messages, and timestamps. Database validation checks expected record counts and verifies that orders reference existing customers.

## 7. Docker Configuration

The `docker-compose.yml` file defines the PostgreSQL and Airflow services.

- PostgreSQL stores the project data.
- The database schema is initialized from `sql/schema.sql`.
- Airflow runs the ETL DAG.
- Project directories are mounted into the Airflow container so it can access the DAG, data files, scripts, and SQL schema.
- A PostgreSQL health check helps ensure the database is ready before Airflow starts.

Docker Compose provides a consistent way to run the project services.

## 8. Execution Instructions

Run the following commands from the project directory.

### Start the Services

```powershell
docker compose up -d postgres
docker compose up -d airflow
```

### Check Service Status

```powershell
docker compose ps
```

### Open Airflow

1. Open `http://localhost:8080` in a browser.
2. Sign in using the Airflow credentials created by the standalone setup.
3. Locate the `data430_etl_pipeline` DAG.
4. Trigger the DAG manually when needed.
5. Review task statuses and logs to check execution.

### Stop the Services

```powershell
docker compose down
```

This stops the containers but preserves the database volume unless the volume is explicitly removed.

## 9. Validation and Testing

The pipeline was executed successfully in the development environment.

The PostgreSQL tables were checked and contained:

- Customers: 1,000 records.
- Products: 1,000 records.
- Orders: 1,000 records.

A foreign-key check found zero orders without a matching customer. The `etl_audit` table also contained audit records.

These checks demonstrate that the pipeline loaded the expected number of records and maintained the customer-order relationship.

## 10. Design Decisions

- **Apache Airflow:** Selected to define task dependencies, schedule execution, and monitor pipeline runs.
- **PostgreSQL:** Selected as the relational database for structured records and primary- and foreign-key constraints.
- **CSV, JSON, and YAML:** Used to demonstrate reading multiple common data formats.
- **Docker Compose:** Used to simplify starting and managing the application services.
- **Audit Table:** Used to retain information about ETL processing and support troubleshooting.
- **Synthetic Data:** Used to test the pipeline without relying on real customer information.

## 11. Conclusion

The project demonstrates automated data generation, input-file validation, multi-format data extraction, relational database loading, and execution monitoring. Apache Airflow manages the pipeline tasks, PostgreSQL stores the records, and Docker Compose provides a repeatable way to run the services.

The successful validation of 1,000 customers, 1,000 products, and 1,000 orders confirms that the pipeline worked in the tested environment.