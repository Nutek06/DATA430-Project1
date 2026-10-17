\# DATA-430 Project 1: Technical Documentation



\## 1. Introduction



This project implements an automated ETL (Extract, Transform, Load) pipeline using Python, Apache Airflow, PostgreSQL, and Docker Compose. The pipeline processes customer, product, and order datasets and loads them into a relational database.



\## 2. System Architecture



The system consists of the following components:



\- \*\*Data Generation:\*\* The `generate\_data.py` script creates synthetic customer, product, and order records.

\- \*\*Data Storage:\*\* Each dataset is saved in CSV, JSON, and YAML formats in the `data` directory.

\- \*\*Workflow Orchestration:\*\* Apache Airflow executes and monitors the ETL tasks.

\- \*\*Database:\*\* PostgreSQL stores the processed data in relational tables.

\- \*\*Container Management:\*\* Docker Compose starts and manages the Airflow and PostgreSQL services.



\## 3. Dataset Design



The pipeline generates 1,000 records for each of the following datasets.



\### Customers



The customers dataset contains customer ID, first name, last name, email, phone, city, registration date, and customer type.



\### Products



The products dataset contains product ID, product name, category, brand, unit price, stock quantity, and active status.



\### Orders



The orders dataset contains order ID, customer ID, order date, order status, payment method, total amount, discount amount, and shipping cost.



Each dataset is stored in CSV, JSON, and YAML formats, producing nine data files.



\## 4. Database Design



The database schema is defined in `sql/schema.sql`.



\- \*\*customers:\*\* Stores customer information. `customer\_id` is the primary key, and email addresses must be unique.

\- \*\*products:\*\* Stores product details. `product\_id` is the primary key.

\- \*\*orders:\*\* Stores order information. `order\_id` is the primary key, and `customer\_id` references the customers table.

\- \*\*etl\_audit:\*\* Stores ETL execution details, including pipeline name, entity, status, record count, message, and timestamps.



Primary keys identify individual records, and the foreign key maintains the relationship between orders and customers.



\## 5. ETL Workflow



The Airflow DAG is defined in `dags/etl\_pipeline.py`.



The workflow includes these stages:



1\. \*\*File Validation:\*\* Checks that the expected nine data files exist and contain 1,000 records each.

2\. \*\*Database Setup:\*\* Creates the database tables using the SQL schema.

3\. \*\*Data Extraction:\*\* Reads the input data in CSV, JSON, and YAML formats.

4\. \*\*Data Loading:\*\* Loads customer, product, and order records into PostgreSQL.

5\. \*\*Data Validation:\*\* Checks database record counts and validates the loaded data.

6\. \*\*Completion:\*\* Marks the workflow as complete when the preceding tasks succeed.



Airflow manages the dependencies between tasks and provides task status and execution logs.



\## 6. Error Handling and Audit Logging



The pipeline validates the input files before loading data. Airflow task logs can be used to investigate execution errors. The `etl\_audit` table records pipeline activity and processing information.



Database transactions and rollback behavior depend on the implementation of the relevant pipeline tasks.



\## 7. Docker Configuration



The `docker-compose.yml` file defines the PostgreSQL and Airflow services.



PostgreSQL stores the project data and initializes its schema from `sql/schema.sql`. Airflow runs the DAG and accesses the data and SQL files through mounted directories.



The PostgreSQL service uses a health check so that Airflow waits for the database to become ready.



\## 8. Execution Instructions



From the project directory, start PostgreSQL:



```powershell

docker compose up -d postgres

```



Start Airflow:



```powershell

docker compose up -d airflow

```



Check service status:



```powershell

docker compose ps

```



Open `http://localhost:8080`, sign in to Airflow, locate `data430\_etl\_pipeline`, and trigger a DAG run.



\## 9. Validation and Testing



The pipeline was executed successfully in the development environment.



The PostgreSQL tables were checked and contained:



\- Customers: 1,000 records.

\- Products: 1,000 records.

\- Orders: 1,000 records.



A foreign-key check found zero orders without a matching customer. The `etl\_audit` table also contained audit records.



\## 10. Conclusion



The project demonstrates automated data generation, file validation, ETL orchestration, relational database loading, and execution monitoring. Docker Compose provides a repeatable way to run the services, while Apache Airflow manages the pipeline tasks.



