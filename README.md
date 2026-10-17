\# DATA-430 Project 1: Rapid ETL Pipeline Development



\## 1. Project Overview



The purpose of this project is to develop an automated ETL (Extract, Transform, Load) pipeline using Python, Apache Airflow, and PostgreSQL.



The pipeline generates synthetic customer, product, and order datasets, stores them in CSV, JSON, and YAML formats, validates the files, and loads the data into a PostgreSQL database. Apache Airflow manages the workflow and allows the pipeline to be triggered manually or scheduled to run daily.



\## 2. Technologies Used



\- \*\*Python:\*\* Generates synthetic datasets and processes data.

\- \*\*Pandas:\*\* Supports data processing and transformation.

\- \*\*Faker:\*\* Generates realistic sample data.

\- \*\*PyYAML:\*\* Reads and supports YAML data.

\- \*\*Apache Airflow:\*\* Orchestrates and monitors ETL tasks.

\- \*\*PostgreSQL:\*\* Stores the processed customer, product, and order data.

\- \*\*Docker Compose:\*\* Runs the Airflow and PostgreSQL services in containers.



\## 3. Project Structure



```text

DATA430\_Project1/

├── dags/

│   └── etl\_pipeline.py

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

├── scripts/

├── sql/

│   └── schema.sql

├── generate\_data.py

├── requirements.txt

├── docker-compose.yml

└── README.md

```



\## 4. Dataset Description



The project generates three datasets, with 1,000 records in each dataset.



\- \*\*Customers:\*\* Customer ID, name, email, phone, city, registration date, and customer type.

\- \*\*Products:\*\* Product ID, product name, category, brand, unit price, stock quantity, and active status.

\- \*\*Orders:\*\* Order ID, customer ID, order date, order status, payment method, total amount, discount, and shipping cost.



Each dataset is saved in CSV, JSON, and YAML formats, resulting in nine data files.



\## 5. Environment Setup



The project uses Docker Desktop and Docker Compose to run the required services.



Prerequisites:



\- Docker Desktop installed and running.

\- Python and the required Python packages for data generation.

\- Apache Airflow running in a Docker container.

\- PostgreSQL running in a Docker container.



\## 6. Generate the Datasets



Open PowerShell in the project directory and run:



```powershell

docker run --rm -v "${PWD}:/app" -w /app python:3.12-slim sh -c "pip install -q Faker PyYAML \&\& python generate\_data.py"

```



The script creates customer, product, and order datasets in CSV, JSON, and YAML formats inside the `data` folder.



\## 7. Start the ETL Services



Start the PostgreSQL service:



```powershell

docker compose up -d postgres

```



Start the Airflow service:



```powershell

docker compose up -d airflow

```



Check the status of the services:



```powershell

docker compose ps

```



\## 8. Run the ETL Pipeline



1\. Open `http://localhost:8080` in a web browser.

2\. Sign in to the Apache Airflow interface.

3\. Locate the DAG named `data430\_etl\_pipeline`.

4\. Unpause the DAG if necessary.

5\. Trigger the DAG manually.

6\. Monitor the tasks in the Graph or Grid view.

7\. Confirm that all tasks complete successfully.



The pipeline validates the input files, prepares the database tables, processes the datasets, loads records, validates the results, and records execution details in the audit table.



\## 9. Database Validation



Run the following command in PowerShell to check the number of records in each database table:



```powershell

docker compose exec postgres psql -U data430 -d data430 -c "SELECT 'customers' AS table\_name, COUNT(\*) FROM customers UNION ALL SELECT 'products', COUNT(\*) FROM products UNION ALL SELECT 'orders', COUNT(\*) FROM orders;"

```



Expected results after a successful run:



\- Customers: 1,000 records.

\- Products: 1,000 records.

\- Orders: 1,000 records.



Check for orders that reference nonexistent customers:



```powershell

docker compose exec postgres psql -U data430 -d data430 -c "SELECT COUNT(\*) AS orders\_without\_customer FROM orders o LEFT JOIN customers c ON o.customer\_id = c.customer\_id WHERE c.customer\_id IS NULL;"

```



The expected result is zero orders without a matching customer.



\## 10. Error Handling and Logging



The pipeline validates input files and database records. It records ETL execution details in the `etl\_audit` table and uses database transactions where implemented by the pipeline tasks. Airflow provides task status and execution logs to help identify errors.



\## 11. Expected Results



A successful project run should produce:



\- Nine data files across CSV, JSON, and YAML formats.

\- Three PostgreSQL tables containing 1,000 records each.

\- Valid relationships between orders and customers.

\- A working Airflow DAG with successful task execution.

\- Audit records documenting pipeline activity.



\## 12. Conclusion



This project demonstrates how to generate, validate, transform, and load data using an automated ETL workflow. Docker Compose simplifies service startup, Apache Airflow manages task dependencies, and PostgreSQL stores the resulting structured data. The project also demonstrates data validation, referential integrity, and execution monitoring.



