\# DATA-430 Project 1: User Guide



\## 1. Introduction



This user guide explains how to start, run, and verify the Rapid ETL Pipeline Development project. The project uses Docker Compose, Apache Airflow, and PostgreSQL to process customer, product, and order data.



\## 2. Prerequisites



Before running the project, make sure that:



\- Docker Desktop is installed and running.

\- The project files are available in the `DATA430\_Project1` folder.

\- The required data files have been generated.

\- The Docker services can start successfully.



\## 3. Generate the Data Files



1\. Open PowerShell.

2\. Navigate to the project directory:



&#x20;  `C:\\Users\\namit\\OneDrive\\Documents\\DATA430\_Project1`



3\. Run the following command:



```powershell

docker run --rm -v "${PWD}:/app" -w /app python:3.12-slim sh -c "pip install -q Faker PyYAML \&\& python generate\_data.py"

```



4\. Confirm that the `data` folder contains nine files: three each for customers, products, and orders in CSV, JSON, and YAML formats.



\## 4. Start the Services



Make sure Docker Desktop is running, then execute these commands from the project directory.



Start PostgreSQL:



```powershell

docker compose up -d postgres

```



Start Airflow:



```powershell

docker compose up -d airflow

```



Check the services:



```powershell

docker compose ps

```



The PostgreSQL service should show as healthy, and the Airflow service should be running.



\## 5. Access Apache Airflow



1\. Open a web browser.

2\. Navigate to `http://localhost:8080`.

3\. Sign in using the Airflow administrator credentials configured for your environment.

4\. Locate the DAG named `data430\_etl\_pipeline`.



Keep your password private and do not include it in project documentation.



\## 6. Run the Pipeline



1\. Find `data430\_etl\_pipeline` in the Airflow interface.

2\. Unpause the DAG if it is paused.

3\. Click the trigger option to start a run.

4\. Open the Graph or Grid view.

5\. Monitor the tasks until they finish.



A successful run should show green task indicators. If a task fails, open the task logs to investigate the error.



\## 7. Verify the Database



Run the following command in PowerShell to check the record counts:



```powershell

docker compose exec postgres psql -U data430 -d data430 -c "SELECT 'customers' AS table\_name, COUNT(\*) FROM customers UNION ALL SELECT 'products', COUNT(\*) FROM products UNION ALL SELECT 'orders', COUNT(\*) FROM orders;"

```



Expected counts:



\- Customers: 1,000.

\- Products: 1,000.

\- Orders: 1,000.



To check that all orders reference valid customers, run:



```powershell

docker compose exec postgres psql -U data430 -d data430 -c "SELECT COUNT(\*) AS orders\_without\_customer FROM orders o LEFT JOIN customers c ON o.customer\_id = c.customer\_id WHERE c.customer\_id IS NULL;"

```



The expected result is zero.



\## 8. Troubleshooting



\### Docker is not running



Open Docker Desktop and wait until its engine is ready. Then try the Docker Compose commands again.



\### Airflow page does not open



Check that the Airflow container is running:



```powershell

docker compose ps

```



If needed, inspect its logs:



```powershell

docker compose logs --tail=50 airflow

```



\### A pipeline task fails



Open the failed task in Airflow and review its logs. Check that the data files exist, PostgreSQL is healthy, and the database connection settings are correct.



\### Database counts are incorrect



Trigger the pipeline again after reviewing the task logs. Check the input files and database validation results.



\## 9. Stop the Services



To stop the running services, execute:



```powershell

docker compose down

```



This stops and removes the containers but preserves the named PostgreSQL data volume.



To stop the services and also delete the database volume, use the following command only if you intentionally want to erase the stored database data:



```powershell

docker compose down -v

```



\## 10. Conclusion



This guide provides the basic steps for generating the datasets, starting the services, running the Airflow pipeline, verifying the PostgreSQL tables, and troubleshooting common problems.



