# Transactional Data Pipeline Orchestration




This project is a **modular, production-ready data pipeline** built using **Apache Airflow** and **Apache PySpark**, designed to simulate real-time transaction data processing. It performs a full ETL flow—starting from ingesting `.csv` files to partitioned and cleaned parquet files, ensuring schema validation, deduplication, and data quality checks along the way.

---

## 📌 Project Objective

The goal was to develop an end-to-end PySpark data pipeline that could:
- Ingest raw `.csv` transaction data files.
- Validate the schema and filter invalid records.
- Deduplicate and transform the data.
- Perform rigorous data quality checks.
- Incrementally load data into partitioned folders.
- Log and audit each pipeline execution for monitoring and debugging.

This is a part of my hands-on learning in building scalable and fault-tolerant data engineering pipelines.

---

## 🛠️ Tech Stack

- **Apache Airflow** – for orchestration and scheduling
- **Apache PySpark** – for distributed data processing
- **Python 3.x**
- **Local FileSystem (CSV + Parquet)**
- **Airflow XCom** – for inter-task communication
- **Airflow Sensor** – to wait for files before ingestion

---

## 📂 Directory Structure

airflow/
│
├── dags/
│ └── transaction_pipeline.py # Main DAG file
│
├── data/
│ ├── landing/ # Incoming .csv files
│ ├── raw/ # Archived raw .csv files
│ ├── processed/ # Ingested data in Parquet
│ ├── deduplicated/ # Deduplicated data
│ ├── cleaned/ # Final output with partitioned data
│ └── invalids/ # Invalid records (nulls, negatives, schema issues)




## 🔁 Workflow Steps

1. **File Availability Check**: Waits for new `.csv` files in the `landing/` directory using an Airflow `PythonSensor`.

2. **Ingestion**: Reads all `.csv` files from the `landing/` folder into a PySpark DataFrame with defined schema, converts to Parquet, and moves original files to `raw/`.

3. **Schema Validation**: Confirms if all required columns exist. If not, moves data to `invalids/`.

4. **Deduplication**: Removes duplicate records based on transaction composite keys.

5. **Data Quality Check**: Filters records with:
   - Null values in critical fields
   - Negative transaction amounts
   - Outdated transaction dates (older than 2 years)

6. **Incremental Load**: Writes valid data into partitioned folders (`year/month/date`) in `cleaned/`.

7. **Metrics Logging**: Logs key statistics like number of valid/invalid/duplicate records using Airflow XCom.

8. **Audit Logging**: Final summary of pipeline run status (success/partial success) and metrics.

---

## How to Run

### ✅ Prerequisites

- Python 3.8+
- Apache Airflow (2.x)
- Java 8+ (for PySpark)
- Apache Spark (3.x)
- Airflow initialized with `airflow db init` and a user created

###  Install Dependencies

pip install apache-airflow
pip install pyspark


### Optionally set up a virtual environment:

python -m venv venv
source venv/bin/activate

###  Start Airflow Services

airflow scheduler
airflow webserver

Access the Airflow UI at http://localhost:8080


### Trigger the DAG

Place one or more .csv files inside data/landing/.
Trigger the DAG transaction_pipeline manually from the UI or wait for the scheduler to pick it.
Files are automatically processed and moved to appropriate directories.


### Sample Output Metrics
Valid Records:  980
Null Records:  10
Negative Amounts:  5
Outdated Records (older than 2 years):  7
Final Cleaned Records:  958


##  Key Learnings

-  End-to-end orchestration using **Apache Airflow DAGs**
-  Schema enforcement and partitioning in **PySpark**
-  Error handling using **Airflow XComs** and logging
-  Real-world ingestion and **data quality validation workflows**


## 📬 Contact

If you'd like to discuss this project or similar use cases, feel free to connect:

**Kanchan Kharkwal**  
[LinkedIn](https://www.linkedin.com/in/kanchan-kharkwal)  
 Email: [Gmail](kharkwal.kanchan31@gmail.com)
