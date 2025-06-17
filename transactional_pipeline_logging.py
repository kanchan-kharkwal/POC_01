from airflow import DAG
from airflow.operators.python import PythonOperator  # To run Python functions as tasks
from datetime import datetime, timedelta
import glob  # For file pattern matching
from pyspark.sql.functions import year, month, to_date
import os
import shutil # for file operations
from pyspark.sql.functions import col, expr, current_date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from airflow.sensors.python import PythonSensor  # To wait for a file to be available before starting the DAG

# common settings for all tasks 
default_args = {
    'owner': 'kanchan',
    'start_date': datetime(2025, 5, 21),
    'retries': 2,  # Number of retries in case of failure
    'retry_exponential_backoff': True,  # Use exponential backoff for retries
    'retry_delay': timedelta(minutes=2),  # Delay between retries
    'depends_on_past': False  # Do not depend on past runs
    # 'execution_timeout': timedelta(mi),  # Set a timeout for the entire DAG run
}

def csv_file_exists(**kwargs):
    file_path = '/home/kanchan/airflow/data/landing'
    return any(f.endswith('.csv') for f in os.listdir(file_path))

def create_spark_session():
    return SparkSession.builder \
        .master("local[*]")\
        .appName("transaction_pipeline")\
        .getOrCreate()

# Ingest data from a CSV file and save it as Parquet
#  context- dictionary Airflow gives to ur Python func when we set provide_context=True
def ingest_data(**context):
    
    # FOR MANUAL TRIGGERING
    # run_date=context['dag_run'].conf.get('run_date','20250521') # default
    file_path = f"/home/kanchan/airflow/data/landing"
    raw_path = "/home/kanchan/airflow/data/raw"

    spark = create_spark_session()
    
    # Find all CSV files in landing folder (or can target a single file if needed)
    csv_files = glob.glob(f"{file_path}/*.csv")
    
    if not csv_files:
        print("No CSV files found to ingest.")
        spark.stop()
        return "no file present to ingest"
    
    # Defining the expected schema of incoming file
    schema_config={
        "transaction_id": "string",
        "customer_id": "string",
        "product_id": "string",
        "amount": "double",
        "transaction_date": "timestamp"
    }
    #  for automatic column mapping
    # Mapping string datatype to PySpark types
    type_map = {
        "string": StringType(),
        "double": DoubleType(),
        "timestamp": TimestampType()
    }
    transaction_schema = StructType([
        StructField(col, type_map[dt], True)
        for col, dt in schema_config.items()
    ])
    # schema_config - key value pair mai column name and data type
    # dynamic schema creation
    
    df = spark.read.option("header", True).schema(transaction_schema).csv(file_path)
    df.show(5)
    df.printSchema()
    df.write.mode("overwrite").parquet("/home/kanchan/airflow/data/processed")
    print(f"✅ Data ingested from {file_path} to processed")
    spark.stop()
    
    dest_dir = '/home/kanchan/airflow/data/raw'
    os.makedirs(dest_dir, exist_ok=True)  # creates directory if missing, no error if exists

    for file in csv_files:
        filename = os.path.basename(file)
        dest_file = os.path.join(raw_path, filename)
        print(f"Moving {file} to {dest_file}")
        shutil.move(file, dest_file)
        print(f"✅ Moved {filename} to raw folder")
    
    return True

# Check if the file has the correct schema, else move to invalid folder
def validate_schema(**context):
    # run_date = context['dag_run'].conf.get('run_date', '20250521')
    
    file_path = f"/home/kanchan/airflow/data/processed"
    invalids_path = file_path.replace("processed", "invalids")
    spark = create_spark_session()
    
    # Expected schema
    expected_columns = {"transaction_id", "customer_id", "product_id", "amount", "transaction_date"}

    df = spark.read.parquet("/home/kanchan/airflow/data/processed")
    actual_columns = set(df.columns)

    missing_columns = expected_columns - actual_columns

    if missing_columns:
        print(f" Missing columns: {missing_columns}")
        shutil.move(file_path, invalids_path)
        raise ValueError(" Schema validation failed!")
    else:
        print("✅ Schema validation passed")
        return True
        
# Remove duplicates and load data incrementally into partitioned folders 
def deduplicate_data(**context):
    spark = create_spark_session()
    df = spark.read.parquet("/home/kanchan/airflow/data/processed")
    
    # Drop duplicates
    deduped = df.dropDuplicates(["customer_id", "transaction_id", "transaction_date"])
    print(f" Deduplicated records: {deduped.count()}")
    
    deduped.write.mode("overwrite").parquet("/home/kanchan/airflow/data/deduplicated")
    spark.stop()
    return True

# Check for nulls, negative amounts, and outdated records
def data_quality_check(**context):
    spark = create_spark_session()
    df = spark.read.parquet("/home/kanchan/airflow/data/deduplicated")
    
    # Identify invalid transaction dates (older than 2 years)
    invalid_date_df = df.filter(expr("transaction_date < date_sub(current_date(), 730)"))
    valid_date_df = df.filter(expr("transaction_date >= date_sub(current_date(), 730)"))

    critical_fields = ["customer_id", "transaction_id", "amount"]

    # Capture invalids
    nulls_df = df.filter(
        col("customer_id").isNull() | 
        col("transaction_id").isNull() | 
        col("amount").isNull()
        )
    # Identify negative amount rows
    negative_df = df.filter(col("amount") < 0)

    # Save invalids
    base_path = "/home/kanchan/airflow/data/invalids"
    nulls_df.write.mode("overwrite").parquet(f"{base_path}/null_records")
    negative_df.write.mode("overwrite").parquet(f"{base_path}/negative_amount")
    print(f"✅ Invalid records saved. Nulls: {nulls_df.count()}, Negatives: {negative_df.count()}")
    
    # Filter valid records: no nulls, amount >= 0, within 2 years
    #  expr() - to write SQL-like expressions inside PySpark.
    valid_df = df.filter(col("amount") >= 0) \
                 .filter(expr("transaction_date >= date_sub(current_date(), 730)")) \
                 .dropna(subset=critical_fields)

    # ti - task istance (metadata) object - gives access to task instance information ie status push pull reties count etc
    # context['ti'].xcom_push(key='valid_count', value=valid_count)
    
    # Count metrics for logging
    ti = context['ti']
    ti.xcom_push(key='invalid_date_count', value=invalid_date_df.count())
    ti.xcom_push(key='null_count', value=nulls_df.count())
    ti.xcom_push(key='negative_amount_count', value=negative_df.count())
    ti.xcom_push(key='valid_count', value=valid_date_df.count())
    
    # If nulls or negatives exist, raise error
    if nulls_df.count() > 0 or negative_df.count() > 0:
        raise ValueError("❌ Data quality check failed: Found nulls or negative amounts in valid-dated records.")
    
    # valid_df.write.mode("overwrite").parquet("/home/kanchan/airflow/data/validated")   
    print(f"✅ Data quality passed. With these metrics: \n"
          f"Valid Records: {valid_date_df.count()}\n"
          f"Invalid Dates: {invalid_date_df.count()}\n"
          f"Null Records: {nulls_df.count()}\n"
          f"Negative Amounts: {negative_df.count()}")
    spark.stop()
    return True


def incremental_load(**context):
    spark = create_spark_session()
    df = spark.read.parquet("/home/kanchan/airflow/data/deduplicated")
    
    # Add year and month columns for partitioning
    # SINCE FOR EVERY TIMESTAMP DIFFERENT FOLDER WAS GETTING CREATED IN CLEANED FOLDER we convert transaction_date to date type (avoid partition explosion)
    df = df.withColumn("transaction_year", year("transaction_date")) \
           .withColumn("transaction_month", month("transaction_date"))\
           .withColumn("transaction_date", to_date("transaction_date"))
           
    # Load and merge previous data if exists
    processed_path = "/home/kanchan/airflow/data/cleaned"
    if os.path.exists(processed_path):
        prev_df = spark.read.parquet(processed_path)
        combined_df = prev_df.unionByName(df).dropDuplicates(["customer_id", "transaction_id", "transaction_date"])
    else:
        combined_df = df
        
    combined_count = combined_df.count()
    combined_df.write.mode("overwrite") \
        .partitionBy("transaction_year", "transaction_month", "transaction_date") \
        .parquet(processed_path)
    
    # # push data to for logs or you can just return the value to be pulled later
    # context['ti'].xcom_push(key='combined_count', value=combined_count)
    
    print(f"✅ Cleaned data written to {processed_path}")
    combined_df.printSchema()
    # combined_df.show(5)
    return combined_count  # Return count for logging


# Log metrics by pulling values from previous steps using XCom
# XCom (cross-communication) is a feature in Airflow that allows tasks to exchange messages or data.
# context['ti'] - gives current task’s metadata and features Pulling data from other tasks (xcom_pull) Pushing data (xcom_push) ,Knowing task status, retries, etc
def log_metrics(**context):
    ti = context['ti']
    # pulling data from previous tasks
    combined_count = ti.xcom_pull(task_ids='deduplicate_incremental_load')  # task returns the count of combined records
    # If you want to pull from a specific key, you can specify it like this: (task_ids='deduplicate_incremental_load', key='combined_count')
    
    valid_count = ti.xcom_pull(task_ids='data_quality_check', key='valid_count')
    null_count = ti.xcom_pull(task_ids='data_quality_check', key='null_count')
    negative_count = ti.xcom_pull(task_ids='data_quality_check', key='negative_amount_count')
    run_date = context['dag_run'].conf.get('run_date', '20250521')
    
    # Just a simple print log for now
    failed = combined_count - valid_count
    print(f"Run Date: {run_date}")
    print(f"Total Records: {combined_count}")
    print(f"Failed Records: {failed}")
    print(f"⚠️ Null Issues: {null_count}")
    print(f"⚠️ Negative Amounts: {negative_count}")


    return {"total": combined_count, "failed": failed}

# Final audit log to print overall pipeline status
def audit_log(**context):
    ti = context['ti']
    # fetch data from log_metrics task
    logs = ti.xcom_pull(task_ids='log_metrics')
    run_date = context['dag_run'].conf.get('run_date', '20250521')
    file_path = f"/home/kanchan/airflow/data/landing"
    
    status = "success" if logs['failed'] < logs['total'] else "partial_success"
    
    # Here you would typically log to a database or external system
    print(f"Audit Log for {run_date}: File: {file_path}, Status: {status}")
    return True

# DAG definition
with DAG(
    dag_id='transaction_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes = 2),  # manual trigger 
    catchup=False,
    max_active_runs=1
) as dag:
    # within dag: we dont use dag = dag, as it is already defined in the context
    
    wait_for_csv = PythonSensor(
        task_id='wait_for_csv_file',
        python_callable=csv_file_exists,
        poke_interval=30,
        timeout=600,
        mode='poke'
    )
    
    # initial step to indicate the DAG is running
    def initialize():
        print("✅ DAG is running")
        
    start_pipeline = PythonOperator(
        task_id='start_pipeline',
        python_callable=initialize
    )
    ingest_task = PythonOperator(
        task_id='ingest_transactions',
        python_callable=ingest_data,
        provide_context=True
    )    
    validate_schema_task = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema,
        provide_context=True
   )
    deduplication_task = PythonOperator(
        task_id='deduplicate_incremental_load',
        python_callable=deduplicate_data,
        provide_context=True
    )
    data_quality_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
        provide_context=True
    )
    incremental_load_task = PythonOperator(
        task_id='incremental_load',
        python_callable=incremental_load,
        provide_context=True
    )
    log_metrics_task = PythonOperator(
        task_id='log_metrics',
        python_callable=log_metrics,
        provide_context=True
    )
    audit_log_task = PythonOperator(
        task_id='audit_log',
        python_callable=audit_log,
        provide_context=True
    )

    wait_for_csv >> start_pipeline >> ingest_task >> validate_schema_task >> deduplication_task >> data_quality_task >> incremental_load_task >> log_metrics_task >> audit_log_task
    
    # DAGBAG/UPDATERELATIVE ERROR IN AIRFLOW -- ASSIGNED FUNCTION AS A DOWNSTREAM TASK (fixed this )
