# Import necessary libraries and functions
from datetime import timedelta, datetime  # To work with time intervals and dates
from airflow import DAG  # Import the DAG object
from airflow.operators.python_operator import PythonOperator  # To use Python functions as tasks
from airflow.utils.dates import days_ago  # Utility function for date calculations

# Import custom function for ETL process
from twitter_etl import run_twitter_etl  # Custom ETL function to extract Twitter data

# Define the default arguments to be used by the DAG
dag_config = {
    'owner': 'airflow',  # Owner name
    'depends_on_past': False,  # DAG does not depend on past executions
    'start_date': datetime(2019, 11, 8),  # Start date of the DAG
    'email': ['PUT YOUR EMAIL ADDRESS HERE'],  # Notification email address
    'email_on_failure': False,  # Disable email on failure
    'email_on_retry': False,  # Disable email on retry
    'retries': 1,  # Number of retries
    'retry_delay': timedelta(minutes=1)  # Delay between retries
}

# Initialize the DAG object
twitter_data_dag = DAG(
    'twitter_data_extraction_dag',  # DAG ID
    default_args=dag_config,  # Reference to the default arguments
    description='DAG for extracting Twitter data for ETL processing',  # Description of the DAG
    schedule_interval=timedelta(days=1),  # Interval at which the DAG will run
)

# Create a PythonOperator task to run the ETL process
etl_task = PythonOperator(
    task_id='execute_twitter_etl',  # Task ID
    python_callable=run_twitter_etl,  # Function to be executed
    dag=twitter_data_dag,  # Reference to the DAG object
)

# Set dependencies and order of execution
etl_task  # This task is currently the only task and has no dependencies
