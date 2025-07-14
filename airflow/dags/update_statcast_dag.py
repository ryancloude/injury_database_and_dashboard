from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set DAG timezone to Eastern Time
local_tz = timezone("US/Eastern")

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'retry_delay':timedelta(minutes=30)
}

# Define the DAG for updating Statcast data daily
with DAG(
    dag_id = 'update_statcast_daily',
    description='Updating statcast table in baseball db',
    schedule='0 6 * * *',
    start_date=datetime(2025, 7, 10, tzinfo=local_tz),
    default_args=default_args
) as dag: 
    # BashOperator to run the Statcast update script inside the container
    run_script = BashOperator(
    task_id='run_update_statcast',
    bash_command='python /opt/airflow/scripts/update_statcast.py',
    env={ # Pass required environment variables into the Bash subprocess
            'DATABASE_URL': os.environ['DATABASE_URL'],
            'PYTHONPATH': '/opt/airflow/scripts'
        }
)