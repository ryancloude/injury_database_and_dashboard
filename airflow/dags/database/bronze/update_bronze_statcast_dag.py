from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os

# Load environment variables from .env file


# Set DAG timezone to Eastern Time
local_tz = timezone("US/Eastern")

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
}

# Define the DAG for updating Statcast data daily
with DAG(
    dag_id = 'update_bronze_statcast_daily',
    description='Updating bronze statcast table in baseball db',
    schedule='0 6 * 2-11 *',
    start_date=datetime(2025, 9, 3, tzinfo=local_tz),
    catchup=False,
    default_args=default_args
) as dag: 
    # BashOperator to run the Statcast update script inside the container
    run_script = BashOperator(
    task_id='update_bronze_statcast',
    bash_command='python /opt/airflow/scripts/database/bronze/bronze_statcast.py',
    env={ # Pass required environment variables into the Bash subprocess
            'BASEBALL_URL': os.environ['BASEBALL_URL'],
            'PYTHONPATH': '/opt/airflow/scripts/database/bronze'
        }
)