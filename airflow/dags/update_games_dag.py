from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os



# Set DAG timezone to Eastern Time
local_tz = timezone("US/Eastern")

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
}

# Define the DAG for updating Statcast data daily
with DAG(
    dag_id = 'update_games_daily',
    description='Updating games table in baseball db',
    schedule='0 6 * 2-11 *',
    start_date=datetime(2025, 8, 8, tzinfo=local_tz),
    catchup=False,  # Avoid backfilling
    max_active_runs=1,  # Prevent overlapping DAG runs
    default_args=default_args
) as dag: 
    # BashOperator to run the Statcast update script inside the container
    run_script = BashOperator(
    task_id='run_games',
    bash_command='python /opt/airflow/scripts/update_games.py',
    env={ # Pass required environment variables into the Bash subprocess
            'DATABASE_URL': os.environ['DATABASE_URL'],
            'PYTHONPATH': '/opt/airflow/scripts'
        }
)