from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from pendulum import timezone
import os
from docker.types import Mount

LOCAL_TZ = timezone("US/Eastern")

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "airflow_airflow_net")
HOST_SCRIPTS_DIR = os.environ["HOST_SCRIPTS_DIR"]                               
PIPELINE_APP_IMAGE = os.getenv("PIPELINE_APP_IMAGE", "pipeline-app:latest")
BASEBALL_URL = os.getenv("BASEBALL_URL", "")
HOST_PROJECTIONS_DIR =  os.environ["HOST_PROJECTIONS_DIR"]

scripts_mount = [Mount(target="/app/scripts", source=HOST_SCRIPTS_DIR, type="bind")]
projections_mount = [Mount(target="/app/projections", source=HOST_PROJECTIONS_DIR, type="bind")]

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
}

with DAG(
    dag_id="yearly",
    description="Single DAG For yearly injestions of teams info, preseason projections, and openening day salary data",
    start_date=datetime(2025, 9, 1, tzinfo=LOCAL_TZ),
    schedule="0 6 1 4 *",     # 6:00 AM On April 1st
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    bronze_teams = DockerOperator(
    task_id="teams",
    image=PIPELINE_APP_IMAGE,
    command="python /app/scripts/database/bronze/bronze_teams.py",
    docker_url="unix://var/run/docker.sock",
    api_version="auto",
    network_mode=DOCKER_NETWORK,
    auto_remove=True,
    mounts=scripts_mount,
    mount_tmp_dir=False,
    environment={"BASEBALL_URL": BASEBALL_URL},
)
    
    bronze_salary = DockerOperator(
    task_id="salary",
    image=PIPELINE_APP_IMAGE,
    command="python /app/scripts/database/bronze/bronze_salary.py",
    docker_url="unix://var/run/docker.sock",
    api_version="auto",
    network_mode=DOCKER_NETWORK,
    auto_remove=True,
    mounts=scripts_mount,
    mount_tmp_dir=False,
    environment={"BASEBALL_URL": BASEBALL_URL},
    )

    bronze_projections = DockerOperator(
    task_id="projections",
    image=PIPELINE_APP_IMAGE,
    command="python /app/scripts/database/bronze/bronze_projections.py",
    docker_url="unix://var/run/docker.sock",
    api_version="auto",
    network_mode=DOCKER_NETWORK,
    auto_remove=True,
    mounts=scripts_mount + projections_mount,
    mount_tmp_dir=False,
    environment={"BASEBALL_URL": BASEBALL_URL},
    )

    bronze_teams >> bronze_projections >> bronze_salary