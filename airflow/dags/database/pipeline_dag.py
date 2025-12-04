from __future__ import annotations

import os
from datetime import datetime, timedelta
from pendulum import timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

LOCAL_TZ = timezone("US/Eastern")

# ──────────────── Environment + host mounts ─────────────────────────────
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "airflow_airflow_net")

HOST_SCRIPTS_DIR = os.environ["HOST_SCRIPTS_DIR"]
HOST_DBT_DIR = os.environ["HOST_DBT_DIR"]
HOST_DBT_PROFILES = os.environ["HOST_DBT_PROFILES"]

PIPELINE_APP_IMAGE = os.getenv("PIPELINE_APP_IMAGE", "pipeline-app:latest")
DBT_IMAGE = os.getenv("DBT_IMAGE", "ghcr.io/dbt-labs/dbt-postgres:1.9.latest")

# Environment-driven DB selection (matches get_baseball_engine)
APP_ENV = os.getenv("APP_ENV", "local")
LOCAL_PG_DSN = os.getenv("LOCAL_PG_DSN", "")
AWS_PG_DSN = os.getenv("AWS_PG_DSN", "")

# dbt profile env vars
LOCAL_PG_USER = os.getenv("LOCAL_PG_USER", "")
LOCAL_PG_PASSWORD = os.getenv("LOCAL_PG_PASSWORD", "")
AWS_PG_HOST = os.getenv("AWS_PG_HOST", "")
AWS_PG_USER = os.getenv("AWS_PG_USER", "")
AWS_PG_PASSWORD = os.getenv("AWS_PG_PASSWORD", "")

# ──────────────── Reusable mounts ─────────────────────────────
scripts_mount = [
    Mount(target="/app/scripts", source=HOST_SCRIPTS_DIR, type="bind")
]

dbt_mounts = [
    Mount(target="/usr/app", source=HOST_DBT_DIR, type="bind", read_only=False),
    Mount(target="/root/.dbt", source=HOST_DBT_PROFILES, type="bind", read_only=True),
]

# ──────────────── DAG config ─────────────────────────────
default_args = {"execution_timeout": timedelta(hours=2)}

with DAG(
    dag_id="pipeline",
    description="bronze → dbt silver → dbt gold (Python + dbt in Docker)",
    start_date=datetime(2025, 9, 1, tzinfo=LOCAL_TZ),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "dbt", "dockeroperator"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ======================================================================
    #                           BRONZE GROUP
    # ======================================================================
    with TaskGroup(group_id="bronze") as bronze:

        bronze_games = DockerOperator(
            task_id="games",
            image=PIPELINE_APP_IMAGE,
            command="python /app/scripts/database/bronze/bronze_games.py",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mounts=scripts_mount,
            mount_tmp_dir=False,
            environment={
                "APP_ENV": APP_ENV,
                "LOCAL_PG_DSN": LOCAL_PG_DSN,
                "AWS_PG_DSN": AWS_PG_DSN,
            },
        )

        bronze_players = DockerOperator(
            task_id="players",
            image=PIPELINE_APP_IMAGE,
            command="python /app/scripts/database/bronze/bronze_players.py",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mounts=scripts_mount,
            mount_tmp_dir=False,
            environment={
                "APP_ENV": APP_ENV,
                "LOCAL_PG_DSN": LOCAL_PG_DSN,
                "AWS_PG_DSN": AWS_PG_DSN,
            },
        )

        bronze_statcast = DockerOperator(
            task_id="statcast",
            image=PIPELINE_APP_IMAGE,
            command="python /app/scripts/database/bronze/bronze_statcast.py",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mounts=scripts_mount,
            mount_tmp_dir=False,
            environment={
                "APP_ENV": APP_ENV,
                "LOCAL_PG_DSN": LOCAL_PG_DSN,
                "AWS_PG_DSN": AWS_PG_DSN,
            },
        )

        bronze_transactions = DockerOperator(
            task_id="transactions",
            image=PIPELINE_APP_IMAGE,
            command="python /app/scripts/database/bronze/bronze_transactions.py",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mounts=scripts_mount,
            mount_tmp_dir=False,
            environment={
                "APP_ENV": APP_ENV,
                "LOCAL_PG_DSN": LOCAL_PG_DSN,
                "AWS_PG_DSN": AWS_PG_DSN,
            },
        )

        bronze_roster_entries = DockerOperator(
            task_id="roster_entries",
            image=PIPELINE_APP_IMAGE,
            command="python /app/scripts/database/bronze/bronze_roster_entries.py",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mounts=scripts_mount,
            mount_tmp_dir=False,
            environment={
                "APP_ENV": APP_ENV,
                "LOCAL_PG_DSN": LOCAL_PG_DSN,
                "AWS_PG_DSN": AWS_PG_DSN,
            },
        )

        # intra-group dependencies
        [bronze_players, bronze_transactions] >> bronze_roster_entries

    bronze_done = EmptyOperator(
        task_id="bronze_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ======================================================================
    #                         DBT SILVER
    # ======================================================================
    dbt_silver = DockerOperator(
        task_id="dbt_run_silver",
        image=DBT_IMAGE,
        command=f"run --target {APP_ENV} --select +path:models/silver",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        working_dir="/usr/app",
        mounts=dbt_mounts,
        mount_tmp_dir=False,
        environment={
            "PYTHONUNBUFFERED": "1",
            # dbt profile env vars
            "LOCAL_PG_USER": LOCAL_PG_USER,
            "LOCAL_PG_PASSWORD": LOCAL_PG_PASSWORD,
            "AWS_PG_HOST": AWS_PG_HOST,
            "AWS_PG_USER": AWS_PG_USER,
            "AWS_PG_PASSWORD": AWS_PG_PASSWORD,
            "APP_ENV": APP_ENV,
        },
    )

    # ======================================================================
    #                       SILVER PYTHON TASKS
    # ======================================================================
    silver_il_placements = DockerOperator(
        task_id="silver_il_placements",
        image=PIPELINE_APP_IMAGE,
        command="python /app/scripts/database/silver/silver_il_placements.py",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mounts=scripts_mount,
        mount_tmp_dir=False,
        environment={
            "APP_ENV": APP_ENV,
            "LOCAL_PG_DSN": LOCAL_PG_DSN,
            "AWS_PG_DSN": AWS_PG_DSN,
        },
    )

    silver_injury = DockerOperator(
        task_id="silver_injury_spans",
        image=PIPELINE_APP_IMAGE,
        command="python /app/scripts/database/silver/silver_injury_spans.py",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        mounts=scripts_mount,
        mount_tmp_dir=False,
        environment={
            "APP_ENV": APP_ENV,
            "LOCAL_PG_DSN": LOCAL_PG_DSN,
            "AWS_PG_DSN": AWS_PG_DSN,
        },
    )

    # ======================================================================
    #                           DBT GOLD
    # ======================================================================
    dbt_gold = DockerOperator(
        task_id="dbt_run_gold",
        image=DBT_IMAGE,
        command=f"run --target {APP_ENV} --select +path:models/gold",
        docker_url="unix://var/run/docker.sock",
        api_version="auto",
        network_mode=DOCKER_NETWORK,
        auto_remove=True,
        working_dir="/usr/app",
        mounts=dbt_mounts,
        mount_tmp_dir=False,
        environment={
            "PYTHONUNBUFFERED": "1",
            "LOCAL_PG_USER": LOCAL_PG_USER,
            "LOCAL_PG_PASSWORD": LOCAL_PG_PASSWORD,
            "AWS_PG_HOST": AWS_PG_HOST,
            "AWS_PG_USER": AWS_PG_USER,
            "AWS_PG_PASSWORD": AWS_PG_PASSWORD,
            "APP_ENV": APP_ENV,
        },
    )

    end = EmptyOperator(task_id="end")

    # ======================================================================
    #                               GRAPH
    # ======================================================================
    start >> bronze
    bronze >> bronze_done
    bronze_done >> dbt_silver >> silver_il_placements >> silver_injury >> dbt_gold >> end