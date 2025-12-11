from __future__ import annotations

import os
from datetime import datetime, timedelta

from pendulum import timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

LOCAL_TZ = timezone("US/Eastern")

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "airflow_airflow_net")

# Images built locally and pushed to ECR for AWS
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "injury_pipeline:latest")
DBT_IMAGE = os.getenv("DBT_IMAGE", "injury_dbt:latest")

APP_ENV = os.getenv("APP_ENV", "local")
LOCAL_PG_DSN = os.getenv("LOCAL_PG_DSN", "")
AWS_PG_DSN = os.getenv("AWS_PG_DSN", "")

LOCAL_PG_USER = os.getenv("LOCAL_PG_USER", "")
LOCAL_PG_PASSWORD = os.getenv("LOCAL_PG_PASSWORD", "")
AWS_PG_HOST = os.getenv("AWS_PG_HOST", "")
AWS_PG_USER = os.getenv("AWS_PG_USER", "")
AWS_PG_PASSWORD = os.getenv("AWS_PG_PASSWORD", "")

IS_LOCAL = APP_ENV == "local"

# ECS config (for when APP_ENV != "local")
ECS_CLUSTER = os.getenv("ECS_CLUSTER", "")
ECS_PIPELINE_TASK_DEF = os.getenv("ECS_PIPELINE_TASK_DEF", "")
ECS_PIPELINE_CONTAINER_NAME = os.getenv("ECS_PIPELINE_CONTAINER_NAME", "pipeline")

ECS_DBT_TASK_DEF = os.getenv("ECS_DBT_TASK_DEF", "")
ECS_DBT_CONTAINER_NAME = os.getenv("ECS_DBT_CONTAINER_NAME", "dbt")

ECS_SUBNETS = os.getenv("ECS_SUBNETS", "")          # comma-separated
ECS_SECURITY_GROUPS = os.getenv("ECS_SECURITY_GROUPS", "")  # comma-separated


def _base_env() -> dict[str, str]:
    return {
        "APP_ENV": APP_ENV,
        "LOCAL_PG_DSN": LOCAL_PG_DSN,
        "AWS_PG_DSN": AWS_PG_DSN,
    }


def _dbt_env() -> dict[str, str]:
    env = _base_env().copy()
    env.update(
        {
            "LOCAL_PG_USER": LOCAL_PG_USER,
            "LOCAL_PG_PASSWORD": LOCAL_PG_PASSWORD,
            "AWS_PG_HOST": AWS_PG_HOST,
            "AWS_PG_USER": AWS_PG_USER,
            "AWS_PG_PASSWORD": AWS_PG_PASSWORD,
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def _ecs_network_conf() -> dict:
    subnets = [s for s in ECS_SUBNETS.split(",") if s]
    security_groups = [g for g in ECS_SECURITY_GROUPS.split(",") if g]
    return {
        "awsvpcConfiguration": {
            "subnets": subnets,
            "securityGroups": security_groups,
            "assignPublicIp": "ENABLED",
        }
    }


def make_pipeline_task(task_id: str, script_path: str) -> DockerOperator | EcsRunTaskOperator:
    env = _base_env()

    if IS_LOCAL:
        return DockerOperator(
            task_id=task_id,
            image=PIPELINE_IMAGE,
            command=f"python {script_path}",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mount_tmp_dir=False,
            environment=env,
        )
    else:
        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=ECS_CLUSTER,
            task_definition=ECS_PIPELINE_TASK_DEF,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": ECS_PIPELINE_CONTAINER_NAME,
                        "command": ["python", script_path],
                        "environment": [
                            {"name": k, "value": v} for k, v in env.items()
                        ],
                    }
                ],
            },
            network_configuration=_ecs_network_conf(),
        )


def make_dbt_task(task_id: str, select_path: str) -> DockerOperator | EcsRunTaskOperator:
    env = _dbt_env()
    dbt_cmd = f"run --target {APP_ENV} --select +path:{select_path}"

    if IS_LOCAL:
        return DockerOperator(
            task_id=task_id,
            image=DBT_IMAGE,
            command=dbt_cmd,
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            working_dir="/usr/app",
            mount_tmp_dir=False,
            environment=env,
        )
    else:
        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=ECS_CLUSTER,
            task_definition=ECS_DBT_TASK_DEF,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": ECS_DBT_CONTAINER_NAME,
                        "command": dbt_cmd.split(),
                        "environment": [
                            {"name": k, "value": v} for k, v in env.items()
                        ],
                    }
                ],
            },
            network_configuration=_ecs_network_conf(),
        )


default_args = {"execution_timeout": timedelta(hours=2)}

with DAG(
    dag_id="pipeline",
    description="bronze → dbt silver → dbt gold (Python + dbt, Docker/ECS switch)",
    start_date=datetime(2025, 9, 1, tzinfo=LOCAL_TZ),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["bronze", "dbt", "dockeroperator", "ecs"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ======================================================================
    #                           BRONZE GROUP
    # ======================================================================
    with TaskGroup(group_id="bronze") as bronze:
        bronze_games = make_pipeline_task(
            task_id="games",
            script_path="/app/bronze/bronze_games.py",
        )

        bronze_players = make_pipeline_task(
            task_id="players",
            script_path="/app/bronze/bronze_players.py",
        )

        bronze_statcast = make_pipeline_task(
            task_id="statcast",
            script_path="/app/bronze/bronze_statcast.py",
        )

        bronze_transactions = make_pipeline_task(
            task_id="transactions",
            script_path="/app/bronze/bronze_transactions.py",
        )

        bronze_roster_entries = make_pipeline_task(
            task_id="roster_entries",
            script_path="/app/bronze/bronze_roster_entries.py",
        )

        [bronze_players, bronze_transactions] >> bronze_roster_entries

    bronze_done = EmptyOperator(
        task_id="bronze_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ======================================================================
    #                         DBT SILVER
    # ======================================================================
    dbt_silver = make_dbt_task(
        task_id="dbt_run_silver",
        select_path="models/silver",
    )

    # ======================================================================
    #                       SILVER PYTHON TASKS
    # ======================================================================
    silver_il_placements = make_pipeline_task(
        task_id="silver_il_placements",
        script_path="/app/silver/silver_il_placements.py",
    )

    silver_injury = make_pipeline_task(
        task_id="silver_injury_spans",
        script_path="/app/silver/silver_injury_spans.py",
    )

    # ======================================================================
    #                           DBT GOLD
    # ======================================================================
    dbt_gold = make_dbt_task(
        task_id="dbt_run_gold",
        select_path="models/gold",
    )

    end = EmptyOperator(task_id="end")

    start >> bronze
    bronze >> bronze_done
    bronze_done >> dbt_silver >> silver_il_placements >> silver_injury >> dbt_gold >> end