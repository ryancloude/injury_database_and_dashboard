from __future__ import annotations

import os
from datetime import datetime

from pendulum import timezone
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator

LOCAL_TZ = timezone("US/Eastern")

DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "airflow_airflow_net")

# Pipeline image built by docker-compose; code is baked into this image
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "injury_pipeline:latest")

# Environment-driven DB selection (used by pipeline scripts)
APP_ENV = os.getenv("APP_ENV", "local")
LOCAL_PG_DSN = os.getenv("LOCAL_PG_DSN", "")
AWS_PG_DSN = os.getenv("AWS_PG_DSN", "")

IS_LOCAL = APP_ENV == "local"

# ECS settings (for when APP_ENV != "local")
ECS_CLUSTER = os.getenv("ECS_CLUSTER", "")
ECS_PIPELINE_TASK_DEF = os.getenv("ECS_PIPELINE_TASK_DEF", "")
ECS_PIPELINE_CONTAINER_NAME = os.getenv("ECS_PIPELINE_CONTAINER_NAME", "pipeline")
ECS_SUBNETS = os.getenv("ECS_SUBNETS", "")          # comma-separated
ECS_SECURITY_GROUPS = os.getenv("ECS_SECURITY_GROUPS", "")  # comma-separated

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}


def make_pipeline_task(task_id: str, command: str) -> DockerOperator | EcsRunTaskOperator:
    env = {
        "APP_ENV": APP_ENV,
        "LOCAL_PG_DSN": LOCAL_PG_DSN,
        "AWS_PG_DSN": AWS_PG_DSN,
    }

    if IS_LOCAL:
        # Local: run container via DockerOperator on the same host/network as Airflow
        return DockerOperator(
            task_id=task_id,
            image=PIPELINE_IMAGE,
            command=f"python {command}",
            docker_url="unix://var/run/docker.sock",
            api_version="auto",
            network_mode=DOCKER_NETWORK,
            auto_remove=True,
            mount_tmp_dir=False,
            environment=env,
        )
    else:
        # AWS: run the same image as an ECS task
        subnets = [s for s in ECS_SUBNETS.split(",") if s]
        security_groups = [g for g in ECS_SECURITY_GROUPS.split(",") if g]

        return EcsRunTaskOperator(
            task_id=task_id,
            cluster=ECS_CLUSTER,
            task_definition=ECS_PIPELINE_TASK_DEF,
            launch_type="FARGATE",
            overrides={
                "containerOverrides": [
                    {
                        "name": ECS_PIPELINE_CONTAINER_NAME,
                        "command": ["python"] + command.split(),
                        "environment": [{"name": k, "value": v} for k, v in env.items()],
                    }
                ],
            },
            network_configuration={
                "awsvpcConfiguration": {
                    "subnets": subnets,
                    "securityGroups": security_groups,
                    "assignPublicIp": "ENABLED",
                }
            },
            awslogs_group=os.getenv("ECS_LOG_GROUP", ""),
            awslogs_stream_prefix=os.getenv("ECS_LOG_STREAM_PREFIX", "pipeline"),
        )


with DAG(
    dag_id="yearly",
    description="Yearly ingestion: teams, projections, salary",
    start_date=datetime(2025, 9, 1, tzinfo=LOCAL_TZ),
    schedule="0 6 1 4 *",  # 6:00 AM on April 1st
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # NOTE: commands now point into /app/bronze inside the pipeline image
    bronze_teams = make_pipeline_task(
        task_id="teams",
        command="/app/bronze/bronze_teams.py",
    )

    bronze_salary = make_pipeline_task(
        task_id="salary",
        command="/app/bronze/bronze_salary.py",
    )

    bronze_projections = make_pipeline_task(
        task_id="projections",
        command="/app/bronze/bronze_projections.py",
    )

    bronze_teams >> bronze_projections >> bronze_salary