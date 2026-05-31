# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, Final

AIRFLOW_DAG_ID: Final = "realtimedatastreaming_user_profile_ingestion"
AIRFLOW_START_DATE: Final = datetime(2026, 1, 1, tzinfo=UTC)
AIRFLOW_SCHEDULE: Final = "@hourly"
AIRFLOW_TAGS: Final = ("realtimedatastreaming", "ingestion", "user-profiles")
AIRFLOW_OWNER: Final = "data-platform"
AIRFLOW_RETRIES: Final = 2
AIRFLOW_RETRY_DELAY: Final = timedelta(minutes=5)
AIRFLOW_DEPENDS_ON_PAST: Final = False
AIRFLOW_CATCHUP: Final = False
AIRFLOW_MAX_ACTIVE_RUNS: Final = 1
AIRFLOW_BACKFILL_POLICY: Final = "manual-only-no-automatic-catchup"

DEFAULT_ARGS: Final[dict[str, object]] = {
    "owner": AIRFLOW_OWNER,
    "depends_on_past": AIRFLOW_DEPENDS_ON_PAST,
    "retries": AIRFLOW_RETRIES,
    "retry_delay": AIRFLOW_RETRY_DELAY,
}


@dataclass(frozen=True, slots=True)
class BashTaskSpec:
    task_id: str
    bash_command: str


USER_PROFILE_INGESTION_TASKS: Final[tuple[BashTaskSpec, ...]] = (
    BashTaskSpec(
        task_id="register_schema_registry_contracts",
        bash_command="python -m realtimedatastreaming.orchestration.schema_registry_contracts",
    ),
    BashTaskSpec(
        task_id="run_user_profile_ingestion",
        bash_command="realtimedatastreaming",
    ),
)


def build_user_profile_ingestion_dag() -> Any:
    airflow_module = import_module("airflow")
    empty_operator_module = import_module("airflow.operators.empty")
    bash_operator_module = import_module("airflow.operators.bash")

    dag_class = airflow_module.DAG
    empty_operator_class = empty_operator_module.EmptyOperator
    bash_operator_class = bash_operator_module.BashOperator

    with dag_class(
        dag_id=AIRFLOW_DAG_ID,
        description="Orchestrates the user profile ingestion pipeline.",
        start_date=AIRFLOW_START_DATE,
        schedule=AIRFLOW_SCHEDULE,
        catchup=AIRFLOW_CATCHUP,
        max_active_runs=AIRFLOW_MAX_ACTIVE_RUNS,
        default_args=DEFAULT_ARGS,
        tags=list(AIRFLOW_TAGS),
    ) as airflow_dag:
        start = empty_operator_class(task_id="start")
        finish = empty_operator_class(task_id="finish")

        previous_task = start
        for task_spec in USER_PROFILE_INGESTION_TASKS:
            current_task = bash_operator_class(
                task_id=task_spec.task_id,
                bash_command=task_spec.bash_command,
            )
            previous_task >> current_task
            previous_task = current_task

        previous_task >> finish

    return airflow_dag
