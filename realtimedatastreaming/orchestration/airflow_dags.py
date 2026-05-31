# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from typing import Any

from realtimedatastreaming.orchestration import pipelines

AIRFLOW_DAG_ID = pipelines.AIRFLOW_DAG_ID
AIRFLOW_TAGS = pipelines.AIRFLOW_TAGS


def _load_dag_for_airflow() -> Any | None:
    try:
        return pipelines.build_user_profile_ingestion_dag()
    except ModuleNotFoundError as exc:
        if exc.name is not None and exc.name.startswith("airflow"):
            return None
        raise


dag = _load_dag_for_airflow()
