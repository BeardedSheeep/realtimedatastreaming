# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import importlib
import importlib.util
import sys
from types import ModuleType
from typing import Any, cast

from realtimedatastreaming.orchestration import airflow_dags
from realtimedatastreaming.orchestration.pipelines import (
    AIRFLOW_BACKFILL_POLICY,
    AIRFLOW_CATCHUP,
    AIRFLOW_DAG_ID,
    AIRFLOW_DEPENDS_ON_PAST,
    AIRFLOW_MAX_ACTIVE_RUNS,
    AIRFLOW_RETRIES,
    AIRFLOW_RETRY_DELAY,
    AIRFLOW_SCHEDULE,
    AIRFLOW_TAGS,
    DEFAULT_ARGS,
    USER_PROFILE_INGESTION_TASKS,
)


class FakeDag:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.tasks: list[FakeTask] = []

    def __enter__(self) -> FakeDag:
        FakeTask.active_dag = self
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        FakeTask.active_dag = None


class FakeTask:
    active_dag: FakeDag | None = None

    def __init__(self, *, task_id: str, **kwargs: Any) -> None:
        self.task_id = task_id
        self.kwargs = kwargs
        self.downstream_task_ids: list[str] = []
        if self.active_dag is not None:
            self.active_dag.tasks.append(self)

    def __rshift__(self, other: FakeTask) -> FakeTask:
        self.downstream_task_ids.append(other.task_id)
        return other


class FakeEmptyOperator(FakeTask):
    pass


class FakeBashOperator(FakeTask):
    pass


def install_fake_airflow_modules(monkeypatch: Any) -> None:
    airflow_module = ModuleType("airflow")
    airflow_module_dynamic = cast(Any, airflow_module)
    airflow_module_dynamic.DAG = FakeDag

    operators_module = ModuleType("airflow.operators")
    empty_module = ModuleType("airflow.operators.empty")
    empty_module_dynamic = cast(Any, empty_module)
    empty_module_dynamic.EmptyOperator = FakeEmptyOperator
    bash_module = ModuleType("airflow.operators.bash")
    bash_module_dynamic = cast(Any, bash_module)
    bash_module_dynamic.BashOperator = FakeBashOperator

    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "airflow.operators", operators_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.empty", empty_module)
    monkeypatch.setitem(sys.modules, "airflow.operators.bash", bash_module)


def test_airflow_dag_entrypoint_is_inside_application_package() -> None:
    assert airflow_dags.AIRFLOW_DAG_ID == AIRFLOW_DAG_ID == "realtimedatastreaming_user_profile_ingestion"
    assert airflow_dags.AIRFLOW_TAGS == AIRFLOW_TAGS == ("realtimedatastreaming", "ingestion", "user-profiles")


def test_airflow_pipeline_has_explicit_operational_steps_outside_dag_entrypoint() -> None:
    assert [task.task_id for task in USER_PROFILE_INGESTION_TASKS] == [
        "register_schema_registry_contracts",
        "run_user_profile_ingestion",
    ]
    assert USER_PROFILE_INGESTION_TASKS[0].bash_command == (
        "python -m realtimedatastreaming.orchestration.schema_registry_contracts"
    )
    assert USER_PROFILE_INGESTION_TASKS[1].bash_command == "realtimedatastreaming"


def test_airflow_retry_and_backfill_policy_is_explicit() -> None:
    assert AIRFLOW_SCHEDULE == "@hourly"
    assert AIRFLOW_CATCHUP is False
    assert AIRFLOW_MAX_ACTIVE_RUNS == 1
    assert AIRFLOW_BACKFILL_POLICY == "manual-only-no-automatic-catchup"
    assert AIRFLOW_DEPENDS_ON_PAST is False
    assert AIRFLOW_RETRIES == 2
    assert DEFAULT_ARGS["depends_on_past"] is AIRFLOW_DEPENDS_ON_PAST
    assert DEFAULT_ARGS["retries"] == AIRFLOW_RETRIES
    assert DEFAULT_ARGS["retry_delay"] == AIRFLOW_RETRY_DELAY


def test_airflow_task_commands_do_not_embed_secret_values() -> None:
    secret_markers = ("password", "secret", "token", "salt", "dsn", "basic_auth")

    for task in USER_PROFILE_INGESTION_TASKS:
        command = task.bash_command.lower()
        assert all(marker not in command for marker in secret_markers)


def test_airflow_dag_import_builds_expected_dag_when_airflow_is_available(monkeypatch: Any) -> None:
    install_fake_airflow_modules(monkeypatch)

    reloaded_airflow_dags = importlib.reload(airflow_dags)
    dag = reloaded_airflow_dags.dag

    assert isinstance(dag, FakeDag)
    assert dag.kwargs["dag_id"] == AIRFLOW_DAG_ID
    assert dag.kwargs["schedule"] == AIRFLOW_SCHEDULE
    assert dag.kwargs["catchup"] is AIRFLOW_CATCHUP
    assert dag.kwargs["default_args"] == DEFAULT_ARGS
    assert dag.kwargs["tags"] == list(AIRFLOW_TAGS)
    assert [task.task_id for task in dag.tasks] == [
        "start",
        "finish",
        "register_schema_registry_contracts",
        "run_user_profile_ingestion",
    ]
    assert dag.tasks[0].downstream_task_ids == ["register_schema_registry_contracts"]
    assert dag.tasks[2].downstream_task_ids == ["run_user_profile_ingestion"]
    assert dag.tasks[3].downstream_task_ids == ["finish"]

    importlib.reload(airflow_dags)


def test_airflow_is_optional_for_default_developer_environment() -> None:
    if importlib.util.find_spec("airflow") is not None:
        return

    assert importlib.reload(airflow_dags).dag is None
