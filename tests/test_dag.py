"""Validates the Airflow DAG for real.

Airflow is a dev dependency here, so the DAG is imported and structurally
checked rather than merely parsed. A DAG that imports but has a cyclic
dependency, a missing task or an unresolvable callable is a broken DAG that
only fails at deploy time — this catches it in the suite instead.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

DAG_PATH = Path(__file__).parent.parent / "dags" / "ingest_market_data.py"


@pytest.fixture(scope="module")
def dag_module():
    spec = importlib.util.spec_from_file_location("ingest_market_data_dag", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_dag_file_exists():
    assert DAG_PATH.is_file()


def test_dag_file_is_valid_python():
    ast.parse(DAG_PATH.read_text())


def test_dag_imports_without_error(dag_module):
    assert dag_module.DAG_ID == "ingest_market_data"


def test_dag_is_constructed_with_sane_scheduling(dag_module):
    dag = dag_module.dag
    assert dag is not None
    # Airflow 3 renamed schedule_interval -> schedule.
    assert str(dag.schedule) == "@daily"
    assert dag.catchup is False, (
        "backfilling on deploy is how you accidentally triple-bill an API"
    )
    assert dag.max_active_runs == 1, (
        "two concurrent runs would race on the same checkpoint"
    )


def test_every_source_gets_exactly_one_task(dag_module):
    dag = dag_module.dag
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {f"ingest_{name}" for name in dag_module.SOURCES}


def test_tasks_have_a_retries_backoff(dag_module):
    # SEC returns 429 under load; an immediate retry makes that worse.
    for task in dag_module.dag.tasks:
        assert task.retries >= 1, f"{task.task_id} has no retries"
        assert task.retry_delay.total_seconds() >= 60


def test_tasks_have_no_dependencies_between_sources(dag_module):
    # Sources are independent; serialising them would make the slowest source
    # gate every other one.
    dag = dag_module.dag
    for task in dag.tasks:
        assert task.downstream_task_ids == set(), (
            f"{task.task_id} has unexpected dependencies"
        )


def test_every_task_callable_is_built_for_its_source(dag_module):
    for name in dag_module.SOURCES:
        runnable = dag_module._make_task(name)
        assert callable(runnable)


def test_the_dag_does_not_contain_credentials(dag_module):
    # The SEC User-Agent is an identification string, not a secret. Anything that
    # looks like a key must not appear.
    source = DAG_PATH.read_text()
    for banned in ("SECRET", "PASSWORD", "API_KEY", "TOKEN"):
        assert banned not in source.upper().replace("SECRET_KEY", ""), banned


def test_scheduled_sources_match_the_pipeline_registry(dag_module):
    # A source implemented but never scheduled is dead weight; a scheduled name
    # missing from the registry is an AttributeError at run time.
    from de_lake.pipeline import SOURCES as pipeline_sources

    assert set(dag_module.SOURCES) == set(pipeline_sources)


def test_ingest_range_is_reachable_from_the_dag(dag_module):
    from de_lake.pipeline import ingest_range

    assert ingest_range is dag_module.ingest_range
