"""Airflow DAG for scheduled market-data ingestion.

Kept deliberately thin: one task per source, each calling
`de_lake.pipeline.ingest_range`. All the behaviour worth reading — checkpoint
resume, landing, curation, quarantine — lives in that function and is tested
without a scheduler.

Retries matter more than usual here because SEC rate-limits aggressively.
`retries=2` with a five-minute backoff is the difference between backing off and
hammering an endpoint that has already asked us to stop.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import requests

from de_lake.pipeline import (
    DEFAULT_BINANCE_TICKERS,
    DEFAULT_CIKS,
    DEFAULT_STOOQ_TICKERS,
    SOURCES,
    default_store,
    ingest_range,
    sources_for,
)

DAG_ID = "ingest_market_data"

#: How far back each source is pulled on a run that has no prior state.
DEFAULT_LOOKBACK = timedelta(days=3)

#: Binance and Stooq take separate ticker lists — `aapl.us` is a Stooq symbol and
#: a nonsense Binance one. See de_lake.pipeline.sources_for.
BINANCE_TICKERS = (
    tuple(filter(None, os.environ.get("DE_LAKE_BINANCE_TICKERS", "").split(",")))
    or DEFAULT_BINANCE_TICKERS
)
STOOQ_TICKERS = (
    tuple(filter(None, os.environ.get("DE_LAKE_STOOQ_TICKERS", "").split(",")))
    or DEFAULT_STOOQ_TICKERS
)
CIKS = DEFAULT_CIKS


def _session_factory():
    return requests.Session()


def _make_task(source_name: str):
    def run(**_context):
        store = default_store()
        now = datetime.now(tz=UTC)
        start = (now - DEFAULT_LOOKBACK).date()
        end = now.date()

        built = sources_for(_session_factory, BINANCE_TICKERS, STOOQ_TICKERS, CIKS)
        source = next(s for s in built if s.name == source_name)
        stats = ingest_range(
            source, store, start, end, unit_name=SOURCES[source_name][0]
        )
        print(f"{source_name}: {stats}")
        return stats

    return run


try:  # pragma: no cover - only exercised when Airflow is installed
    from airflow.sdk import DAG
except ImportError:  # pragma: no cover
    DAG = None  # type: ignore[assignment]


if DAG is not None:
    with DAG(
        dag_id=DAG_ID,
        description="Ingest SEC, Binance and Stooq market data into the partitioned Parquet lake.",
        schedule="@daily",
        # Airflow 3 requires a timezone-aware start_date; a naive one is rejected.
        start_date=datetime(2026, 10, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "depends_on_past": False,
        },
        tags=["data-engineering", "ingestion", "p1"],
    ) as dag:
        for name in SOURCES:
            from airflow.providers.standard.operators.python import PythonOperator

            PythonOperator(
                task_id=f"ingest_{name}",
                python_callable=_make_task(name),
                op_kwargs={},
            )
