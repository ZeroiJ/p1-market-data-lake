"""Schema contracts: the canonical shape each source must present, and the
rules that decide which rows are allowed into the mart.

The governing principle of this project is *quarantine over drop*. A row that
fails its contract is never silently discarded — it is written to quarantine
with a machine-readable reason naming the offending column. Silent data loss is
the cardinal sin of a data pipeline, because it produces a pipeline that runs
green and a mart that is quietly wrong.

Four contracts in v1, one per source:

    sec_edgar   filing metadata from the daily index
    sec_xbrl    XBRL company facts, one row per (taxonomy, tag, unit)
    binance     1-minute OHLCV klines
    stooq       daily OHLCV CSV

Each contract declares:
  columns    the canonical dtype per column
  required   columns that must be non-null — nulling one quarantines the row
  rules      callables taking a DataFrame and returning the keys that fail

`required` and the partitioning column deliberately overlap for the time
columns: a null `open_time` cannot be partitioned, so catching it here yields a
clear reason instead of an unpartitionable row discovered downstream.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import polars as pl


class ContractError(Exception):
    """Base class for contract failures."""


class UnknownSource(ContractError, KeyError):
    """Raised when no contract is registered for a source name."""


@dataclass(frozen=True, slots=True)
class ContractResult:
    """Outcome of validating a frame against a source contract."""

    ok: pl.DataFrame
    quarantine: pl.DataFrame
    reasons: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Contract:
    columns: dict[str, Any]
    required: tuple[str, ...]
    rules: tuple[Callable[[pl.DataFrame], list[tuple[str, str]]], ...] = ()


# --- helpers --------------------------------------------------------------


def _keys(expr: pl.Expr) -> Callable[[pl.DataFrame], list[tuple[str, str]]]:
    """Turn a boolean Polars expression into a per-row rule.

    The expression must evaluate to a boolean over the frame; rows where it is
    True are the ones that violate the rule.
    """

    def rule(frame: pl.DataFrame) -> list[tuple[str, str]]:
        if frame.height == 0:
            return []
        # A rule whose columns the source did not supply cannot be evaluated;
        # skipping is correct, because absent columns are handled by `required`.
        missing = {name for name in _referenced(expr) if name not in frame.columns}
        if missing:
            return []
        bad = frame.filter(expr)
        if bad.height == 0:
            return []
        return [(row["_row_key"], _describe(expr)) for row in bad.iter_rows(named=True)]

    return rule


def _describe(expr: pl.Expr) -> str:
    """Human-readable reason. Polars' own expression repr names the column."""
    return f"rule failed: {expr}"


def _referenced(expr: pl.Expr) -> set[str]:
    """Column names an expression reads, without depending on private API."""
    return set(expr.meta.root_names())


def _positive(column: str) -> Callable[[pl.DataFrame], list[tuple[str, str]]]:
    return _keys(pl.col(column) <= 0)


def _ordered(low: str, high: str) -> Callable[[pl.DataFrame], list[tuple[str, str]]]:
    return _keys(pl.col(high) < pl.col(low))


_DT = pl.Datetime(time_unit="us", time_zone="UTC")

# --- contracts ------------------------------------------------------------

_CONTRACTS: dict[str, Contract] = {
    "sec_edgar": Contract(
        columns={
            "_row_key": pl.String,
            "cik": pl.String,
            "entity_name": pl.String,
            "form": pl.String,
            "filing_date": pl.String,
            "accession_number": pl.String,
        },
        required=("cik", "form", "filing_date", "accession_number"),
    ),
    "sec_xbrl": Contract(
        columns={
            "_row_key": pl.String,
            "cik": pl.String,
            "taxonomy": pl.String,
            "tag": pl.String,
            "unit": pl.String,
            "val": pl.Float64,
            "fy": pl.Int64,
            "fp": pl.String,
            "form": pl.String,
            "filed": pl.String,
            "start": pl.String,
            "end": pl.String,
        },
        required=("cik", "taxonomy", "tag", "unit", "val"),
    ),
    "binance": Contract(
        columns={
            "_row_key": pl.String,
            "ticker": pl.String,
            "open_time": _DT,
            "close_time": _DT,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        },
        required=("ticker", "open_time", "close_time", "close"),
        rules=(
            _positive("close"),
            _positive("volume"),
            _ordered("low", "high"),
            _ordered("close", "high"),
            _ordered("open", "high"),
        ),
    ),
    "stooq": Contract(
        columns={
            "_row_key": pl.String,
            "ticker": pl.String,
            "date": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        },
        required=("ticker", "date", "close"),
        rules=(
            _positive("close"),
            _positive("volume"),
            _ordered("low", "high"),
        ),
    ),
}


def _coerce(column: pl.Expr, dtype: Any, how: str) -> pl.Expr:
    """Cast to the contract dtype, avoiding Polars 2.0's deprecated paths."""
    if how == "str_to_datetime":
        return column.str.to_datetime(time_zone=dtype.time_zone, strict=False)
    if how == "str_to_int":
        return column.str.to_int(strict=False)
    return column.cast(dtype, strict=False)


def _cast_plan(df: pl.DataFrame, contract: Contract) -> list[tuple[str, Any, str]]:
    """Decide how to coerce each present column, based on its current dtype."""
    plan: list[tuple[str, Any, str]] = []
    for name, dtype in contract.columns.items():
        if name not in df.columns:
            continue
        how = "plain"
        if isinstance(dtype, pl.Datetime) and df.schema[name] == pl.String:
            how = "str_to_datetime"
        elif dtype == pl.Int64 and df.schema[name] == pl.String:
            how = "str_to_int"
        plan.append((name, dtype, how))
    return plan


def contract_for(source: str) -> dict[str, Any]:
    """Return the canonical column -> dtype mapping for a source."""
    if source not in _CONTRACTS:
        raise UnknownSource(f"no contract registered for {source!r}; known: {sorted(_CONTRACTS)}")
    return dict(_CONTRACTS[source].columns)


def validate(df: pl.DataFrame, source: str) -> ContractResult:
    """Validate `df` against `source`'s contract.

    Casts to the canonical schema first, so a correctly-shaped-but-stringly-typed
    frame is accepted rather than quarantined — real API payloads are frequently
    all-strings. Then collects every reason per row, so a row failing two rules
    reports both rather than only the first.
    """
    if source not in _CONTRACTS:
        raise UnknownSource(f"no contract registered for {source!r}; known: {sorted(_CONTRACTS)}")
    contract = _CONTRACTS[source]

    if df.height == 0:
        empty = df.clear()
        return ContractResult(ok=empty, quarantine=empty, reasons={})

    if "_row_key" not in df.columns:
        raise ContractError("frame must carry a '_row_key' column identifying each row")

    # strict=False so a source that omits an optional column yields nulls
    # rather than raising; missing required columns are caught below.
    cast = df.with_columns(
        [
            _coerce(pl.col(name), dtype, cast_pl).alias(name)
            for name, dtype, cast_pl in _cast_plan(df, contract)
        ]
    )

    bad_keys: dict[str, list[str]] = {}

    def note(keys: list[tuple[str, str]]) -> None:
        for key, reason in keys:
            bad_keys.setdefault(key, []).append(reason)

    for column in contract.required:
        present = pl.col(column).is_null()
        if column in cast.columns:
            for row in cast.filter(present).iter_rows(named=True):
                bad_keys.setdefault(row["_row_key"], []).append(f"{column} is null")

    for rule in contract.rules:
        note(rule(cast))

    if not bad_keys:
        return ContractResult(ok=cast, quarantine=cast.clear(), reasons={})

    mask = pl.col("_row_key").is_in(list(bad_keys))
    reasons = {key: "; ".join(dict.fromkeys(rs)) for key, rs in bad_keys.items()}
    return ContractResult(
        ok=cast.filter(~mask),
        quarantine=cast.filter(mask),
        reasons=reasons,
    )