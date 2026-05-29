"""
Registry helper for metrics_schedule_registry.
Manages the local CSV that tracks which metrics are assigned to which scheduled jobs.
"""
from __future__ import annotations

import csv
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_CSV_PATH = Path(__file__).parent.parent / "data" / "metrics_schedule_registry.csv"

FIELDS = [
    "METRIC_NAME",
    "JOB_ID",
    "JOB_NAME",
    "CRON_EXPRESSION",
    "TIMEZONE",
    "EXTRACTION_TYPE",
    "ASSIGNED_AT",
]

JOB_NAME_PREFIX = "METR-COMPUTE-"


def _read_all() -> list[dict]:
    """Read all rows from the registry CSV."""
    if not _CSV_PATH.exists():
        return []
    with open(_CSV_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _write_all(rows: list[dict]) -> None:
    """Write all rows to the registry CSV (overwrite)."""
    _CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def get_all_assigned() -> list[dict]:
    """Return all assigned metrics."""
    return _read_all()


def get_assigned_metric_names() -> set[str]:
    """Return the set of metric names currently assigned to any job."""
    return {row["METRIC_NAME"].lower() for row in _read_all() if row.get("METRIC_NAME")}


def get_jobs() -> dict[str, dict]:
    """
    Return a dict of job_id -> {job_name, cron_expression, timezone, metrics: [...]}.
    Grouped view for the manage page.
    """
    rows = _read_all()
    jobs: dict[str, dict] = {}
    for row in rows:
        jid = row.get("JOB_ID", "")
        if not jid:
            continue
        if jid not in jobs:
            jobs[jid] = {
                "job_id": jid,
                "job_name": row.get("JOB_NAME", ""),
                "cron_expression": row.get("CRON_EXPRESSION", ""),
                "timezone": row.get("TIMEZONE", ""),
                "metrics": [],
            }
        jobs[jid]["metrics"].append({
            "metric_name": row.get("METRIC_NAME", ""),
            "extraction_type": row.get("EXTRACTION_TYPE", ""),
            "assigned_at": row.get("ASSIGNED_AT", ""),
        })
    return jobs


def find_job_for_dependency(dep_name: str) -> Optional[str]:
    """
    Given a dependency metric name, return the JOB_ID it belongs to, or None.
    """
    rows = _read_all()
    for row in rows:
        if row.get("METRIC_NAME", "").lower() == dep_name.lower():
            return row.get("JOB_ID", "") or None
    return None


def find_job_for_dependent(metric_name: str, hierarchy: list[dict]) -> Optional[str]:
    """
    Given a metric name, check if any metric in an existing job DEPENDS ON it.
    hierarchy is a list of {METRIC_NAME, DEPENDENCY} rows.
    Returns the JOB_ID if found, or None.
    """
    # Find metrics that depend on metric_name (i.e., metric_name is listed as DEPENDENCY)
    dependents = set()
    for row in hierarchy:
        dep = (row.get("DEPENDENCY") or row.get("dependency_name") or "").lower()
        if dep == metric_name.lower():
            m = (row.get("METRIC_NAME") or row.get("metric_name") or "").lower()
            if m:
                dependents.add(m)

    if not dependents:
        return None

    # Check if any of these dependents are already assigned to a job
    rows = _read_all()
    for row in rows:
        if row.get("METRIC_NAME", "").lower() in dependents:
            return row.get("JOB_ID", "") or None
    return None


def get_metrics_in_job(job_id: str) -> set[str]:
    """Return the set of metric names assigned to a given job."""
    rows = _read_all()
    return {row["METRIC_NAME"].lower() for row in rows if row.get("JOB_ID") == str(job_id)}


def unregister_metrics(metric_names: list[str]) -> int:
    """Remove multiple metrics from the registry. Returns count removed."""
    to_remove = {m.lower() for m in metric_names}
    rows = _read_all()
    remaining = [r for r in rows if r.get("METRIC_NAME", "").lower() not in to_remove]
    removed = len(rows) - len(remaining)
    if removed:
        _write_all(remaining)
    return removed


def generate_job_name(metric_names: list[str]) -> str:
    """
    Generate a deterministic job name: METR-COMPUTE-<md5[:8]>
    The hash is based on sorted metric names at creation time.
    """
    sorted_names = sorted(m.lower() for m in metric_names)
    digest = hashlib.md5("|".join(sorted_names).encode()).hexdigest()[:8]
    return f"{JOB_NAME_PREFIX}{digest}"


def register_metrics(
    metric_names: list[str],
    job_id: str,
    job_name: str,
    cron_expression: str,
    timezone_id: str,
    extraction_types: Optional[dict[str, str]] = None,
) -> None:
    """
    Register a list of metrics as assigned to a job.
    If any metric already exists, it's updated.
    """
    rows = _read_all()
    existing_map = {row["METRIC_NAME"].lower(): i for i, row in enumerate(rows)}
    now = datetime.now(timezone.utc).isoformat()
    ext_types = extraction_types or {}

    for name in metric_names:
        new_row = {
            "METRIC_NAME": name.lower(),
            "JOB_ID": str(job_id),
            "JOB_NAME": job_name,
            "CRON_EXPRESSION": cron_expression,
            "TIMEZONE": timezone_id,
            "EXTRACTION_TYPE": ext_types.get(name.lower(), ""),
            "ASSIGNED_AT": now,
        }
        idx = existing_map.get(name.lower())
        if idx is not None:
            rows[idx] = new_row
        else:
            rows.append(new_row)

    _write_all(rows)


def update_schedule(job_id: str, cron_expression: str, timezone_id: str) -> int:
    """
    Update the CRON and timezone for all metrics in a given job.
    Returns the number of rows updated.
    """
    rows = _read_all()
    count = 0
    for row in rows:
        if row.get("JOB_ID") == str(job_id):
            row["CRON_EXPRESSION"] = cron_expression
            row["TIMEZONE"] = timezone_id
            count += 1
    if count:
        _write_all(rows)
    return count


def unregister_job(job_id: str) -> int:
    """Remove all metrics for a job. Returns count removed."""
    rows = _read_all()
    remaining = [r for r in rows if r.get("JOB_ID") != str(job_id)]
    removed = len(rows) - len(remaining)
    if removed:
        _write_all(remaining)
    return removed


def unregister_metric(metric_name: str) -> bool:
    """Remove a single metric from its scheduled job. Returns True if found and removed."""
    rows = _read_all()
    remaining = [r for r in rows if r.get("METRIC_NAME", "").lower() != metric_name.lower()]
    if len(remaining) < len(rows):
        _write_all(remaining)
        return True
    return False
