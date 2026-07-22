"""
Registry helper for metrics schedule management.
Uses the Databricks Jobs SDK as the single source of truth.
Managed jobs are identified by the MTRCS_MATERIALIZE_ name prefix.
"""
from __future__ import annotations

import hashlib
from typing import Optional

from databricks.sdk import WorkspaceClient

JOB_NAME_PREFIX = "MTRCS_MATERIALIZE_"


# ─── Name Generation ─────────────────────────────────────────────────────────

def generate_job_name(metric_names: list[str]) -> str:
    """Generate a deterministic job name: MTRCS_MATERIALIZE_<md5[:8]>"""
    sorted_names = sorted(m.lower() for m in metric_names)
    digest = hashlib.md5("|".join(sorted_names).encode()).hexdigest()[:8]
    return f"{JOB_NAME_PREFIX}{digest}"


# ─── SDK Helpers ─────────────────────────────────────────────────────────────

def _metric_names_from_job(job) -> list[str]:
    """Extract lowercase metric names from a job's task list."""
    names = []
    tasks = (job.settings.tasks or []) if job.settings else []
    for task in tasks:
        params: dict = {}
        if task.sql_task and task.sql_task.parameters:
            params = task.sql_task.parameters
        # Prefer explicit parameter; fall back to task_key (stored uppercase)
        name = params.get("metric_name") or (task.task_key or "").lower()
        if name:
            names.append(name.lower())
    return names


def _get_managed_jobs(w: WorkspaceClient):
    """
    Return all Databricks Job objects whose name starts with JOB_NAME_PREFIX.
    Tries expand_tasks=True first; falls back silently for older SDK versions.
    """
    try:
        all_jobs = list(w.jobs.list(expand_tasks=True))
    except TypeError:
        # expand_tasks not supported by this SDK version
        all_jobs = list(w.jobs.list())

    return [
        j for j in all_jobs
        if j.settings and (j.settings.name or "").startswith(JOB_NAME_PREFIX)
    ]


# ─── Read Operations ──────────────────────────────────────────────────────────

def get_assigned_metric_names(w: WorkspaceClient) -> set[str]:
    """Return the set of all metric names assigned to any managed job."""
    result: set[str] = set()
    for job in _get_managed_jobs(w):
        result.update(_metric_names_from_job(job))
    return result


def get_jobs(w: WorkspaceClient, extraction_type_map: Optional[dict] = None) -> dict[str, dict]:
    """
    Return a dict of job_id -> {job_id, job_name, cron_expression, timezone, metrics: [...]}.

    extraction_type_map: optional dict of metric_name.lower() -> extraction_type string,
    used to enrich each metric entry with its type from the metadata view.
    """
    ext_map = extraction_type_map or {}
    jobs: dict[str, dict] = {}

    for job in _get_managed_jobs(w):
        jid = str(job.job_id)
        schedule = job.settings.schedule if job.settings else None
        metrics = [
            {
                "metric_name": m,
                "extraction_type": ext_map.get(m, ""),
                "assigned_at": "",  # Not persisted in Databricks; omitted
            }
            for m in _metric_names_from_job(job)
        ]
        jobs[jid] = {
            "job_id": jid,
            "job_name": (job.settings.name or "") if job.settings else "",
            "cron_expression": (schedule.quartz_cron_expression or "") if schedule else "",
            "timezone": (schedule.timezone_id or "") if schedule else "",
            "metrics": metrics,
        }

    return jobs


def find_job_for_dependency(dep_name: str, w: WorkspaceClient) -> Optional[str]:
    """
    Return the job_id of the first managed job that contains dep_name as a metric.
    Used for forward auto-assign: 'my dependencies are already in this job'.
    """
    dep_lower = dep_name.lower()
    for job in _get_managed_jobs(w):
        if dep_lower in _metric_names_from_job(job):
            return str(job.job_id)
    return None


def find_job_for_dependent(metric_name: str, hierarchy: list[dict], w: WorkspaceClient) -> Optional[str]:
    """
    Return the job_id if any metric in a managed job depends on metric_name.
    Used for reverse auto-assign: 'metrics in this job need what I'm scheduling'.

    hierarchy: list of {METRIC_NAME, DEPENDENCY} rows from the hierarchy view.
    """
    dependents: set[str] = set()
    for row in hierarchy:
        dep = (row.get("DEPENDENCY") or row.get("dependency_name") or "").lower()
        if dep == metric_name.lower():
            m = (row.get("METRIC_NAME") or row.get("metric_name") or "").lower()
            if m:
                dependents.add(m)

    if not dependents:
        return None

    for job in _get_managed_jobs(w):
        if dependents & set(_metric_names_from_job(job)):
            return str(job.job_id)
    return None


def get_metrics_in_job(job_id: str, w: WorkspaceClient) -> set[str]:
    """Return the set of lowercase metric names in a specific managed job."""
    try:
        job = w.jobs.get(job_id=int(job_id))
        return set(_metric_names_from_job(job))
    except Exception:
        return set()
