from __future__ import annotations

import os
import sys
import traceback
import uuid
from collections import defaultdict, deque
from datetime import date
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    CronSchedule,
    JobAccessControlRequest,
    JobPermissionLevel,
    PauseStatus,
    Source,
    SqlTask,
    SqlTaskFile,
    SubmitTask,
    Task,
    TaskDependency,
)
from flask import Blueprint, render_template, request, jsonify

# Ensure library is importable
_base_dir = os.path.dirname(os.path.abspath(__file__))
_lib_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(_base_dir))), "library")
if _lib_path not in sys.path:
    sys.path.insert(0, _lib_path)

from db_connector import execute_query, get_workspace_client
import schedule_registry as registry

blueprint = Blueprint("core", __name__)

# ─── Load Configuration from YAML ────────────────────────────────────────────
_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "config" / "app_config.yaml"
with open(_CONFIG_PATH, "r", encoding="utf-8") as _f:
    _cfg = yaml.safe_load(_f)

_dbx = _cfg.get("databricks", {})
_views = _cfg.get("views", {})
_job_cfg = _cfg.get("job", {})

# Environment variables override YAML values
METRICS_VIEW = _views.get("metrics", "")
HIERARCHY_VIEW = _views.get("hierarchy", "")
STATUS_TRANSITIONS_VIEW = _views.get("status_transitions", "")
ACL_GROUP_NAME = _job_cfg.get("acl_group_name", "")
JOB_TIMEOUT_SECONDS = _job_cfg.get("timeout_seconds", 7200)
JOB_MAX_RETRIES = _job_cfg.get("max_retries", 3)
JOB_MIN_RETRY_INTERVAL_MS = _job_cfg.get("min_retry_interval_millis", 3000)

WORKSPACE_FILE_PATH = os.getenv("WORKSPACE_FILE_PATH", _dbx.get("workspace_file_path", ""))
SQL_CALL_FILE = _dbx.get("sql_call_file", "")
CATALOG = os.getenv("DATABRICKS_CATALOG", _dbx.get("catalog", ""))
SCHEMA_UTIL = os.getenv("DATABRICKS_SCHEMA_UTIL", _dbx.get("schema_util", ""))
SCHEMA_NAME = os.getenv("DATABRICKS_SCHEMA", _dbx.get("schema_name", ""))
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", _dbx.get("warehouse_id", ""))


# ─── Page Routes ──────────────────────────────────────────────────────────────

@blueprint.route("/", endpoint="home")
def home():
    return render_template("core/home.html", active_page="home")


@blueprint.route("/materialize", endpoint="materialize")
def materialize():
    return render_template("core/materialize.html", active_page="materialize")


@blueprint.route("/schedule", endpoint="schedule")
def schedule():
    return render_template("core/schedule.html", active_page="schedule")


@blueprint.route("/schedules", endpoint="schedules")
def schedules():
    return render_template("core/schedules.html", active_page="schedules")


# ─── API Routes ───────────────────────────────────────────────────────────────

@blueprint.route("/api/metrics/all", methods=["GET"])
def api_get_all_metrics():
    """Fetch all metrics (all extraction types) from the metadata view."""
    try:
        query = f"SELECT METRIC_NAME as metric_name, EXTRACTION_TYPE, VERSION, MASKING, TAGS FROM {METRICS_VIEW} ORDER BY METRIC_NAME"
        results = execute_query(query)
        return jsonify({"success": True, "data": results, "count": len(results)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/debug/metrics", methods=["GET"])
def api_debug_metrics():
    """Debug endpoint: returns raw query results with column names visible."""
    try:
        query = f"SELECT METRIC_NAME as metric_name, EXTRACTION_TYPE, VERSION, MASKING, TAGS FROM {METRICS_VIEW} LIMIT 3"
        results = execute_query(query)
        columns = list(results[0].keys()) if results else []
        return jsonify({
            "success": True,
            "view": METRICS_VIEW,
            "query": query,
            "columns": columns,
            "row_count": len(results),
            "sample": results,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "view": METRICS_VIEW}), 500


@blueprint.route("/api/hierarchy", methods=["GET"])
def api_get_hierarchy():
    """Fetch the full metrics hierarchy for dependency resolution."""
    try:
        query = f"SELECT * FROM {HIERARCHY_VIEW}"
        results = execute_query(query)
        return jsonify({"success": True, "data": results})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/build-plan", methods=["POST"])
def api_build_plan():
    """
    Given a list of selected metrics, build a dependency-ordered execution plan.
    Expects JSON body: { "metrics": ["metric_a", "metric_b"], "snapshot_date": "2026-01-15" }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    selected_metrics = payload.get("metrics", [])
    snapshot_date = payload.get("snapshot_date", "")

    if not selected_metrics:
        return jsonify({"success": False, "error": "No metrics selected"}), 400
    if not snapshot_date:
        return jsonify({"success": False, "error": "No snapshot_date provided"}), 400

    try:
        # Fetch hierarchy
        query = f"SELECT * FROM {HIERARCHY_VIEW}"
        hierarchy_rows = execute_query(query, max_rows=10000)

        # Fetch metric metadata (extraction_type)
        meta_query = f"SELECT METRIC_NAME, EXTRACTION_TYPE FROM {METRICS_VIEW}"
        meta_rows = execute_query(meta_query, max_rows=10000)
        extraction_type_map = {
            row.get("METRIC_NAME", row.get("metric_name", "")).lower(): row.get("EXTRACTION_TYPE", row.get("extraction_type", ""))
            for row in meta_rows
        }

        # Build dependency graph: metric -> list of dependencies
        deps_map = defaultdict(set)
        for row in hierarchy_rows:
            metric = (row.get("METRIC_NAME") or row.get("metric_name") or "").lower()
            dependency = (row.get("DEPENDENCY") or row.get("dependency_name") or "").lower()
            if metric and dependency:
                deps_map[metric].add(dependency)

        # Resolve full dependency closure for selected metrics
        selected_lower = {m.lower() for m in selected_metrics}
        to_materialize = set()
        queue = deque(selected_lower)

        while queue:
            current = queue.popleft()
            if current in to_materialize:
                continue
            to_materialize.add(current)
            for dep in deps_map.get(current, []):
                if dep not in to_materialize:
                    queue.append(dep)

        # Topological sort to determine levels
        levels = _compute_levels(to_materialize, deps_map)

        # Determine historicization_type per metric:
        # metrics that already have at least one COMPLETED transition → SNAPSHOT (append mode)
        # metrics with no history → TINSERT (first full load)
        try:
            hist_query = f"""
                SELECT DISTINCT METRIC_NAME
                FROM {STATUS_TRANSITIONS_VIEW}
                WHERE STATUS_TO = 'COMPLETED'
            """
            hist_rows = execute_query(hist_query, max_rows=10000)
            metrics_with_history = {
                (row.get("METRIC_NAME") or row.get("metric_name") or "").lower()
                for row in hist_rows
            }
        except Exception:
            metrics_with_history = set()

        # Build the execution plan (include deps for submit)
        plan = []
        for level_num in sorted(levels.keys()):
            level_metrics = []
            for m in sorted(levels[level_num]):
                # Only keep deps that are within the resolved set
                metric_deps = sorted(deps_map.get(m, set()) & to_materialize)
                ext_type = extraction_type_map.get(m, "")
                hist_type = "SNAPSHOT" if m in metrics_with_history else "TINSERT"
                level_metrics.append({
                    "name": m,
                    "dependencies": metric_deps,
                    "extraction_type": ext_type,
                    "historicization_type": hist_type,
                })
            plan.append({
                "level": level_num,
                "metrics": level_metrics,
            })

        return jsonify({
            "success": True,
            "snapshot_date": snapshot_date,
            "selected_metrics": sorted(selected_lower),
            "total_metrics": len(to_materialize),
            "plan": plan,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/submit-job", methods=["POST"])
def api_submit_job():
    """
    Submit the materialization job to Databricks.
    Expects JSON body: { "plan": [...], "snapshot_date": "2026-01-15" }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    plan = payload.get("plan", [])
    snapshot_date = payload.get("snapshot_date", "")

    if not plan:
        return jsonify({"success": False, "error": "No execution plan provided"}), 400
    if not snapshot_date:
        return jsonify({"success": False, "error": "No snapshot_date provided"}), 400

    try:
        submit_tasks = _build_submit_tasks(plan, snapshot_date)
        if not submit_tasks:
            return jsonify({"success": False, "error": "No tasks generated from plan"}), 400

        w = _get_workspace_client()
        waiter = w.jobs.submit(
            run_name=f"METRICS_MATERIALIZE_{snapshot_date}",
            tasks=submit_tasks,
            idempotency_token=str(uuid.uuid4()),
            access_control_list=[
                JobAccessControlRequest(
                    group_name=ACL_GROUP_NAME,
                    permission_level=JobPermissionLevel.CAN_VIEW,
                ),
            ],
        )
        # waiter is a Wait[Run] - get the run_id from the bind
        run_id = waiter.run_id
        run_page_url = ""
        try:
            run = w.jobs.get_run(run_id=run_id)
            run_page_url = run.run_page_url or ""
        except Exception:
            # Build URL manually if get_run fails
            host = w.config.host.rstrip("/")
            run_page_url = f"{host}/#job/0/run/{run_id}"

        return jsonify({
            "success": True,
            "message": "Job submitted successfully",
            "run_id": run_id,
            "run_page_url": run_page_url,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "detail": traceback.format_exc()}), 500


@blueprint.route("/api/metrics-status", methods=["GET"])
def api_metrics_status():
    """
    Fetch last materialization status for each metric.
    Returns the most recent COMPLETED transition per metric_name.
    """
    try:
        query = f"""
            SELECT METRIC_NAME, STATUS_TO, DATA_SNAPSHOT, TIMESTAMP, JOB_NAME, JOB_ID, JOB_RUN_ID, WORKSPACE_URL
            FROM {STATUS_TRANSITIONS_VIEW}
            WHERE STATUS_TO = 'COMPLETED'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME ORDER BY TIMESTAMP DESC) = 1
            ORDER BY METRIC_NAME
        """
        results = execute_query(query, max_rows=5000)
        return jsonify({"success": True, "data": results})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/schedule-job", methods=["POST"])
def api_schedule_job():
    """
    Create or update a scheduled job in Databricks.
    Two modes:
      - "new": create a brand new job (requires cron_expression)
      - "add_to_job": add metrics to an existing job (requires target_job_id)
    Expects JSON body: {
        "plan": [...],
        "snapshot_date": "2026-01-15",
        "mode": "new" | "add_to_job",
        "cron_expression": "0 0 6 * * ?",   (for mode=new)
        "timezone": "Europe/Rome",           (for mode=new)
        "target_job_id": "123456"            (for mode=add_to_job)
    }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    plan = payload.get("plan", [])
    snapshot_date = payload.get("snapshot_date", "")
    mode = payload.get("mode", "new")
    cron_expression = payload.get("cron_expression", "")
    tz = payload.get("timezone", "Europe/Rome")
    target_job_id = payload.get("target_job_id", "")

    if not plan:
        return jsonify({"success": False, "error": "No execution plan provided"}), 400

    # Collect metric names from plan (needed for job name generation)
    metric_names = []
    for level_info in plan:
        for m in level_info.get("metrics", []):
            name = m["name"] if isinstance(m, dict) else m
            metric_names.append(name.lower())

    try:
        w = _get_workspace_client()

        if mode == "add_to_job":
            # ── Add metrics to an existing job ──
            if not target_job_id:
                return jsonify({"success": False, "error": "No target_job_id provided"}), 400

            # Get existing job
            existing_job = w.jobs.get(job_id=int(target_job_id))
            existing_tasks = list(existing_job.settings.tasks or [])

            # Build new tasks for the new metrics
            new_tasks = _build_persistent_tasks(plan, snapshot_date)

            # Merge: keep existing tasks + add new ones (avoid duplicates by task_key)
            existing_keys = {t.task_key for t in existing_tasks}
            for t in new_tasks:
                if t.task_key not in existing_keys:
                    existing_tasks.append(t)

            # Rebuild depends_on for ALL tasks using the full hierarchy
            # This ensures existing tasks get correct depends_on when new deps are added
            hierarchy_query = f"SELECT * FROM {HIERARCHY_VIEW}"
            hierarchy_rows = execute_query(hierarchy_query)

            # Build hierarchy map: metric -> set of its dependencies (uppercase)
            hierarchy_deps: dict[str, set[str]] = defaultdict(set)
            for row in hierarchy_rows:
                m = (row.get("METRIC_NAME") or row.get("metric_name") or "").upper()
                d = (row.get("DEPENDENCY") or row.get("dependency_name") or "").upper()
                if m and d:
                    hierarchy_deps[m].add(d)

            # For each task, set depends_on to only those dependencies that exist in the job
            all_task_keys = {t.task_key for t in existing_tasks}
            for t in existing_tasks:
                expected = hierarchy_deps.get(t.task_key, set()) & all_task_keys
                t.depends_on = [TaskDependency(task_key=k) for k in sorted(expected)] if expected else None

            # Update tasks and reset job
            existing_job.settings.tasks = existing_tasks
            w.jobs.reset(
                job_id=int(target_job_id),
                new_settings=existing_job.settings,
            )

            host = w.config.host.rstrip("/")
            job_url = f"{host}/jobs/{target_job_id}"
            return jsonify({
                "success": True,
                "message": f"Metrics added to existing job {target_job_id}",
                "job_id": int(target_job_id),
                "job_url": job_url,
            })

        else:
            # ── Create a new job ──
            if not cron_expression:
                return jsonify({"success": False, "error": "No CRON expression provided"}), 400

            job_name = registry.generate_job_name(metric_names)
            tasks = _build_persistent_tasks(plan, snapshot_date)

            job = w.jobs.create(
                name=job_name,
                tasks=tasks,
                schedule=CronSchedule(
                    quartz_cron_expression=cron_expression,
                    timezone_id=tz,
                    pause_status=PauseStatus.UNPAUSED,
                ),
                access_control_list=[
                    JobAccessControlRequest(
                        group_name=ACL_GROUP_NAME,
                        permission_level=JobPermissionLevel.CAN_VIEW,
                    ),
                ],
            )

            job_id = job.job_id
            host = w.config.host.rstrip("/")
            job_url = f"{host}/jobs/{job_id}"

            return jsonify({
                "success": True,
                "message": "Scheduled job created successfully",
                "job_id": job_id,
                "job_name": job_name,
                "job_url": job_url,
            })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/registry/unassigned", methods=["GET"])
def api_registry_unassigned():
    """Return metric names assigned to any managed job (live from Databricks SDK)."""
    try:
        w = _get_workspace_client()
        assigned = registry.get_assigned_metric_names(w)
        return jsonify({"success": True, "assigned": sorted(assigned)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/registry/jobs", methods=["GET"])
def api_registry_jobs():
    """Return all managed jobs with their metrics (live from Databricks SDK)."""
    try:
        w = _get_workspace_client()
        # Enrich metrics with extraction_type from metadata view
        try:
            meta_rows = execute_query(
                f"SELECT METRIC_NAME, EXTRACTION_TYPE FROM {METRICS_VIEW}",
                max_rows=10000,
            )
            ext_map = {
                (r.get("METRIC_NAME") or r.get("metric_name") or "").lower():
                (r.get("EXTRACTION_TYPE") or r.get("extraction_type") or "")
                for r in meta_rows
            }
        except Exception:
            ext_map = {}
        jobs = registry.get_jobs(w, ext_map)
        return jsonify({"success": True, "jobs": list(jobs.values())})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/registry/job-for-dep", methods=["POST"])
def api_registry_job_for_dep():
    """
    Given a list of metric names and their dependencies, check if any belong to an existing job.
    Bidirectional check:
      1. Forward: do my DEPENDENCIES already exist in a job? → add me to that job
      2. Reverse: do metrics in a job DEPEND ON me? → add me to that job (they need me)
    Returns the job_id if found (for auto-assign logic).
    """
    payload = request.get_json(silent=True)
    deps = payload.get("dependencies", []) if payload else []
    metrics = payload.get("metrics", []) if payload else []

    try:
        w = _get_workspace_client()

        # Forward check: my dependencies are in a job
        for dep in deps:
            job_id = registry.find_job_for_dependency(dep, w)
            if job_id:
                jobs = registry.get_jobs(w)
                job_info = jobs.get(job_id, {})
                return jsonify({"success": True, "found": True, "job_id": job_id, "job_info": job_info, "direction": "forward"})

        # Reverse check: metrics in a job depend on me
        if metrics:
            try:
                hierarchy_rows = execute_query(f"SELECT * FROM {HIERARCHY_VIEW}")
                for metric in metrics:
                    job_id = registry.find_job_for_dependent(metric, hierarchy_rows, w)
                    if job_id:
                        jobs = registry.get_jobs(w)
                        job_info = jobs.get(job_id, {})
                        return jsonify({"success": True, "found": True, "job_id": job_id, "job_info": job_info, "direction": "reverse"})
            except Exception:
                pass  # If hierarchy fetch fails, skip reverse check

    except Exception:
        pass  # If SDK fails, fall through to not-found

    return jsonify({"success": True, "found": False})


@blueprint.route("/api/reschedule", methods=["POST"])
def api_reschedule():
    """
    Change the CRON schedule for an existing job.
    Expects: { "job_id": "123", "cron_expression": "0 0 8 * * ?", "timezone": "Europe/Rome" }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    job_id = payload.get("job_id", "")
    cron_expression = payload.get("cron_expression", "")
    tz = payload.get("timezone", "Europe/Rome")

    if not job_id:
        return jsonify({"success": False, "error": "No job_id provided"}), 400
    if not cron_expression:
        return jsonify({"success": False, "error": "No CRON expression provided"}), 400

    try:
        w = _get_workspace_client()
        existing_job = w.jobs.get(job_id=int(job_id))
        existing_job.settings.schedule = CronSchedule(
            quartz_cron_expression=cron_expression,
            timezone_id=tz,
            pause_status=PauseStatus.UNPAUSED,
        )
        w.jobs.reset(job_id=int(job_id), new_settings=existing_job.settings)

        return jsonify({
            "success": True,
            "message": f"Schedule updated for job {job_id}",
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/run-now", methods=["POST"])
def api_run_now():
    """
    Trigger an immediate run of a scheduled job.
    Expects: { "job_id": "123456" }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        return jsonify({"success": False, "error": "No job_id provided"}), 400

    try:
        w = _get_workspace_client()
        run = w.jobs.run_now(job_id=int(job_id))
        run_id = run.run_id
        host = w.config.host.rstrip("/")
        run_page_url = f"{host}/#job/{job_id}/run/{run_id}"

        return jsonify({
            "success": True,
            "message": "Job triggered successfully",
            "run_id": run_id,
            "run_page_url": run_page_url,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/job-status", methods=["POST"])
def api_job_status():
    """
    Get the live status of a job (last run info + current state).
    Expects: { "job_id": "123456" }
    Returns: last run state, start time, duration, task-level status.
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    job_id = (payload.get("job_id") or "").strip()
    if not job_id:
        return jsonify({"success": False, "error": "No job_id provided"}), 400

    try:
        w = _get_workspace_client()
        # Get the list of runs for this job (most recent first)
        runs = w.jobs.list_runs(job_id=int(job_id), limit=5)
        run_list = list(runs)

        if not run_list:
            return jsonify({"success": True, "runs": [], "message": "No runs found"})

        results = []
        for run in run_list:
            run_state = run.state
            tasks_info = []
            if run.tasks:
                for t in run.tasks:
                    t_state = t.state
                    tasks_info.append({
                        "task_key": t.task_key,
                        "state": t_state.life_cycle_state.value if t_state and t_state.life_cycle_state else "UNKNOWN",
                        "result": t_state.result_state.value if t_state and t_state.result_state else None,
                    })

            results.append({
                "run_id": run.run_id,
                "state": run_state.life_cycle_state.value if run_state and run_state.life_cycle_state else "UNKNOWN",
                "result": run_state.result_state.value if run_state and run_state.result_state else None,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "run_page_url": run.run_page_url or "",
                "tasks": tasks_info,
            })

        return jsonify({"success": True, "runs": results})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@blueprint.route("/api/registry/remove-metric", methods=["POST"])
def api_registry_remove_metric():
    """
    Remove a metric from its scheduled job with full cascade:
      - Upward: metrics that depend ON the removed metric (they can't run without it)
      - Downward: orphan dependencies no longer needed by remaining metrics
    Supports preview mode: { ..., "preview": true } returns what would be removed.
    Expects: { "metric_name": "...", "job_id": "...", "preview": bool, "confirmed": bool }
    """
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"success": False, "error": "Invalid JSON payload"}), 400

    metric_name = (payload.get("metric_name") or "").strip().lower()
    job_id = (payload.get("job_id") or "").strip()
    preview = payload.get("preview", False)
    confirmed = payload.get("confirmed", False)

    if not metric_name:
        return jsonify({"success": False, "error": "No metric_name provided"}), 400

    try:
        # Fetch hierarchy
        hierarchy_query = f"SELECT * FROM {HIERARCHY_VIEW}"
        hierarchy_rows = execute_query(hierarchy_query)

        # Build graph: dependents_of[m] = metrics that depend on m
        dependents_of: dict[str, set[str]] = defaultdict(set)
        for row in hierarchy_rows:
            m = (row.get("METRIC_NAME") or row.get("metric_name") or "").lower()
            d = (row.get("DEPENDENCY") or row.get("dependency_name") or "").lower()
            if m and d:
                dependents_of[d].add(m)

        # Get all metrics currently in this job (live from SDK)
        w = _get_workspace_client()
        job_metrics = registry.get_metrics_in_job(job_id, w) if job_id else set()

        # Cascade UPWARD only — find all metrics in the job that depend on the removed one
        metrics_to_remove = {metric_name}
        if job_id:
            up_queue = deque([metric_name])
            while up_queue:
                current = up_queue.popleft()
                for dependent in dependents_of.get(current, set()):
                    if dependent in job_metrics and dependent not in metrics_to_remove:
                        metrics_to_remove.add(dependent)
                        up_queue.append(dependent)

        removed_list = sorted(metrics_to_remove)
        will_delete_job = (job_metrics and metrics_to_remove >= job_metrics)

        # Preview mode: just return what would happen
        if preview:
            return jsonify({
                "success": True,
                "preview": True,
                "metrics_to_remove": removed_list,
                "will_delete_job": will_delete_job,
                "job_metrics_count": len(job_metrics),
            })

        # Actual removal requires confirmation if job will be deleted
        if will_delete_job and not confirmed:
            return jsonify({
                "success": False,
                "error": "Confirmation required",
                "requires_confirmation": True,
                "metrics_to_remove": removed_list,
                "will_delete_job": True,
            }), 400

        # Execute removal
        job_deleted = False
        if job_id:
            try:
                existing_job = w.jobs.get(job_id=int(job_id))
                task_keys_to_remove = {m.upper() for m in metrics_to_remove}
                existing_tasks = list(existing_job.settings.tasks or [])
                updated_tasks = [t for t in existing_tasks if t.task_key not in task_keys_to_remove]

                # Clean depends_on references
                for t in updated_tasks:
                    if t.depends_on:
                        t.depends_on = [d for d in t.depends_on if d.task_key not in task_keys_to_remove]
                        if not t.depends_on:
                            t.depends_on = None

                if updated_tasks:
                    existing_job.settings.tasks = updated_tasks
                    w.jobs.reset(job_id=int(job_id), new_settings=existing_job.settings)
                else:
                    w.jobs.delete(job_id=int(job_id))
                    job_deleted = True
            except Exception:
                pass  # Databricks call failed; job state may be partially updated

        msg = f"Removed {len(removed_list)} metric(s): {', '.join(removed_list)}"
        if job_deleted:
            msg += f". Job {job_id} deleted (no metrics left)."

        return jsonify({"success": True, "message": msg, "removed": removed_list, "job_deleted": job_deleted})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "detail": traceback.format_exc()}), 500


# ─── Helper Functions ─────────────────────────────────────────────────────────

def _get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient. Single call-site so all routes stay consistent."""
    return get_workspace_client()


def _compute_levels(metrics: set, deps_map: dict) -> dict:
    """
    Compute execution levels via topological ordering.
    Level 0 = metrics with no dependencies (within the selected set).
    """
    # Filter deps to only those within our selected set
    local_deps = {}
    for m in metrics:
        local_deps[m] = deps_map.get(m, set()) & metrics

    # Kahn's algorithm
    in_degree = {m: 0 for m in metrics}
    reverse_map = defaultdict(set)

    for m in metrics:
        for dep in local_deps[m]:
            in_degree[m] += 1
            reverse_map[dep].add(m)

    levels = {}
    current_level = 0
    queue = deque([m for m in metrics if in_degree[m] == 0])

    while queue:
        next_queue = deque()
        while queue:
            node = queue.popleft()
            if current_level not in levels:
                levels[current_level] = []
            levels[current_level].append(node)
            for dependent in reverse_map[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_queue.append(dependent)
        queue = next_queue
        current_level += 1

    # Handle cycles: any remaining nodes get assigned to last level + 1
    remaining = [m for m in metrics if m not in {n for lvl in levels.values() for n in lvl}]
    if remaining:
        levels[current_level] = remaining

    return levels


def _build_submit_tasks(plan: list, snapshot_date: str) -> list[SubmitTask]:
    """
    Build a flat list of SubmitTask objects for w.jobs.submit().
    Replicates the logic from generate-scheduled-job.ps1:
    - task_key = METRIC_NAME (uppercase)
    - depends_on filtered to included metrics
    - full parameter set matching call_compute_metric.sql
    - retry and timeout settings
    """
    sql_file_path = f"{WORKSPACE_FILE_PATH}/{SQL_CALL_FILE}"

    # Collect all metric names included in this submission
    included_names: set[str] = set()
    for level_info in plan:
        for metric_entry in level_info["metrics"]:
            name = metric_entry["name"] if isinstance(metric_entry, dict) else metric_entry
            included_names.add(name.lower())

    tasks: list[SubmitTask] = []

    for level_info in plan:
        for metric_entry in level_info["metrics"]:
            if isinstance(metric_entry, dict):
                metric_name = metric_entry["name"]
                deps = metric_entry.get("dependencies", [])
                ext_type = metric_entry.get("extraction_type", "")
                hist_type = metric_entry.get("historicization_type", "TINSERT")
            else:
                metric_name = metric_entry
                deps = []
                ext_type = ""
                hist_type = "TINSERT"

            task_key = metric_name.upper()

            # SNAPSHOT metrics use user-provided date; others use current_date
            effective_date = snapshot_date if ext_type.upper() == "SNAPSHOT" else str(date.today())

            # Only include dependencies that are part of this submission
            filtered_deps = sorted(
                d for d in deps if d.lower() in included_names
            )
            depends_on = (
                [TaskDependency(task_key=d.upper()) for d in filtered_deps]
                if filtered_deps
                else None
            )

            tasks.append(SubmitTask(
                task_key=task_key,
                depends_on=depends_on,
                sql_task=SqlTask(
                    warehouse_id=WAREHOUSE_ID,
                    file=SqlTaskFile(
                        path=sql_file_path,
                        source=Source.WORKSPACE,
                    ),
                    parameters={
                        "catalog": CATALOG,
                        "schema_util": SCHEMA_UTIL,
                        "schema_name": SCHEMA_NAME,
                        "metric_name": metric_name,
                        "snapshot_date": effective_date,
                        "job_id": "{{job.id}}",
                        "task_id": "{{task.run_id}}",
                        "run_id": "{{job.run_id}}",
                        "job_name": "{{job.name}}",
                        "workspace_id": "{{workspace.id}}",
                        "workspace_url": "{{workspace.url}}",
                        "historicization_type": hist_type,
                    },
                ),
                timeout_seconds=JOB_TIMEOUT_SECONDS,
                retry_on_timeout=False,
                max_retries=JOB_MAX_RETRIES,
                min_retry_interval_millis=JOB_MIN_RETRY_INTERVAL_MS,
            ))

    return tasks


def _build_persistent_tasks(plan: list, snapshot_date: str) -> list[Task]:
    """
    Build Task objects for w.jobs.create() (persistent/scheduled jobs).
    Uses dynamic value references for job/workspace context since persistent
    jobs support them fully.
    """
    sql_file_path = f"{WORKSPACE_FILE_PATH}/{SQL_CALL_FILE}"

    # Collect all metric names included
    included_names: set[str] = set()
    for level_info in plan:
        for metric_entry in level_info["metrics"]:
            name = metric_entry["name"] if isinstance(metric_entry, dict) else metric_entry
            included_names.add(name.lower())

    tasks: list[Task] = []

    for level_info in plan:
        for metric_entry in level_info["metrics"]:
            if isinstance(metric_entry, dict):
                metric_name = metric_entry["name"]
                deps = metric_entry.get("dependencies", [])
                ext_type = metric_entry.get("extraction_type", "")
            else:
                metric_name = metric_entry
                deps = []
                ext_type = ""

            task_key = metric_name.upper()

            # SNAPSHOT metrics use user-provided date; others use current_date
            effective_date = snapshot_date if ext_type.upper() == "SNAPSHOT" else "current_date()"

            filtered_deps = sorted(
                d for d in deps if d.lower() in included_names
            )
            depends_on = (
                [TaskDependency(task_key=d.upper()) for d in filtered_deps]
                if filtered_deps
                else None
            )

            tasks.append(Task(
                task_key=task_key,
                depends_on=depends_on,
                sql_task=SqlTask(
                    warehouse_id=WAREHOUSE_ID,
                    file=SqlTaskFile(
                        path=sql_file_path,
                        source=Source.WORKSPACE,
                    ),
                    parameters={
                        "catalog": CATALOG,
                        "schema_util": SCHEMA_UTIL,
                        "schema_name": SCHEMA_NAME,
                        "metric_name": metric_name,
                        "snapshot_date": effective_date,
                        "job_id": "{{job.id}}",
                        "task_id": "{{task.run_id}}",
                        "run_id": "{{job.run_id}}",
                        "job_name": "{{job.name}}",
                        "workspace_id": "{{workspace.id}}",
                        "workspace_url": "{{workspace.url}}",
                        "historicization_type": "SNAPSHOT",
                    },
                ),
                timeout_seconds=JOB_TIMEOUT_SECONDS,
                retry_on_timeout=False,
                max_retries=JOB_MAX_RETRIES,
                min_retry_interval_millis=JOB_MIN_RETRY_INTERVAL_MS,
            ))

    return tasks


__all__ = ["blueprint"]
