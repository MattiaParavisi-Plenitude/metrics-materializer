import os
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient, config
from databricks.sdk.service import sql as sql_service


def _get_warehouse_id():
    wid = (os.getenv('DATABRICKS_WAREHOUSE_ID', '') or '').strip()
    if not wid:
        # Fallback to application config default for local runs.
        try:
            from config import Config
            wid = (getattr(Config, 'DATABRICKS_WAREHOUSE_ID', '') or '').strip()
        except Exception:
            wid = ''
    if not wid:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID must be set in environment or config.py "
            "(expected a Databricks SQL Warehouse ID)."
        )
    return wid


def _get_config():
    # Prefer explicit env/app credentials, then local databricks.cfg for local runs.
    host = (os.getenv("DATABRICKS_HOST", "") or "").strip()
    token = (os.getenv("DATABRICKS_TOKEN", "") or "").strip()

    if not host or not token:
        try:
            from config import Config
            host = host or (getattr(Config, "DATABRICKS_HOST", "") or "").strip()
            token = token or (getattr(Config, "DATABRICKS_TOKEN", "") or "").strip()
        except Exception:
            pass

    if host and token:
        return config.Config(host=host, token=token)

    local_cfg = Path(__file__).resolve().parent.parent / "databricks.cfg"
    if local_cfg.exists() and not os.getenv("DATABRICKS_CONFIG_FILE"):
        os.environ["DATABRICKS_CONFIG_FILE"] = str(local_cfg)

    env_host = (os.getenv("DATABRICKS_HOST", "") or "").strip()
    env_token = (os.getenv("DATABRICKS_TOKEN", "") or "").strip()
    should_mask_partial_env = bool(env_host) != bool(env_token)
    if not should_mask_partial_env:
        return config.Config()

    # Prevent partial env credentials from overriding local cfg/SDK fallback resolution.
    previous_host = os.environ.pop("DATABRICKS_HOST", None)
    previous_token = os.environ.pop("DATABRICKS_TOKEN", None)
    try:
        return config.Config()
    finally:
        if previous_host is not None:
            os.environ["DATABRICKS_HOST"] = previous_host
        if previous_token is not None:
            os.environ["DATABRICKS_TOKEN"] = previous_token


def _get_workspace_client():
    return WorkspaceClient(config=_get_config())


def get_workspace_client():
    return _get_workspace_client()


def _get_status_value(response):
    status = getattr(response, "status", None)
    state = getattr(status, "state", None)
    return getattr(state, "value", state)


def _get_status_error(response):
    status = getattr(response, "status", None)
    error = getattr(status, "error", None)
    message = getattr(error, "message", None)
    return message or str(error or "Unknown Databricks SQL statement error")


def _wait_for_statement(client, response, timeout_seconds=120):
    deadline = time.time() + timeout_seconds
    while _get_status_value(response) in {"PENDING", "RUNNING"}:
        if time.time() >= deadline:
            statement_id = getattr(response, "statement_id", "unknown")
            raise TimeoutError(f"Timed out waiting for statement {statement_id}")
        time.sleep(1)
        response = client.statement_execution.get_statement(response.statement_id)
    return response


def _raise_on_terminal_failure(response):
    state = _get_status_value(response)
    if state in {"FAILED", "CANCELED", "CLOSED"}:
        raise RuntimeError(_get_status_error(response))


def _get_column_names(response):
    manifest = getattr(response, "manifest", None)
    schema = getattr(manifest, "schema", None)
    columns = getattr(schema, "columns", None) or []
    return [column.name for column in columns]


def _collect_result_rows(client, response, max_rows):
    rows = []
    result = getattr(response, "result", None)
    while result is not None:
        data = getattr(result, "data_array", None) or []
        rows.extend(data)
        if len(rows) >= max_rows:
            return rows[:max_rows]
        next_chunk_index = getattr(result, "next_chunk_index", None)
        if next_chunk_index is None:
            break
        result = client.statement_execution.get_statement_result_chunk_n(
            statement_id=response.statement_id,
            chunk_index=next_chunk_index,
        )
    return rows


def _execute_inline_statement(sql_string, row_limit=None):
    client = _get_workspace_client()
    response = client.statement_execution.execute_statement(
        statement=sql_string,
        warehouse_id=_get_warehouse_id(),
        disposition=sql_service.Disposition.INLINE,
        format=sql_service.Format.JSON_ARRAY,
        on_wait_timeout=sql_service.ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        wait_timeout="30s",
        row_limit=row_limit,
    )
    response = _wait_for_statement(client, response)
    _raise_on_terminal_failure(response)
    return client, response


def execute_query(sql_string, max_rows=1000):
    """Execute a SQL query against Databricks and return results as list of dicts."""
    try:
        client, response = _execute_inline_statement(sql_string, row_limit=max_rows)
        columns = _get_column_names(response)
        rows = _collect_result_rows(client, response, max_rows)
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"Query error: {e}")
        return []


def execute_statement(sql_string):
    """Execute a SQL statement (no result expected)."""
    _execute_inline_statement(sql_string)
