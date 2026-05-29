from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Optional

from flask import current_app, has_request_context, request


def get_logger(name: Optional[str] = None) -> logging.Logger:
    base_name = "app"
    try:
        base_name = current_app.config.get("LOGGER_NAME", "app") or "app"
    except RuntimeError:
        pass
    return logging.getLogger(name or str(base_name))


def get_user_email() -> str:
    if not has_request_context():
        return ""
    try:
        email = (request.headers.get("X-Forwarded-Preferred-Email", "") or "").strip()
        username = (request.headers.get("X-Forwarded-Preferred-Username", "") or "").strip()
        return email or username or ""
    except Exception:
        return ""


@lru_cache(maxsize=1)
def _get_databricks_display_name() -> str:
    """Fetch the current user's display name from Databricks (cached for process lifetime)."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        me = w.current_user.me()
        # Prefer formatted name (First Last), fall back to display_name, then userName
        if me.name and (me.name.given_name or me.name.family_name):
            parts = [p for p in [me.name.given_name, me.name.family_name] if p]
            return " ".join(parts)
        return me.display_name or me.user_name or ""
    except Exception:
        return ""


def inject_template_context() -> dict[str, Any]:
    user_email = get_user_email()

    # Try Databricks SDK for real display name first
    display_name = _get_databricks_display_name()

    if not display_name:
        # Fall back to email prefix from proxy headers
        display_name = user_email.split("@")[0] if "@" in user_email else (user_email or "")

    return {
        "with_utente": display_name,
        "user_email": user_email,
    }
