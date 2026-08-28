from __future__ import annotations

import os
from typing import Any

from snowflake_dryrun.plan import parse_explain_payload


def explain_with_snowflake(
    sql: str,
    *,
    account: str,
    user: str,
    password: str | None,
    warehouse: str | None,
    database: str | None,
    schema: str | None,
    role: str | None,
    authenticator: str | None,
) -> dict[str, Any]:
    try:
        import snowflake.connector
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("snowflake-connector-python is not installed") from exc

    connect_kwargs: dict[str, Any] = {
        "account": account,
        "user": user,
    }
    if authenticator:
        connect_kwargs["authenticator"] = authenticator
    if password:
        connect_kwargs["password"] = password
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    if warehouse:
        connect_kwargs["warehouse"] = warehouse
    if database:
        connect_kwargs["database"] = database
    if schema:
        connect_kwargs["schema"] = schema
    if role:
        connect_kwargs["role"] = role

    stmt = sql.strip().rstrip(";")
    explain_sql = f"EXPLAIN USING JSON {stmt}"
    with snowflake.connector.connect(**connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(explain_sql)
            rows = cur.fetchall()
            cols = [c[0] for c in (cur.description or [])]
    if not rows:
        raise RuntimeError("EXPLAIN returned no rows")
    payload: Any
    if len(cols) == 1:
        payload = rows[0][0]
    else:
        payload = [dict(zip(cols, row)) for row in rows]
    parsed = parse_explain_payload(payload)
    parsed.source = "snowflake"
    return parsed.raw | {"_parsed_source": "snowflake"}


def fetch_plan_or_none(sql: str, creds: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    account = creds.get("account") or os.environ.get("SNOWFLAKE_ACCOUNT")
    user = creds.get("user") or os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        return None, None
    try:
        raw = explain_with_snowflake(
            sql,
            account=account,
            user=user,
            password=creds.get("password"),
            warehouse=creds.get("warehouse") or os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=creds.get("database") or os.environ.get("SNOWFLAKE_DATABASE"),
            schema=creds.get("schema_name") or creds.get("schema") or os.environ.get("SNOWFLAKE_SCHEMA"),
            role=creds.get("role") or os.environ.get("SNOWFLAKE_ROLE"),
            authenticator=creds.get("authenticator") or os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
        )
        return raw, None
    except Exception as exc:  # noqa: BLE001 — surface connector errors to the UI
        return None, str(exc)
