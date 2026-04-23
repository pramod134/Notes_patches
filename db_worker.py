import json
from typing import Any

import httpx


async def db_insert_raw(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    key: str,
    table: str,
    payload: Any,
    returning: str = "minimal",
    log_label: str = "DB",
) -> dict:
    """
    Generic raw insert helper.

    Rules:
    - does not inspect table semantics
    - does not inspect field names
    - does not query before insert
    - does not query after insert
    - sends exactly what it receives
    - waits for DB success/failure
    - logs and returns
    - retries up to 2 additional times on failure (total 3 attempts)
    - never raises; always returns success/failure payload

    Args:
        client: shared AsyncClient
        base_url: Supabase base URL, without trailing slash
        key: Supabase service key / API key
        table: table name to insert into
        payload: dict for single-row insert or list[dict] for bulk insert
        returning: "minimal" or "representation"
        log_label: prefix for logs

    Returns:
        dict:
        - success=True, data=..., status_code=...
        - success=False, error=..., status_code=..., attempts=...
    """
    endpoint = f"{base_url.rstrip('/')}/rest/v1/{table}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"return={returning}",
    }

    row_count = len(payload) if isinstance(payload, list) else 1

    print(
        f"[{log_label}][DB_WRITE] action=insert table={table} rows={row_count} "
        f"payload={json.dumps(payload, default=str, sort_keys=True)}"
    )

    last_error_payload = None

    # Attempt insert up to 3 times (1 initial + 2 retries)
    for attempt in range(1, 4):
        try:
            response = await client.post(
                endpoint,
                headers=headers,
                json=payload,
                timeout=30.0,
            )

            response.raise_for_status()

            print(
                f"[{log_label}][DB_APPLIED] action=insert table={table} rows={row_count} attempt={attempt}"
            )

            return {
                "success": True,
                "status_code": response.status_code,
                "attempts": attempt,
                "data": response.json() if response.text else None,
            }

        except httpx.HTTPStatusError as e:
            response = e.response
            error_body: Any
            try:
                error_body = response.json() if response is not None and response.text else None
            except Exception:
                error_body = response.text if response is not None else str(e)

            last_error_payload = {
                "success": False,
                "status_code": response.status_code if response is not None else None,
                "attempts": attempt,
                "error": error_body,
            }

            print(
                f"[{log_label}][DB_ERROR] action=insert table={table} rows={row_count} "
                f"attempt={attempt} status={response.status_code if response is not None else 'unknown'} "
                f"error={json.dumps(error_body, default=str, sort_keys=True)}"
            )

        except Exception as e:
            last_error_payload = {
                "success": False,
                "status_code": None,
                "attempts": attempt,
                "error": str(e),
            }

            print(
                f"[{log_label}][DB_ERROR] action=insert table={table} rows={row_count} "
                f"attempt={attempt} error={str(e)}"
            )

    return last_error_payload or {
        "success": False,
        "status_code": None,
        "attempts": 3,
        "error": "unknown insert failure",
    }
