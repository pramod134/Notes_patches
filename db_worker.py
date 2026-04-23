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
) -> Any:
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

    Added:
    - retries up to 2 additional times on failure (total 3 attempts)

    Args:
        client: shared AsyncClient
        base_url: Supabase base URL, without trailing slash
        key: Supabase service key / API key
        table: table name to insert into
        payload: dict for single-row insert or list[dict] for bulk insert
        returning: "minimal" or "representation"
        log_label: prefix for logs

    Returns:
        Parsed JSON response when present, else None.

    Raises:
        Exception: if all retry attempts fail
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

    last_error = None

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

            return response.json() if response.text else None

        except Exception as e:
            last_error = e

            print(
                f"[{log_label}][DB_ERROR] action=insert table={table} rows={row_count} "
                f"attempt={attempt} error={str(e)}"
            )

            if attempt == 3:
                raise last_error
