import json
from typing import Any

import httpx


async def active_trades_checker(
    client: httpx.AsyncClient,
    *,
    base_url: str,
    key: str,
    strategy: str,
    version: str,
    setup_id: str,
    log_label: str = "DB",
) -> dict:
    """
    Check active_trades for one setup using tags:
    - strategy:<strategy>
    - version:<version>
    - id:<setup_id>

    Returns:
        {
            "success": True/False,
            "managing_present": bool,
            "managing_qty": int,
            "waiting_present": bool,
            "waiting_qty": int,
            "rows_found": int,
            "data": list | None,
            "error": Any | None,
        }
    """
    endpoint = f"{base_url.rstrip('/')}/rest/v1/active_trades"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    params = {
        "select": "id,tags,status,manage,qty",
        "tags": f'cs.{{"strategy:{strategy}","version:{version}","id:{setup_id}"}}',
    }

    print(
        f"[{log_label}][DB_READ] action=select table=active_trades "
        f"strategy={strategy} version={version} setup_id={setup_id} "
        f"params={json.dumps(params, default=str, sort_keys=True)}"
    )

    try:
        response = await client.get(
            endpoint,
            headers=headers,
            params=params,
            timeout=30.0,
        )
        response.raise_for_status()

        rows = response.json()
        rows = rows if isinstance(rows, list) else []

        managing_rows = [
            row for row in rows
            if (
                str(row.get("manage") or "") == "O"
                or (
                    str(row.get("status") or "") == "nt-managing"
                    and str(row.get("manage") or "") == "Y"
                )
            )
        ]

        waiting_rows = [
            row for row in rows
            if (
                str(row.get("status") or "") == "nt-waiting"
                and str(row.get("manage") or "") == "Y"
            )
        ]

        result = {
            "success": True,
            "managing_present": len(managing_rows) > 0,
            "managing_qty": len(managing_rows),
            "waiting_present": len(waiting_rows) > 0,
            "waiting_qty": len(waiting_rows),
            "rows_found": len(rows),
            "data": rows,
            "error": None,
        }

        print(
            f"[{log_label}][DB_RESULT] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"rows_found={result['rows_found']} "
            f"managing_present={result['managing_present']} managing_qty={result['managing_qty']} "
            f"waiting_present={result['waiting_present']} waiting_qty={result['waiting_qty']}"
        )

        return result

    except httpx.HTTPStatusError as e:
        response = e.response
        try:
            error_body: Any = response.json() if response is not None and response.text else None
        except Exception:
            error_body = response.text if response is not None else str(e)

        result = {
            "success": False,
            "managing_present": False,
            "managing_qty": 0,
            "waiting_present": False,
            "waiting_qty": 0,
            "rows_found": 0,
            "data": None,
            "error": error_body,
        }

        print(
            f"[{log_label}][DB_ERROR] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"status={response.status_code if response is not None else 'unknown'} "
            f"error={json.dumps(error_body, default=str, sort_keys=True)}"
        )

        return result

    except Exception as e:
        result = {
            "success": False,
            "managing_present": False,
            "managing_qty": 0,
            "waiting_present": False,
            "waiting_qty": 0,
            "rows_found": 0,
            "data": None,
            "error": str(e),
        }

        print(
            f"[{log_label}][DB_ERROR] action=select table=active_trades "
            f"strategy={strategy} version={version} setup_id={setup_id} "
            f"error={str(e)}"
        )

        return result






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
