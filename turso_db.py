"""
turso_db.py — Synchronous Turso (libSQL) helpers for mcp_server.py.

DB1 = main DB  (identity, screener, tickertape, ohlcv, forensic, alerts)
DB2 = signals  (signals, tickers, memories, concall, credit_ratings)
"""
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB1_URL   = os.getenv("TURSO_DATABASE_URL", "").replace("libsql://", "https://")
DB1_TOKEN = os.getenv("TURSO_TOKEN", "")
DB2_URL   = os.getenv("TURSO_DATABASE_URL2", "").replace("libsql://", "https://")
DB2_TOKEN = os.getenv("TURSO_TOKEN2", "")


# ---------------------------------------------------------------------------
# Internal async runner — works whether or not an event loop already exists
# ---------------------------------------------------------------------------
def _run(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # We're inside an existing event loop (e.g. FastMCP async context).
        # Run in a separate thread to avoid blocking the loop.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro).result()
    else:
        return asyncio.run(coro)


def _to_dicts(result) -> list[dict]:
    cols = result.columns
    return [dict(zip(cols, row)) for row in result.rows]


# ---------------------------------------------------------------------------
# DB2 helpers (concall, credit_ratings, signals, memories)
# ---------------------------------------------------------------------------

def db2_query(sql: str, args: list = None) -> list[dict]:
    """SELECT from DB2, returns list of dicts."""
    import libsql_client
    async def _q():
        async with libsql_client.create_client(url=DB2_URL, auth_token=DB2_TOKEN) as c:
            r = await c.execute(sql, args or [])
            return _to_dicts(r)
    return _run(_q())


def db2_execute(sql: str, args: list = None) -> None:
    """Single write on DB2."""
    import libsql_client
    async def _e():
        async with libsql_client.create_client(url=DB2_URL, auth_token=DB2_TOKEN) as c:
            await c.execute(sql, args or [])
    _run(_e())


def db2_batch(statements: list[tuple]) -> None:
    """
    Batch write to DB2.
    Each tuple: (sql,) or (sql, [args])
    """
    import libsql_client
    async def _b():
        async with libsql_client.create_client(url=DB2_URL, auth_token=DB2_TOKEN) as c:
            stmts = [
                libsql_client.Statement(s[0], list(s[1]) if len(s) > 1 else [])
                for s in statements
            ]
            await c.batch(stmts)
    _run(_b())


# ---------------------------------------------------------------------------
# DB1 helpers (identity, screener, tickertape, ohlcv, forensic, alerts)
# ---------------------------------------------------------------------------

def db1_query(sql: str, args: list = None) -> list[dict]:
    """SELECT from DB1."""
    import libsql_client
    async def _q():
        async with libsql_client.create_client(url=DB1_URL, auth_token=DB1_TOKEN) as c:
            r = await c.execute(sql, args or [])
            return _to_dicts(r)
    return _run(_q())


def db1_execute(sql: str, args: list = None) -> None:
    """Single write on DB1."""
    import libsql_client
    async def _e():
        async with libsql_client.create_client(url=DB1_URL, auth_token=DB1_TOKEN) as c:
            await c.execute(sql, args or [])
    _run(_e())


def db1_batch(statements: list[tuple]) -> None:
    """
    Batch write to DB1.
    Each tuple: (sql,) or (sql, [args])
    """
    import libsql_client
    async def _b():
        async with libsql_client.create_client(url=DB1_URL, auth_token=DB1_TOKEN) as c:
            stmts = [
                libsql_client.Statement(s[0], list(s[1]) if len(s) > 1 else [])
                for s in statements
            ]
            await c.batch(stmts)
    _run(_b())
