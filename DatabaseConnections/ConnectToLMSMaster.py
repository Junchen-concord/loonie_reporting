from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from scripts.logging_utils import setup_logger

LOGGER = setup_logger(__name__, "db_connector")


def _configure_odbc_ini_for_homebrew_macos() -> None:
    """Ensure unixODBC config is discoverable on macOS."""
    if os.name != "posix":
        return
    try:
        if os.uname().sysname.lower() != "darwin":
            return
    except Exception:
        return

    hb_etc = Path("/opt/homebrew/etc")
    alt_etc = Path("/usr/local/etc")
    etc_dir = hb_etc if hb_etc.exists() else alt_etc
    if not (etc_dir / "odbcinst.ini").exists():
        return

    os.environ.setdefault("ODBCSYSINI", str(etc_dir))
    os.environ.setdefault("ODBCINSTINI", "odbcinst.ini")
    os.environ.setdefault("ODBCINI", str(etc_dir / "odbc.ini"))


class ConnectToLMSMaster:
    """
    DB connector mirroring the referenced alert pipeline pattern:
    - Build SQLAlchemy+pyodbc engine from .env
    - Execute stored procedures with positional params
    - Return all result sets as pandas DataFrames
    """

    def __init__(self, database: str = "LMSMaster") -> None:
        load_dotenv()
        _configure_odbc_ini_for_homebrew_macos()

        server = os.getenv("DB_SERVER", "")
        username = os.getenv("DB_USERNAME", "") or os.getenv("DB_USER", "")
        password = os.getenv("DB_PASSWORD", "")
        driver = os.getenv("ODBC_DRIVER_VERSION", "ODBC Driver 18 for SQL Server")

        if not server or not username or not password:
            raise ValueError(
                "Missing DB credentials. Set DB_SERVER, DB_USERNAME (or DB_USER), and DB_PASSWORD in .env."
            )

        conn_str = (
            f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};"
            "TrustServerCertificate=yes"
        )
        LOGGER.info("Initializing DB engine for database=%s driver=%s", database, driver)
        odbc_connect = quote_plus(conn_str)
        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={odbc_connect}")

    @staticmethod
    def _yield_result_sets(cursor):
        while True:
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                rows = cursor.fetchall()
                yield pd.DataFrame.from_records(rows, columns=cols)
            if not cursor.nextset():
                break

    def callStoredProcedure(self, query: str, *params: Any) -> list[pd.DataFrame]:
        """
        Execute a stored procedure call string, e.g.
        `EXEC USP_SystemAlert_AcceptCountProcedure ?, ?`
        and return all result sets.
        """
        LOGGER.info("Executing stored procedure: %s", query)
        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result_sets = list(self._yield_result_sets(cursor))
            LOGGER.info("Stored procedure returned %d result set(s)", len(result_sets))
            return result_sets
        finally:
            conn.close()

    def callQuery(self, query: str, *params: Any) -> pd.DataFrame:
        """
        Execute a parameterized SQL query and return a single DataFrame.
        """
        LOGGER.info("Executing query call (params=%d)", len(params))
        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            if not cursor.description:
                LOGGER.info("Query returned no tabular result.")
                return pd.DataFrame()
            cols = [c[0] for c in cursor.description]
            rows = cursor.fetchall()
            out = pd.DataFrame.from_records(rows, columns=cols)
            LOGGER.info("Query returned %d row(s)", len(out))
            return out
        finally:
            conn.close()

