# burningdemand_dagster/resources/duckdb_resource.py
import threading
from pathlib import Path
from uuid import uuid4
from typing import Callable, Optional

import duckdb
import pandas as pd
from dagster import ConfigurableResource
from pydantic import Field


class DuckDBResource(ConfigurableResource):
    db_path: str = Field(default="./data/burningdemand.duckdb")
    schema_path: str = Field(default="burningdemand_dagster/sql/schema.sql")

    def setup_for_execution(self, context) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = duckdb.connect(self.db_path)
        self._conn.execute("PRAGMA threads=4;")
        self._create_schema()

    def teardown_after_execution(self, context) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    def _create_schema(self) -> None:
        schema_file = Path(self.schema_path)
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file.resolve()}")

        sql = schema_file.read_text(encoding="utf-8")
        with self._lock:
            self._conn.execute(sql)

    def query_df(self, sql: str, params=None):
        with self._lock:
            return self._conn.execute(sql, params or []).fetchdf()

    def execute(self, sql: str, params=None) -> None:
        with self._lock:
            self._conn.execute(sql, params or [])

    def transaction(self, fn: Callable[[], None]) -> None:
        """
        Run a set of operations in a single transaction under the resource lock.
        """
        with self._lock:
            self._conn.execute("BEGIN;")
            try:
                fn()
                self._conn.execute("COMMIT;")
            except Exception:
                self._conn.execute("ROLLBACK;")
                raise

    def insert_df(self, table: str, df: pd.DataFrame, columns: list[str]) -> int:
        """
        Bulk insert using DuckDB view registration + INSERT..SELECT, with ON CONFLICT DO NOTHING.
        This avoids Python row loops and is typically the fastest MVP-friendly approach.
        """
        if df is None or df.empty:
            return 0

        view = f"v_{uuid4().hex}"
        cols = ", ".join(columns)

        with self._lock:
            # Register a view backed by the DataFrame
            self._conn.register(view, df[columns])

            # Insert from the view
            self._conn.execute(
                f"""
                INSERT INTO {table} ({cols})
                SELECT {cols} FROM {view}
                ON CONFLICT DO NOTHING
                """
            )

            # Unregister view to prevent name collisions / memory retention
            try:
                self._conn.unregister(view)
            except Exception:
                # Some duckdb builds may not expose unregister; safe to ignore.
                pass

        # Accurate insert count with ON CONFLICT is non-trivial; return attempted rows.
        return int(len(df))
