# application/pipeline/steps/load_seguimiento_produccion.py
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class LoadSeguimientoProduccionStep:
    """
    Carga la tabla detalle de seguimiento de producción:
      snapshot.seguimiento_produccion
    """
    snap_engine: Engine
    schema: str = "snapshot"

    META_COLS = ("snapshot_run_id", "snapshot_run_ts")

    COLS = (
        "company_id",
        "company_name",
        "project_no",
        "fecha_produccion",
        "capitulo",
        "certificacion_pendiente",
        "resto_produccion",
        "observaciones",
    )

    DB_COLS = META_COLS + COLS

    def _tbl(self) -> str:
        return f"{self.schema}.seguimiento_produccion"

    def _ensure_table(self) -> None:
        tbl = self._tbl()
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {tbl} (
            snapshot_run_id          UUID,
            snapshot_run_ts          TIMESTAMPTZ,

            company_id               TEXT,
            company_name             TEXT,
            project_no               TEXT,
            fecha_produccion         DATE,
            capitulo                 TEXT,
            certificacion_pendiente  NUMERIC,
            resto_produccion         NUMERIC,
            observaciones            TEXT
        );
        """
        with self.snap_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(ddl))
            try:
                conn.execute(text(
                    f'CREATE INDEX IF NOT EXISTS idx_segprod_key '
                    f'ON {tbl} (company_name, project_no, fecha_produccion, capitulo);'
                ))
            except Exception as e:
                logger.warning("No se pudo crear índice idx_segprod_key: %s", e)

    @staticmethod
    def _py_clean(v: Any) -> Any:
        if v is None:
            return None
        try:
            from pandas._libs.missing import NAType  # type: ignore
            if isinstance(v, NAType):
                return None
        except Exception:
            pass
        if isinstance(v, pd.Timestamp):
            if v.tzinfo is None:
                return v.to_pydatetime().replace(tzinfo=timezone.utc)
            return v.to_pydatetime()
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, np.floating):
            return float(v)
        if isinstance(v, np.integer):
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return v

    def _prep_dataframe(
        self,
        df: pd.DataFrame,
        snapshot_run_id: str,
        snapshot_run_ts: datetime,
    ) -> pd.DataFrame:
        for c in self.DB_COLS:
            if c not in df.columns:
                df[c] = pd.NA

        df["snapshot_run_id"] = snapshot_run_id
        df["snapshot_run_ts"] = snapshot_run_ts

        df["fecha_produccion"] = pd.to_datetime(df["fecha_produccion"], errors="coerce").dt.date
        df["certificacion_pendiente"] = pd.to_numeric(df["certificacion_pendiente"], errors="coerce")
        df["resto_produccion"] = pd.to_numeric(df["resto_produccion"], errors="coerce")

        df = df.where(pd.notna(df), None)
        return df.loc[:, self.DB_COLS]

    def _to_tuples(self, df: pd.DataFrame) -> List[Tuple[Any, ...]]:
        return [tuple(self._py_clean(row[c]) for c in self.DB_COLS) for _, row in df.iterrows()]

    def run(self, rows: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Carga por append (foto del periodo). No borra datos previos.
        """
        self._ensure_table()
        tbl = self._tbl()

        snapshot_run_id = ctx.get("snapshot_run_id") or str(uuid.uuid4())
        srt = ctx.get("snapshot_run_ts")
        if isinstance(srt, str):
            snapshot_run_ts = pd.to_datetime(srt, utc=True).to_pydatetime()
        elif isinstance(srt, datetime):
            snapshot_run_ts = srt if srt.tzinfo else srt.replace(tzinfo=timezone.utc)
        else:
            snapshot_run_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

        df = rows.copy()
        df = self._prep_dataframe(df, snapshot_run_id, snapshot_run_ts)

        if df.empty:
            logger.info("No hay seguimiento_produccion para cargar en %s.", tbl)
            return {"inserted": 0}

        tuples = self._to_tuples(df)

        cols_csv = ", ".join(f'"{c}"' for c in self.DB_COLS)
        insert_sql = f'INSERT INTO {tbl} ({cols_csv}) VALUES %s'

        raw = self.snap_engine.raw_connection()
        try:
            cur = raw.cursor()
            try:
                template = "(" + ",".join(["%s"] * len(self.DB_COLS)) + ")"
                execute_values(cur, insert_sql, tuples, template=template, page_size=1000)
                inserted = cur.rowcount
                raw.commit()
            finally:
                try: cur.close()
                except Exception: pass
        finally:
            try: raw.close()
            except Exception: pass

        logger.info("Seguimiento producción cargado en %s: inserted=%s", tbl, inserted)
        return {"inserted": inserted}
