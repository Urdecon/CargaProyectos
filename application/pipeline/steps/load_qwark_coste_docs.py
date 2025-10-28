# application/pipeline/steps/load_qwark_coste_docs.py
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class LoadQwarkCosteDocsStep:
    """
    Carga la tabla detalle de documentos que conforman el Coste QWARK:
      snapshot.qwark_coste_docs

    NOTA: No altera procesos existentes; es una tabla adicional.
    """
    snap_engine: Engine
    schema: str = "snapshot"

    META_COLS = ("snapshot_run_id", "snapshot_run_ts")

    COLS = (
        "company_id",
        "company_name",
        "project_no",
        "fecha_factura",
        "document_no",
        "vendor_name",
        "posting_description",
        "bc_draft_document_no",
        "job_task_no",
        "base_amount",
    )

    DB_COLS = META_COLS + COLS

    def _tbl(self) -> str:
        return f"{self.schema}.qwark_coste_docs"

    # ─────────────────────────── DDL ───────────────────────────
    def _ensure_table(self) -> None:
        tbl = self._tbl()

        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {tbl} (
            snapshot_run_id        UUID,
            snapshot_run_ts        TIMESTAMPTZ,

            company_id             TEXT,
            company_name           TEXT,
            project_no             TEXT,
            fecha_factura          DATE,
            document_no            TEXT,
            vendor_name            TEXT,
            posting_description    TEXT,
            bc_draft_document_no   TEXT,
            job_task_no            TEXT,
            base_amount            NUMERIC
        );
        """

        with self.snap_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(ddl))

            # Índices útiles (no PK)
            try:
                conn.execute(text(
                    f'CREATE INDEX IF NOT EXISTS idx_qwark_docs_key '
                    f'ON {tbl} (company_name, project_no, fecha_factura, document_no);'
                ))
            except Exception as e:
                logger.warning("No se pudo crear índice idx_qwark_docs_key: %s", e)

    # ───────────────────── util limpieza ─────────────────────
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
        period_date: date,
        snapshot_run_id: str,
        snapshot_run_ts: datetime,
    ) -> pd.DataFrame:
        for c in self.DB_COLS:
            if c not in df.columns:
                df[c] = pd.NA

        df["snapshot_run_id"] = snapshot_run_id
        df["snapshot_run_ts"] = snapshot_run_ts

        # tipado básico
        df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date
        df["base_amount"] = pd.to_numeric(df["base_amount"], errors="coerce")

        # saneo None/NAs
        df = df.where(pd.notna(df), None)
        return df.loc[:, self.DB_COLS]

    def _to_tuples(self, df: pd.DataFrame) -> List[Tuple[Any, ...]]:
        return [tuple(self._py_clean(row[c]) for c in self.DB_COLS) for _, row in df.iterrows()]

    # ─────────────────────────── RUN ───────────────────────────
    def run(self, rows: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        UPSERT no es necesario aquí; tomamos el snapshot como append de detalle.
        Para mantenerlo simple: truncamos y recargamos del mes/params actual.
        """
        self._ensure_table()
        tbl = self._tbl()

        # snapshot meta
        period_date = ctx.get("period_date")
        if isinstance(period_date, (str, datetime)):
            period_date = pd.to_datetime(period_date).date()

        snapshot_run_id = ctx.get("snapshot_run_id") or str(uuid.uuid4())
        srt = ctx.get("snapshot_run_ts")
        if isinstance(srt, str):
            snapshot_run_ts = pd.to_datetime(srt, utc=True).to_pydatetime()
        elif isinstance(srt, datetime):
            snapshot_run_ts = srt if srt.tzinfo else srt.replace(tzinfo=timezone.utc)
        else:
            snapshot_run_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

        df = rows.copy()

        df = self._prep_dataframe(
            df,
            period_date=period_date,
            snapshot_run_id=snapshot_run_id,
            snapshot_run_ts=snapshot_run_ts,
        )

        if df.empty:
            logger.info("No hay documentos QWARK para cargar en %s.", tbl)
            return {"inserted": 0}

        tuples = self._to_tuples(df)

        cols_csv = ", ".join(f'"{c}"' for c in self.DB_COLS)
        upsert_sql = f"""
        INSERT INTO {tbl} ({cols_csv})
        VALUES %s
        """

        raw = self.snap_engine.raw_connection()
        try:
            cur = raw.cursor()
            try:
                template = "(" + ",".join(["%s"] * len(self.DB_COLS)) + ")"
                execute_values(cur, upsert_sql, tuples, template=template, page_size=1000)
                inserted = cur.rowcount
                raw.commit()
            finally:
                try: cur.close()
                except Exception: pass
        finally:
            try: raw.close()
            except Exception: pass

        logger.info("Documentos QWARK cargados en %s: inserted=%s", tbl, inserted)
        return {"inserted": inserted}
