# application/pipeline/steps/load_facturas.py
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class LoadFacturasStep:
    """
    Persiste la tabla 'snapshot.facturas' (facturas por capítulo).
    Upsert por (company_id, invoice_id, capitulo).
    """
    snap_engine: Engine
    schema: str = "snapshot"

    def _tbl(self) -> str:
        return f"{self.schema}.facturas"

    def _ensure_table(self) -> None:
        tbl = self._tbl()
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {tbl} (
            snapshot_run_id   UUID,
            snapshot_run_ts   TIMESTAMPTZ,

            company_id        TEXT    NOT NULL,
            invoice_id        TEXT    NOT NULL,
            job_no            TEXT    NOT NULL,
            capitulo          TEXT    NOT NULL,

            numero_factura    TEXT,
            proveedor         TEXT,
            fecha_factura     DATE,
            coste_registrado_a_origen NUMERIC,

            PRIMARY KEY (company_id, invoice_id, capitulo)
        );
        """
        with self.snap_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(ddl))

    @staticmethod
    def _clean(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime().replace(tzinfo=timezone.utc) if v.tzinfo is None else v.to_pydatetime()
        if isinstance(v, float) and np.isnan(v):
            return None
        if isinstance(v, (np.floating, )):
            return float(v)
        if isinstance(v, (np.integer, )):
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            return s if s else None
        return v

    def run(self, df_in: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        df_in debe traer (con estos nombres ya normalizados por fetch_facturas_por_capitulo):
          - capitulo
          - coste_registrado_a_origen
          - numero_factura
          - proveedor
          - fecha_factura
          - CompanyId  (→ company_id)
          - id         (→ invoice_id)
          - Job_No     (→ job_no)
        """
        self._ensure_table()

        if df_in is None or df_in.empty:
            return {"inserted": 0, "skipped": 0}

        # Normalización de nombres esperados
        df = df_in.rename(columns={
            "CompanyId": "company_id",
            "id": "invoice_id",
            "Job_No": "job_no",
        }).copy()

        # Columnas obligatorias
        req = ["company_id", "invoice_id", "job_no", "capitulo"]
        for c in req:
            if c not in df.columns:
                df[c] = None

        # Tipos/casts
        df["fecha_factura"] = pd.to_datetime(df.get("fecha_factura"), errors="coerce").dt.date
        if "coste_registrado_a_origen" in df.columns:
            df["coste_registrado_a_origen"] = pd.to_numeric(df["coste_registrado_a_origen"], errors="coerce")

        # Metadatos snapshot
        snapshot_run_id = ctx.get("snapshot_run_id") or str(uuid.uuid4())
        srt = ctx.get("snapshot_run_ts")
        if isinstance(srt, str):
            snapshot_run_ts = pd.to_datetime(srt, utc=True).to_pydatetime()
        elif isinstance(srt, datetime):
            snapshot_run_ts = srt if srt.tzinfo else srt.replace(tzinfo=timezone.utc)
        else:
            snapshot_run_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

        df["snapshot_run_id"] = snapshot_run_id
        df["snapshot_run_ts"] = snapshot_run_ts

        cols = [
            "snapshot_run_id", "snapshot_run_ts",
            "company_id", "invoice_id", "job_no", "capitulo",
            "numero_factura", "proveedor", "fecha_factura", "coste_registrado_a_origen",
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]

        rows: List[Tuple[Any, ...]] = [tuple(self._clean(v) for v in r) for _, r in df.iterrows()]

        sql = f"""
        INSERT INTO {self._tbl()} (
            snapshot_run_id, snapshot_run_ts,
            company_id, invoice_id, job_no, capitulo,
            numero_factura, proveedor, fecha_factura, coste_registrado_a_origen
        )
        VALUES %s
        ON CONFLICT (company_id, invoice_id, capitulo) DO UPDATE SET
            snapshot_run_id = EXCLUDED.snapshot_run_id,
            snapshot_run_ts = EXCLUDED.snapshot_run_ts,
            job_no = EXCLUDED.job_no,
            numero_factura = EXCLUDED.numero_factura,
            proveedor = EXCLUDED.proveedor,
            fecha_factura = EXCLUDED.fecha_factura,
            coste_registrado_a_origen = EXCLUDED.coste_registrado_a_origen
        ;
        """
        raw = self.snap_engine.raw_connection()
        try:
            cur = raw.cursor()
            execute_values(cur, sql, rows, template=None, page_size=1000)
            inserted = cur.rowcount
            raw.commit()
            cur.close()
        finally:
            raw.close()

        return {"inserted": inserted, "skipped": 0}
