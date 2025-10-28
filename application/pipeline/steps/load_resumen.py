# application/pipeline/steps/load_resumen.py
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
class LoadResumenStep:
    """
    Carga incremental de snapshot.resumen_seguimiento.

    NOTA: 'resultado' y '% resultado' vienen ya del Transform (ahora 0.0).
    """
    snap_engine: Engine
    schema: str = "snapshot"

    META_COLS: Tuple[str, ...] = ("snapshot_run_id", "snapshot_run_ts")

    COLS: Tuple[str, ...] = (
        "company_id",
        "company_name",
        "project_no",
        "period_date",
        "grupo",
        "capitulo",
        "nivel",
        "coste_bc_a_origen",
        "coste_bc_mes_actual",
        "coste_bc_mes_anterior",
        "coste_qwark",
        "coste_pendiente",
        "coste_total_a_origen",
        "certificacion_a_origen",
        "certificacion_mes_actual",       # existente
        "certificacion_mes_anterior",     # existente
        "certificacion_pendiente",
        "resto_produccion",
        "produccion_a_origen",
        # ───── NUEVAS columnas mes anterior ─────
        "certificacion_pendiente_mes_anterior",
        "resto_produccion_mes_anterior",
        "produccion_mes_anterior",
        # ────────────────────────────────────────
        "resultado",
        "pct_resultado",
        "contratos",
        "planificado",
        "pct_avance_venta_contrato",
        "pct_avance_coste_planificado",
    )

    DB_COLS: Tuple[str, ...] = META_COLS + COLS

    NUM_COLS: Tuple[str, ...] = (
        "coste_bc_a_origen",
        "coste_bc_mes_actual",
        "coste_bc_mes_anterior",
        "coste_qwark",
        "coste_pendiente",
        "coste_total_a_origen",
        "certificacion_a_origen",
        "certificacion_mes_actual",       # existente
        "certificacion_mes_anterior",     # existente
        "certificacion_pendiente",
        "resto_produccion",
        "produccion_a_origen",
        # ───── NUEVAS columnas mes anterior ─────
        "certificacion_pendiente_mes_anterior",
        "resto_produccion_mes_anterior",
        "produccion_mes_anterior",
        # ────────────────────────────────────────
        "resultado",
        "pct_resultado",
        "contratos",
        "planificado",
        "pct_avance_venta_contrato",
        "pct_avance_coste_planificado",
    )

    KEY_COLS: Tuple[str, ...] = (
        "company_name",
        "project_no",
        "period_date",
        "grupo",
        "capitulo",
        "nivel",
    )

    def _tbl(self) -> str:
        return f"{self.schema}.resumen_seguimiento"

    # ─────────────────────────── DDL ───────────────────────────
    def _ensure_table(self) -> None:
        tbl = self._tbl()

        ddl_create = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};

        CREATE TABLE IF NOT EXISTS {tbl} (
            snapshot_run_id              UUID,
            snapshot_run_ts              TIMESTAMPTZ,

            company_id                   TEXT,
            company_name                 TEXT NOT NULL,
            project_no                   TEXT NOT NULL,
            period_date                  DATE NOT NULL,
            grupo                        TEXT NOT NULL,
            capitulo                     TEXT NOT NULL,
            nivel                        TEXT NOT NULL,

            coste_bc_a_origen            NUMERIC,
            coste_bc_mes_actual          NUMERIC,
            coste_bc_mes_anterior        NUMERIC,
            coste_qwark                  NUMERIC,
            coste_pendiente              NUMERIC,
            coste_total_a_origen         NUMERIC,

            certificacion_a_origen       NUMERIC,
            certificacion_mes_actual     NUMERIC,
            certificacion_mes_anterior   NUMERIC,
            certificacion_pendiente      NUMERIC,
            resto_produccion             NUMERIC,
            produccion_a_origen          NUMERIC,

            -- NUEVAS columnas mes anterior:
            certificacion_pendiente_mes_anterior NUMERIC,
            resto_produccion_mes_anterior      NUMERIC,
            produccion_mes_anterior            NUMERIC,

            resultado                    NUMERIC,
            pct_resultado                NUMERIC,
            contratos                    NUMERIC,
            planificado                  NUMERIC,
            pct_avance_venta_contrato    NUMERIC,
            pct_avance_coste_planificado NUMERIC
        );
        """

        col_types = {
            "snapshot_run_id": "UUID",
            "snapshot_run_ts": "TIMESTAMPTZ",
            "company_id": "TEXT",
            "company_name": "TEXT",
            "project_no": "TEXT",
            "period_date": "DATE",
            "grupo": "TEXT",
            "capitulo": "TEXT",
            "nivel": "TEXT",
            "coste_bc_a_origen": "NUMERIC",
            "coste_bc_mes_actual": "NUMERIC",
            "coste_bc_mes_anterior": "NUMERIC",
            "coste_qwark": "NUMERIC",
            "coste_pendiente": "NUMERIC",
            "coste_total_a_origen": "NUMERIC",
            "certificacion_a_origen": "NUMERIC",
            "certificacion_mes_actual": "NUMERIC",
            "certificacion_mes_anterior": "NUMERIC",
            "certificacion_pendiente": "NUMERIC",
            "resto_produccion": "NUMERIC",
            "produccion_a_origen": "NUMERIC",
            # ───── NUEVAS columnas mes anterior ─────
            "certificacion_pendiente_mes_anterior": "NUMERIC",
            "resto_produccion_mes_anterior": "NUMERIC",
            "produccion_mes_anterior": "NUMERIC",
            # ────────────────────────────────────────
            "resultado": "NUMERIC",
            "pct_resultado": "NUMERIC",
            "contratos": "NUMERIC",
            "planificado": "NUMERIC",
            "pct_avance_venta_contrato": "NUMERIC",
            "pct_avance_coste_planificado": "NUMERIC",
        }

        with self.snap_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            try:
                conn.execute(text(ddl_create))
            except Exception as e:
                logger.warning("CREATE schema/table ignorado: %s", e)

            # Asegura columnas (ADD COLUMN IF NOT EXISTS) por si la tabla ya existía
            for col, typ in col_types.items():
                try:
                    conn.execute(text(f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "{col}" {typ};'))
                except Exception as e:
                    logger.warning("ADD COLUMN %s ignorado: %s", col, e)

            # eliminar PK previa (best-effort) y crear la correcta
            try:
                conn.execute(text(f"""
                DO $$
                DECLARE
                  pk_name text;
                BEGIN
                  SELECT con.conname
                  INTO pk_name
                  FROM pg_constraint con
                  JOIN pg_class rel ON rel.oid = con.conrelid
                  JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
                  WHERE con.contype = 'p'
                    AND nsp.nspname = '{self.schema}'
                    AND rel.relname = 'resumen_seguimiento';

                  IF pk_name IS NOT NULL THEN
                    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', '{self.schema}', 'resumen_seguimiento', pk_name);
                  END IF;
                END$$;
                """))
            except Exception as e:
                logger.warning("DROP PK previo ignorado: %s", e)

            try:
                conn.execute(text(
                    f'ALTER TABLE {tbl} '
                    'ADD CONSTRAINT resumen_seguimiento_pk '
                    'PRIMARY KEY ("company_name","project_no","period_date","grupo","capitulo","nivel");'
                ))
            except Exception as e:
                logger.warning("ADD PK ignorado: %s", e)

    # ───────────────────── normalización DF ─────────────────────
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

        df["period_date"] = pd.to_datetime(period_date).date()
        df["snapshot_run_id"] = snapshot_run_id
        df["snapshot_run_ts"] = snapshot_run_ts

        for c in self.NUM_COLS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.where(pd.notna(df), None)
        return df.loc[:, self.DB_COLS]

    def _to_tuples(self, df: pd.DataFrame) -> List[Tuple[Any, ...]]:
        return [tuple(self._py_clean(row[c]) for c in self.DB_COLS) for _, row in df.iterrows()]

    @staticmethod
    def _unify_capitulo(df: pd.DataFrame) -> pd.DataFrame:
        """Unifica 'NN.NN' → 'NN.NN.DESCRIPCIÓN' si ésta existe en el lote."""
        if "capitulo" not in df.columns:
            return df

        caps = (
            df["capitulo"]
            .astype(str)
            .dropna()
            .str.strip()
        )

        descriptivos = caps[caps.str.match(r"^\d{2}\.\d{2}\..+")]
        if descriptivos.empty:
            return df

        mapa: dict[str, str] = {}
        for full in descriptivos.unique():
            parts = full.split(".", 2)
            if len(parts) >= 2:
                code = f"{parts[0]}.{parts[1]}"
                mapa.setdefault(code, full)

        if not mapa:
            return df

        df = df.copy()
        df["capitulo"] = df["capitulo"].astype(str).str.strip()
        mask = df["capitulo"].isin(mapa.keys())
        df.loc[mask, "capitulo"] = df.loc[mask, "capitulo"].map(mapa)
        return df

    # ─────────────────────────── RUN ───────────────────────────
    def run(self, rows: pd.DataFrame, ctx: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_table()

        period_date = ctx.get("period_date")
        if isinstance(period_date, (str, datetime)):
            period_date = pd.to_datetime(period_date).date()
        assert isinstance(period_date, date)

        snapshot_run_id = ctx.get("snapshot_run_id") or str(uuid.uuid4())
        srt = ctx.get("snapshot_run_ts")
        if isinstance(srt, str):
            snapshot_run_ts = pd.to_datetime(srt, utc=True).to_pydatetime()
        elif isinstance(srt, datetime):
            snapshot_run_ts = srt if srt.tzinfo else srt.replace(tzinfo=timezone.utc)
        else:
            snapshot_run_ts = datetime.utcnow().replace(tzinfo=timezone.utc)

        df = rows.copy()

        # renombrar si llega como 'linea'
        if "capitulo" not in df.columns and "linea" in df.columns:
            df = df.rename(columns={"linea": "capitulo"})

        # normaliza espacios
        if "capitulo" in df.columns:
            df["capitulo"] = df["capitulo"].astype(str).str.replace("\u00A0", " ").str.strip()

        # ► Unificación de capítulos para evitar duplicados
        df = self._unify_capitulo(df)

        key = ["company_id", "company_name", "project_no", "grupo", "capitulo", "nivel"]
        for k in key:
            if k not in df.columns:
                df[k] = pd.NA

        # Sumamos métricas por clave
        num_cols = [c for c in self.NUM_COLS if c in df.columns]
        if num_cols:
            df = df.groupby(key, dropna=False, as_index=False)[num_cols].sum(min_count=1)
        else:
            df = df.drop_duplicates(subset=key, keep="first")

        # Mantener cálculo de 'produccion_a_origen' desde sus 3 componentes
        for need in ("certificacion_a_origen", "certificacion_pendiente", "resto_produccion"):
            if need not in df.columns:
                df[need] = np.nan
        df["produccion_a_origen"] = df[
            ["certificacion_a_origen", "certificacion_pendiente", "resto_produccion"]
        ].sum(axis=1, skipna=True)

        # Preparación final
        df = self._prep_dataframe(
            df,
            period_date=period_date,
            snapshot_run_id=snapshot_run_id,
            snapshot_run_ts=snapshot_run_ts,
        )

        if df.empty:
            logger.info("No hay filas para cargar en %s.", self._tbl())
            return {"inserted": 0, "skipped": 0}

        tuples = self._to_tuples(df)

        cols_csv = ", ".join(f'"{c}"' for c in self.DB_COLS)
        key_csv = ", ".join(f'"{k}"' for k in self.KEY_COLS)

        select_cast_csv = ",\n                ".join(
            [
                'v."snapshot_run_id"::uuid',
                'v."snapshot_run_ts"::timestamptz',
                'v."company_id"',
                'v."company_name"',
                'v."project_no"',
                'v."period_date"::date',
                'v."grupo"',
                'v."capitulo"',
                'v."nivel"',
                'CAST(v."coste_bc_a_origen"           AS numeric)',
                'CAST(v."coste_bc_mes_actual"         AS numeric)',
                'CAST(v."coste_bc_mes_anterior"       AS numeric)',
                'CAST(v."coste_qwark"                 AS numeric)',
                'CAST(v."coste_pendiente"             AS numeric)',
                'CAST(v."coste_total_a_origen"        AS numeric)',
                'CAST(v."certificacion_a_origen"      AS numeric)',
                'CAST(v."certificacion_mes_actual"    AS numeric)',
                'CAST(v."certificacion_mes_anterior"  AS numeric)',
                'CAST(v."certificacion_pendiente"     AS numeric)',
                'CAST(v."resto_produccion"            AS numeric)',
                'CAST(v."produccion_a_origen"         AS numeric)',
                # ───── NUEVAS columnas mes anterior ─────
                'CAST(v."certificacion_pendiente_mes_anterior" AS numeric)',
                'CAST(v."resto_produccion_mes_anterior"        AS numeric)',
                'CAST(v."produccion_mes_anterior"              AS numeric)',
                # ────────────────────────────────────────
                'CAST(v."resultado"                   AS numeric)',
                'CAST(v."pct_resultado"               AS numeric)',
                'CAST(v."contratos"                   AS numeric)',
                'CAST(v."planificado"                 AS numeric)',
                'CAST(v."pct_avance_venta_contrato"   AS numeric)',
                'CAST(v."pct_avance_coste_planificado" AS numeric)',
            ]
        )

        upsert_sql = f"""
        WITH v AS (
            SELECT * FROM (VALUES %s) AS v ({cols_csv})
        )
        INSERT INTO {self._tbl()} ({cols_csv})
        SELECT
                {select_cast_csv}
        FROM v
        ON CONFLICT ({key_csv}) DO UPDATE
        SET
                "snapshot_run_id"            = EXCLUDED."snapshot_run_id",
                "snapshot_run_ts"            = EXCLUDED."snapshot_run_ts",
                "company_id"                 = EXCLUDED."company_id",
                "coste_bc_a_origen"          = EXCLUDED."coste_bc_a_origen",
                "coste_bc_mes_actual"        = EXCLUDED."coste_bc_mes_actual",
                "coste_bc_mes_anterior"      = EXCLUDED."coste_bc_mes_anterior",
                "coste_qwark"                = EXCLUDED."coste_qwark",
                "coste_pendiente"            = EXCLUDED."coste_pendiente",
                "coste_total_a_origen"       = EXCLUDED."coste_total_a_origen",
                "certificacion_a_origen"     = EXCLUDED."certificacion_a_origen",
                "certificacion_mes_actual"   = EXCLUDED."certificacion_mes_actual",
                "certificacion_mes_anterior" = EXCLUDED."certificacion_mes_anterior",
                "certificacion_pendiente"    = EXCLUDED."certificacion_pendiente",
                "resto_produccion"           = EXCLUDED."resto_produccion",
                "produccion_a_origen"        = EXCLUDED."produccion_a_origen",
                -- ───── NUEVAS columnas mes anterior ─────
                "certificacion_pendiente_mes_anterior" = EXCLUDED."certificacion_pendiente_mes_anterior",
                "resto_produccion_mes_anterior"        = EXCLUDED."resto_produccion_mes_anterior",
                "produccion_mes_anterior"              = EXCLUDED."produccion_mes_anterior",
                -- ────────────────────────────────────────
                "resultado"                  = EXCLUDED."resultado",
                "pct_resultado"              = EXCLUDED."pct_resultado",
                "contratos"                  = EXCLUDED."contratos",
                "planificado"                = EXCLUDED."planificado",
                "pct_avance_venta_contrato"  = EXCLUDED."pct_avance_venta_contrato",
                "pct_avance_coste_planificado" = EXCLUDED."pct_avance_coste_planificado"
        ;
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
                try:
                    cur.close()
                except Exception:
                    pass
        finally:
            try:
                raw.close()
            except Exception:
                pass

        logger.info("Snapshot cargado en %s: upserted=%s filas.", self._tbl(), inserted)
        return {"inserted": inserted, "skipped": 0}
