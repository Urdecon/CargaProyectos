
# sanpshot/application/gapfill/gapfill.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy.engine import Engine

from config.config import Config
from infrastructure.pg_gateway import PostgresAdminGateway
from infrastructure.postgres.db import get_conn, get_engine
from interface_adapters.repositories.source_repository import SourceRepository
from interface_adapters.repositories.snapshot_repository import SnapshotRepository
from application.pipeline.pipeline import ResumenSnapshotPipeline, PipelineParams
from interface_adapters.controllers.resumen_controller import ResumenController

logger = logging.getLogger(__name__)
BC_JOBS_TABLE      = 'business_central.public.job_list_bc'
BC_COMPANIES_TABLE = 'business_central.public.companies_bc'

# Candidatas en minúsculas (no sensibles a may/min)
CANDIDATE_COLS_PROJECT      = ['no', 'job_no', 'project_no', 'code']
CANDIDATE_COLS_COMPANY_ID_J = ['companyid', 'company_id']          # FK en Jobs
CANDIDATE_COLS_COMPANY_ID_C = ['id', 'companyid', 'company_id']    # PK en Companies
CANDIDATE_COLS_COMPANY_NAME = ['name', 'display_name', 'companyname', 'company_name']

def _introspect_lower_map(engine, table_name: str) -> dict[str, str]:
    """Devuelve {col_en_minúsculas: col_original} para una tabla."""
    df0 = pd.read_sql(f'SELECT * FROM {table_name} LIMIT 0', engine)
    return {c.lower(): c for c in df0.columns}


def _pick_original(lower_map: dict[str, str], candidates: list[str]) -> str | None:
    """Dada una lista de candidatos en minúsculas, devuelve el nombre ORIGINAL si existe."""
    for cand in candidates:
        if cand in lower_map:
            return lower_map[cand]
    return None

def _last_day_of_month(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    first_next = date(y, m + 1, 1)
    return first_next - timedelta(days=1)

@dataclass(frozen=True)
class MissingProject:
    company_name: str
    project_no: str

class GapFillService:
    """
    Servicio que:
    1) Descubre qué proyectos *deberían* existir en snapshot.resumen_seguimiento para un (año, mes).
    2) Comprueba cuáles existen ya y cuáles faltan (clave: company_name + project_no + period_date).
    3) Ejecuta el snapshot para los que faltan.
    """

    def __init__(self) -> None:
        # Admin: asegura BD de snapshot (igual que en main.py)
        admin = PostgresAdminGateway(config=Config)
        if not admin.database_exists():
            admin.create_database()
        admin.test_connection()

        # DSNs
        self.snapshot_dsn_psycopg2 = Config.snapshot_psycopg2_dsn()
        self.snapshot_sa_dsn = (
            f'postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}'
            f'@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}'
        )
        self.bc_sa_dsn = Config.bc_sqlalchemy_dsn()
        self.excel_sa_dsn = Config.excel_sqlalchemy_dsn()

        # Engines/conexiones
        self.snap_engine: Engine = get_engine(self.snapshot_sa_dsn)
        self.bc_engine: Engine = get_engine(self.bc_sa_dsn)
        self.excel_engine: Engine = get_engine(self.excel_sa_dsn)

        # Repos y pipeline reutilizando tu arquitectura
        self.source_repo = SourceRepository(bc_engine=self.bc_engine, excel_engine=self.excel_engine)
        self.snapshot_repo = None  # se crea por uso con psycopg2 cuando carguemos

    def expected_projects_for_period(self, year: int, month: int, company: Optional[str] = None) -> pd.DataFrame:
        """
        Devuelve TODOS los proyectos (aunque no tengan actividad) leyendo la tabla de Jobs
        y uniendo con Companies para obtener company_name.
        """
        jobs_map = _introspect_lower_map(self.bc_engine, BC_JOBS_TABLE)
        comps_map = _introspect_lower_map(self.bc_engine, BC_COMPANIES_TABLE)

        proj_col = _pick_original(jobs_map, CANDIDATE_COLS_PROJECT)  # p.ej. "No"
        fk_comp = _pick_original(jobs_map, CANDIDATE_COLS_COMPANY_ID_J)  # p.ej. "CompanyId"
        pk_comp = _pick_original(comps_map, CANDIDATE_COLS_COMPANY_ID_C)  # p.ej. "Id"
        name_col = _pick_original(comps_map, CANDIDATE_COLS_COMPANY_NAME)  # p.ej. "Name"

        if not (proj_col and fk_comp and pk_comp and name_col):
            raise RuntimeError(
                "No encuentro columnas necesarias para Jobs/Companies.\n"
                f"Jobs: {list(jobs_map.values())}\nCompanies: {list(comps_map.values())}\n"
                "Ajusta CANDIDATE_* o nombres de tabla."
            )

        where = ""
        params = {}
        if company:
            where = f'WHERE LOWER(c."{name_col}"::text) = LOWER(%(company)s)'
            params["company"] = company

        sql = f'''
            SELECT DISTINCT
                c."{name_col}"::text AS company_name,
                j."{proj_col}"::text AS project_no
            FROM {BC_JOBS_TABLE} j
            LEFT JOIN {BC_COMPANIES_TABLE} c
                   ON c."{pk_comp}" = j."{fk_comp}"
            {where}
            ORDER BY 1, 2
        '''
        df = pd.read_sql(sql, self.bc_engine, params=params)

        if df is None or df.empty:
            return pd.DataFrame(columns=["company_name", "project_no"]).astype(
                {"company_name": "string", "project_no": "string"}
            )

        return (df[["company_name", "project_no"]]
                .drop_duplicates()
                .reset_index(drop=True)
                .astype({"company_name": "string", "project_no": "string"}))

    def existing_projects_in_snapshot(self, year: int, month: int, company: Optional[str] = None) -> pd.DataFrame:
        """
        Devuelve proyectos YA presentes en snapshot para TODO el mes (no solo EOM),
        normalizando claves para evitar diferencias de mayúsculas/espacios.
        """
        first = date(year, month, 1)
        last = _last_day_of_month(year, month)

        company_filter = ""
        params = {"first": first, "last": last}
        if company:
            company_filter = " AND LOWER(company_name::text) = LOWER(%(company)s)"
            params["company"] = company

        sql = f"""
            SELECT DISTINCT
                TRIM(LOWER(company_name::text)) AS company_key,
                TRIM(LOWER(project_no::text))   AS project_key
            FROM snapshot.resumen_seguimiento
            WHERE period_date >= %(first)s AND period_date <= %(last)s
            {company_filter}
        """
        df = pd.read_sql(sql, self.snap_engine, params=params)
        if df is None or df.empty:
            # devolvemos vacío pero con las columnas esperadas
            return pd.DataFrame(columns=["company_key", "project_key"])
        return df[["company_key", "project_key"]].drop_duplicates().reset_index(drop=True)

    def diff_missing(self, year: int, month: int, company: Optional[str] = None) -> pd.DataFrame:
        """
        Left anti-join usando claves normalizadas.
        """
        expected = self.expected_projects_for_period(year, month, company)
        if expected is None or expected.empty:
            return pd.DataFrame(columns=["company_name", "project_no"])

        # normaliza claves en 'esperados'
        exp = expected.copy()
        exp["company_key"] = exp["company_name"].str.strip().str.lower()
        exp["project_key"] = exp["project_no"].str.strip().str.lower()

        # existentes ya vienen como claves normalizadas
        exist = self.existing_projects_in_snapshot(year, month, company)

        merged = exp.merge(
            exist,
            on=["company_key", "project_key"],
            how="left",
            indicator=True
        )
        missing = merged[merged["_merge"] == "left_only"][["company_name", "project_no"]]
        return missing.drop_duplicates().reset_index(drop=True)

    def run_snapshot_for(self, company: Optional[str], project: Optional[str], year: int, month: int) -> dict:
        """
        Ejecuta el snapshot normal para un (company, project, year, month).
        Reutiliza tu pipeline (igual que main.py).
        """
        from infrastructure.postgres.db import get_conn  # import local
        from interface_adapters.repositories.snapshot_repository import SnapshotRepository

        snapshot_dsn_psycopg2 = Config.snapshot_psycopg2_dsn()
        snapshot_sa_dsn = (
            f'postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}'
            f'@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}'
        )

        with get_conn(snapshot_dsn_psycopg2) as snap_conn:
            snap_engine = get_engine(snapshot_sa_dsn)
            source_repo = SourceRepository(bc_engine=self.bc_engine, excel_engine=self.excel_engine)
            snapshot_repo = SnapshotRepository(conn=snap_conn, engine=snap_engine)
            pipeline = ResumenSnapshotPipeline(source_repo, snapshot_repo)
            controller = ResumenController(pipeline)
            return controller.snapshot_mes(company, project, year, month)

    def fill_missing(self, year: int, month: int, company: Optional[str] = None, dry_run: bool = False) -> dict:
        """
        Ejecuta el rellenado de huecos: detecta missing y dispara snapshot por cada proyecto ausente.
        """
        missing = self.diff_missing(year, month, company)
        total = int(len(missing))
        logger.info("Periodo %04d-%02d · empresa=%s · faltan %d proyectos", year, month, company or "*", total)

        if dry_run or total == 0:
            return {"missing": missing, "run": 0}

        run_count = 0
        for _, row in missing.iterrows():
            c = row["company_name"]
            p = row["project_no"]
            try:
                res = self.run_snapshot_for(c, p, year, month)
                run_count += 1
                logger.info("Snapshot OK %s · %s · %04d-%02d · result=%s", c, p, year, month, res)
            except Exception as ex:
                logger.exception("Snapshot FAILED %s · %s · %04d-%02d: %s", c, p, year, month, ex)

        return {"missing": missing, "run": run_count}
