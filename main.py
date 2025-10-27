# main.py
from __future__ import annotations

import logging
import os

from config.config import Config
from infrastructure.pg_gateway import PostgresAdminGateway
from infrastructure.postgres.db import get_conn, get_engine

from interface_adapters.repositories.source_repository import SourceRepository
from interface_adapters.repositories.snapshot_repository import SnapshotRepository
from application.pipeline.pipeline import ResumenSnapshotPipeline
from interface_adapters.controllers.resumen_controller import ResumenController


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def run_snapshot(*, company: str | None, project: str | None, year: int, month: int) -> None:
    """
    Orquesta la ejecución del snapshot para los parámetros dados.
    """
    # 1) Garantiza que la BD de snapshot existe
    admin = PostgresAdminGateway(config=Config)
    if not admin.database_exists():
        admin.create_database()
    admin.test_connection()

    # 2) DSNs (psycopg para snapshot y SQLAlchemy para todas)
    snapshot_dsn_psycopg2 = Config.snapshot_psycopg2_dsn()
    snapshot_sa_dsn = (
        f'postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}'
        f'@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}'
    )
    bc_sa_dsn = Config.bc_sqlalchemy_dsn()
    excel_sa_dsn = Config.excel_sqlalchemy_dsn()

    # 3) Conexiones y pipeline
    with get_conn(snapshot_dsn_psycopg2) as snap_conn:
        snap_engine = get_engine(snapshot_sa_dsn)
        bc_engine = get_engine(bc_sa_dsn)
        excel_engine = get_engine(excel_sa_dsn)

        source_repo = SourceRepository(bc_engine=bc_engine, excel_engine=excel_engine)
        snapshot_repo = SnapshotRepository(conn=snap_conn, engine=snap_engine)

        pipeline = ResumenSnapshotPipeline(source_repo, snapshot_repo)
        controller = ResumenController(pipeline)

        result = controller.snapshot_mes(company, project, year, month)  # ← devuelve dict

        inserted = result.get("inserted", 0) if isinstance(result, dict) else 0
        skipped = result.get("skipped", 0) if isinstance(result, dict) else 0

        print(
            f"Snapshot run {company or '*'}::{project or '*'}::{year}-{month:02d} → "
            f"inserted={inserted}, skipped(no change)={skipped}"
        )


if __name__ == "__main__":
    # ──────────────────────────────────────────────────────────────
    # Hardcode de parámetros (cámbialos aquí cuando quieras)
    # company → companies_bc.name (ej.: "Construcciones Urdecon S.A.")
    # project → job_list_bc.No (ej.: "PY_002427")
    # year    → año de corte (p. ej. 2025)
    # month   → mes (1..12) (p. ej. 8)
    # Usa None para no filtrar por empresa/proyecto.
    # ──────────────────────────────────────────────────────────────
    COMPANY = "Construcciones Urdecon S.A."
    PROJECT = "PY_002427"
    YEAR: int = 2025
    MONTH: int = 8

    run_snapshot(company=COMPANY, project=PROJECT, year=YEAR, month=MONTH)
