# interface_adapters/controllers/snapshot_entry.py
from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

# Importa componentes internos del snapshot
from config.config import Config
from infrastructure.pg_gateway import PostgresAdminGateway
from infrastructure.postgres.db import get_conn, get_engine

from interface_adapters.repositories.source_repository import SourceRepository
from interface_adapters.repositories.snapshot_repository import SnapshotRepository
from application.pipeline.pipeline import ResumenSnapshotPipeline
from interface_adapters.controllers.resumen_controller import ResumenController


def _read_stdin_json() -> Optional[dict]:
    try:
        raw = sys.stdin.read()
        raw = (raw or "").strip()
        if raw.startswith("{"):
            return json.loads(raw)
        return None
    except Exception:
        return None


def run_snapshot_cli(company: Optional[str], project: Optional[str], year: int, month: int) -> dict:
    """
    Ejecuta exactamente el mismo flujo que tu main.run_snapshot, pero
    devolviendo un dict serializable para integraciones.
    """
    # 1) Garantiza BD snapshot
    admin = PostgresAdminGateway(config=Config)
    if not admin.database_exists():
        admin.create_database()
    admin.test_connection()

    # 2) DSNs y engines
    snapshot_dsn_psycopg2 = Config.snapshot_psycopg2_dsn()
    snapshot_sa_dsn = (
        f'postgresql+psycopg2://{Config.PG_USER}:{Config.PG_PASSWORD}'
        f'@{Config.PG_SERVER}:{Config.PG_PORT}/{Config.PG_DATABASE}'
    )
    bc_sa_dsn = Config.bc_sqlalchemy_dsn()
    excel_sa_dsn = Config.excel_sqlalchemy_dsn()

    with get_conn(snapshot_dsn_psycopg2) as snap_conn:
        snap_engine = get_engine(snapshot_sa_dsn)
        bc_engine = get_engine(bc_sa_dsn)
        excel_engine = get_engine(excel_sa_dsn)

        source_repo = SourceRepository(bc_engine=bc_engine, excel_engine=excel_engine)
        snapshot_repo = SnapshotRepository(conn=snap_conn, engine=snap_engine)

        pipeline = ResumenSnapshotPipeline(source_repo, snapshot_repo)
        controller = ResumenController(pipeline)

        result = controller.snapshot_mes(company, project, year, month)
        # Esperado: dict con "inserted" y "skipped"
        if not isinstance(result, dict):
            result = {"inserted": 0, "skipped": 0}

        return {
            "ok": True,
            "message": "Snapshot ejecutado",
            "params": {
                "company": company,
                "project": project,
                "year": year,
                "month": month,
            },
            "result": result,
        }


def main():
    # 1) Intentar leer JSON de stdin
    data = _read_stdin_json()

    # 2) Si no llega JSON, parsear argumentos
    parser = argparse.ArgumentParser(description="Snapshot runner")
    parser.add_argument("--company", type=str, default=None, help="Nombre de empresa (companies_bc.name)")
    parser.add_argument("--project", type=str, default=None, help="Código de proyecto (job_list_bc.No)")
    parser.add_argument("--year", type=int, required=False, help="Año (ej. 2025)")
    parser.add_argument("--month", type=int, required=False, help="Mes (1..12)")
    args, _ = parser.parse_known_args()

    try:
        if data:
            company = data.get("company")
            project = data.get("project")
            year = int(data["year"])
            month = int(data["month"])
        else:
            if args.year is None or args.month is None:
                raise ValueError("Faltan parámetros year/month (ni JSON en stdin ni argumentos).")
            company = args.company
            project = args.project
            year = args.year
            month = args.month

        out = run_snapshot_cli(company, project, year, month)
        print(json.dumps(out, ensure_ascii=False))
        sys.exit(0)
    except Exception as e:
        err = {"ok": False, "message": f"Snapshot error: {e}"}
        print(json.dumps(err, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
