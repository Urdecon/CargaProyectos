# interface_adapters/repositories/snapshot_repository.py
from __future__ import annotations

# interface_adapters/repositories/snapshot_repository.py

from typing import Optional
import json
from datetime import datetime, timezone

import psycopg
from sqlalchemy.engine import Engine

DDL_SNAPSHOT_RUN = """
CREATE SCHEMA IF NOT EXISTS snapshot;

CREATE TABLE IF NOT EXISTS snapshot.snapshot_run (
    snapshot_run_id  uuid PRIMARY KEY,
    started_at       timestamptz NOT NULL,
    params_json      jsonb       NOT NULL
);
"""


class SnapshotRepository:
    """
    - Garantiza snapshot.snapshot_run.
    - Migración NO destructiva de snapshot.resumen_seguimiento:
        · Si existe como **vista** → se elimina y se deja que el loader cree la **tabla**.
        · Si existe como **tabla**:
            - NO renombra columnas (nada de 'linea'→'capitulo' en BD),
            - añade columns meta (snapshot_run_id, snapshot_run_ts) si faltan,
            - crea índice/vista solo si ya existe la columna 'capitulo'.
    """

    def __init__(self, conn: psycopg.Connection, engine: Optional[Engine] = None) -> None:
        self.conn = conn
        self.engine = engine  # opcional

        # 1) Tabla de ejecuciones
        with self.conn.cursor() as cur:
            cur.execute(DDL_SNAPSHOT_RUN)
        self.conn.commit()

        # 2) Migración liviana
        self._migrate_resumen_table()

    # ────────────────────────────────────────────────────────────────────
    # API pública
    # ────────────────────────────────────────────────────────────────────
    def insert_snapshot_run(self, snapshot_run_id: str, params: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO snapshot.snapshot_run (snapshot_run_id, started_at, params_json)
                VALUES (%s, %s, %s)
                """,
                (snapshot_run_id, datetime.now(timezone.utc), json.dumps(params)),
            )
        self.conn.commit()

    # ────────────────────────────────────────────────────────────────────
    # Migración ligera (sin renombrar columnas en BD)
    # ────────────────────────────────────────────────────────────────────
    def _migrate_resumen_table(self) -> None:
        """
        Si existe snapshot.resumen_seguimiento:
          - Si es **vista** → DROP VIEW/MATERIALIZED VIEW y salir (el loader creará la tabla).
          - Si es **tabla**:
              · añade columnas meta si faltan,
              · crea índice y vista solo si existe la columna 'capitulo'.
        """
        with self.conn.cursor() as cur:
            # ¿Existe el objeto y de qué tipo?
            cur.execute(
                """
                SELECT c.relkind   -- 'r' tabla, 'v' vista, 'm' mat. view, etc.
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'snapshot' AND c.relname = 'resumen_seguimiento'
                """
            )
            row = cur.fetchone()
            if row is None:
                # No existe ni tabla ni vista → el loader la creará cuando cargue.
                self.conn.commit()
                return

            relkind = row[0]

            # Si es vista o vista materializada → la eliminamos y salimos.
            if relkind in ("v", "m"):
                # Intentamos ambas por seguridad
                try:
                    cur.execute('DROP MATERIALIZED VIEW IF EXISTS snapshot.resumen_seguimiento CASCADE;')
                except Exception:
                    pass
                try:
                    cur.execute('DROP VIEW IF EXISTS snapshot.resumen_seguimiento CASCADE;')
                except Exception:
                    pass
                self.conn.commit()
                return

            # Si no es tabla ('r'), no hacemos nada más.
            if relkind != "r":
                self.conn.commit()
                return

            # A partir de aquí, es una tabla.
            # 1) Asegurar columnas meta (sin tocar nombres de negocio)
            cur.execute(
                """
                ALTER TABLE snapshot.resumen_seguimiento
                ADD COLUMN IF NOT EXISTS snapshot_run_id  uuid,
                ADD COLUMN IF NOT EXISTS snapshot_run_ts  timestamptz;
                """
            )

            # 2) ¿Existe ya la columna 'capitulo'?
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.columns
                  WHERE table_schema='snapshot' AND table_name='resumen_seguimiento'
                    AND column_name='capitulo'
                )
                """
            )
            has_capitulo = bool(cur.fetchone()[0])

            # 3) Si 'capitulo' existe, (re)creamos índice y vista en torno a esa columna.
            if has_capitulo:
                cur.execute("DROP INDEX IF EXISTS snapshot.idx_resumen_key;")
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_resumen_key
                    ON snapshot.resumen_seguimiento
                    (company_name, project_no, period_date, grupo, capitulo, nivel);
                    """
                )

                cur.execute(
                    """
                    CREATE OR REPLACE VIEW snapshot.resumen_seguimiento_latest_v AS
                    SELECT DISTINCT ON (company_name, project_no, period_date, grupo, capitulo, nivel)
                           *
                    FROM snapshot.resumen_seguimiento
                    ORDER BY company_name, project_no, period_date, grupo, capitulo, nivel, snapshot_run_ts DESC;
                    """
                )
            else:
                # Si aún no existe 'capitulo', evitamos crear objetos que dependan de ella.
                # El loader añadirá la columna y podrá crear/actualizar estos objetos en su siguiente ejecución.
                try:
                    cur.execute("DROP VIEW IF EXISTS snapshot.resumen_seguimiento_latest_v;")
                except Exception:
                    pass

        self.conn.commit()
