# infrastructure/postgres/db.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@contextmanager
def get_conn(dsn: str) -> Iterator[psycopg2.extensions.connection]:
    """Conexión psycopg2 (DDL/escritura)."""
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_engine(sa_dsn: str) -> Engine:
    """Engine SQLAlchemy (lecturas con pandas)."""
    return create_engine(sa_dsn, pool_pre_ping=True)
