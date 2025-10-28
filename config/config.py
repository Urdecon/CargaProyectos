# infrastructure/config.py

from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Carga .env en el momento de importar este módulo
# (antes de evaluar os.getenv de abajo)
load_dotenv(dotenv_path=".env")

@dataclass
class Config:
    # Credenciales base (compartidas)
    PG_SERVER: str = os.getenv("PG_SERVER", "localhost")
    PG_PORT: int = int(os.getenv("PG_PORT", "5432"))
    PG_USER: str = os.getenv("PG_USER", "../infrastructure/postgres")
    PG_PASSWORD: str = os.getenv("PG_PASSWORD", "")

    # BD destino (snapshot)
    PG_DATABASE: str = os.getenv("PG_DATABASE", "snapshot")

    # BD orígenes
    BC_DATABASE: str = os.getenv("BC_DATABASE", "business_central")
    EXCEL_DATABASE: str = os.getenv("EXCEL_DATABASE", "excel_prod")

    # Overrides opcionales (DSN explícitos para SQLAlchemy)
    BC_DSN: str | None = os.getenv("BC_DSN")
    EXCEL_DSN: str | None = os.getenv("EXCEL_DSN")

    @classmethod
    def snapshot_psycopg2_dsn(cls) -> str:
        c = cls()
        return f"postgresql://{c.PG_USER}:{c.PG_PASSWORD}@{c.PG_SERVER}:{c.PG_PORT}/{c.PG_DATABASE}"

    @classmethod
    def bc_sqlalchemy_dsn(cls) -> str:
        c = cls()
        if c.BC_DSN:
            return c.BC_DSN
        return f"postgresql+psycopg2://{c.PG_USER}:{c.PG_PASSWORD}@{c.PG_SERVER}:{c.PG_PORT}/{c.BC_DATABASE}"

    @classmethod
    def excel_sqlalchemy_dsn(cls) -> str:
        c = cls()
        if c.EXCEL_DSN:
            return c.EXCEL_DSN
        return f"postgresql+psycopg2://{c.PG_USER}:{c.PG_PASSWORD}@{c.PG_SERVER}:{c.PG_PORT}/{c.EXCEL_DATABASE}"
