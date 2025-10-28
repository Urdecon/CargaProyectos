# domain/models/resumen_snapshot.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class ResumenSnapshotRow:
    company_name: str
    project_no: str
    project_description: str
    period_date: date
    grupo: str
    linea: str
    nivel: str

    coste_bc_origen: Optional[float]
    coste_bc_mes_anterior: Optional[float]
    coste_bc_mes_actual: Optional[float]
    coste_qwark: Optional[float]
    coste_pendiente: Optional[float]
    coste_total_origen: Optional[float]
    produccion_origen: Optional[float]
    resultado: Optional[float]
    pct_resultado: Optional[float]
    contratos: Optional[float]
    planificado: Optional[float]
    pct_avance_venta_contrato: Optional[float]
    pct_avance_coste_planificado: Optional[float]

    values_hash: str
    snapshot_run_id: str
    snapshot_run_ts: datetime
