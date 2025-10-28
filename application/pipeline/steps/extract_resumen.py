# application/pipeline/steps/extract_resumen.py
from __future__ import annotations
# application/pipeline/steps/extract_resumen.py

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

from interface_adapters.repositories.source_repository import SourceRepository


@dataclass
class ExtractOutput:
    company_name: Optional[str]
    company_id: Optional[str]
    project_no: Optional[str]
    selected_year: int
    month_start: date
    month_end: date
    period_date: date

    fle: pd.DataFrame
    qwark: pd.DataFrame
    pendientes_mes: pd.DataFrame
    produccion_mes: pd.DataFrame
    tasklines: pd.DataFrame
    proporcionales: pd.DataFrame
    planning_lines: pd.DataFrame

    # NUEVO: dataset para “Facturas por capítulo”
    facturas_capitulo: pd.DataFrame = field(default_factory=lambda: pd.DataFrame())


class ExtractResumenStep:
    def __init__(self, repo: SourceRepository) -> None:
        self.repo = repo

    @staticmethod
    def _last_day(year: int, month: int) -> date:
        from calendar import monthrange
        return date(year, month, monthrange(year, month)[1])

    def run(self, company_name: Optional[str], project_no: Optional[str], year: int, month: int) -> ExtractOutput:
        period_date = self._last_day(year, month)
        month_start = date(year, month, 1)
        month_end = period_date

        fle = self.repo.fetch_fact_ledger_entries(company_name, project_no, period_date)
        qwark = self.repo.fetch_qwark_pendiente(company_name, project_no, period_date)
        pendientes = self.repo.fetch_pendientes_mes(company_name, project_no, month_start, month_end)
        produccion = self.repo.fetch_produccion_mes(company_name, project_no, month_start, month_end)
        task = self.repo.fetch_tasklines(company_name, project_no)
        prop = self.repo.fetch_proporcionales(project_no, company_name) if (company_name and project_no) else \
            pd.DataFrame(columns=["gastos_generales", "postventa"])
        planning = self.repo.fetch_job_planning_lines(company_name, project_no)
        # CompanyId: preferimos el que viene en FLE; si no, resolvemos por repo
        company_id = None
        if "company_id" in fle.columns and not fle.empty:
            vals = fle["company_id"].dropna().astype(str).unique().tolist()
            company_id = vals[0] if vals else None
        if company_id is None and company_name and project_no:
            company_id = self.repo._resolve_company_id(company_name, project_no)

        # NUEVO: “Facturas por capítulo”
        facturas_capitulo = self.repo.fetch_facturas_por_capitulo(
            company_display_name=company_name,
            project_no=project_no,
            d1=month_start,
            d2=month_end,
        )

        return ExtractOutput(
            company_name=company_name,
            company_id=str(company_id) if company_id is not None else None,
            project_no=project_no,
            selected_year=year,
            month_start=month_start,
            month_end=month_end,
            period_date=period_date,
            fle=fle,
            qwark=qwark,
            pendientes_mes=pendientes,
            produccion_mes=produccion,
            tasklines=task,
            proporcionales=prop,
            facturas_capitulo=facturas_capitulo,
            planning_lines=planning,
        )
