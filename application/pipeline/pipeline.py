# application/pipeline/pipeline.py
from __future__ import annotations
# application/pipeline/pipeline.py

from dataclasses import dataclass
from typing import Optional

from interface_adapters.repositories.source_repository import SourceRepository
from interface_adapters.repositories.snapshot_repository import SnapshotRepository
from application.pipeline.steps.extract_resumen import ExtractResumenStep, ExtractOutput
from application.pipeline.steps.transform_resumen import TransformResumenStep, TransformOutput
from application.pipeline.steps.load_resumen import LoadResumenStep

from application.pipeline.steps.load_facturas import LoadFacturasStep
from application.pipeline.steps.load_qwark_coste_docs import LoadQwarkCosteDocsStep

from application.pipeline.steps.load_pendientes_jo import LoadPendientesJOStep
from application.pipeline.steps.load_seguimiento_produccion import LoadSeguimientoProduccionStep
from application.pipeline.steps.load_resource_ledger_entries import LoadResourceLedgerEntriesStep



@dataclass
class PipelineParams:
    company_name: Optional[str]
    project_no: Optional[str]
    year: int
    month: int


class ResumenSnapshotPipeline:
    def __init__(self, source_repo: SourceRepository, snapshot_repo: SnapshotRepository) -> None:
        self.source_repo = source_repo
        self.snapshot_repo = snapshot_repo

        self.extract = ExtractResumenStep(self.source_repo)
        self.transform = TransformResumenStep()
        self.load = LoadResumenStep(snap_engine=self.snapshot_repo.engine)

        self.load_facturas = LoadFacturasStep(snap_engine=self.snapshot_repo.engine)
        self.load_qwark_docs = LoadQwarkCosteDocsStep(snap_engine=self.snapshot_repo.engine)

        self.load_pendientes_jo = LoadPendientesJOStep(snap_engine=self.snapshot_repo.engine)
        self.load_seguimiento = LoadSeguimientoProduccionStep(snap_engine=self.snapshot_repo.engine)
        self.load_rle = LoadResourceLedgerEntriesStep(snap_engine=self.snapshot_repo.engine)


    def run(self, params: PipelineParams) -> dict:
        ext: ExtractOutput = self.extract.run(
            params.company_name,
            params.project_no,
            params.year,
            params.month,
        )
        tr: TransformOutput = self.transform.run(ext)

        ctx = {
            "company_name": tr.company_name,
            "project_no": tr.project_no,
            "period_date": tr.last_day.date(),
            "year": tr.year,
            "month": tr.month,
        }

        out_resumen = self.load.run(tr.rows, ctx)

        # Facturas por capítulo (ya existente)
        try:
            df_facturas = self.source_repo.fetch_facturas_por_capitulo(
                company_display_name=params.company_name,
                project_no=params.project_no,
                d1=ext.month_start,
                d2=ext.month_end,
            )
            if df_facturas is not None and not df_facturas.empty:
                self.load_facturas.run(df_facturas, ctx)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("No se pudo cargar snapshot.facturas: %s", e)

        # Documentos QWARK (ya añadido)
        try:
            df_qwark_docs = self.source_repo.fetch_qwark_coste_docs(
                company_display_name=params.company_name,
                project_no=params.project_no,
                d1=ext.month_start,
                d2=ext.month_end,
            )
            if df_qwark_docs is not None and not df_qwark_docs.empty:
                self.load_qwark_docs.run(df_qwark_docs, ctx)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("No se pudo cargar snapshot.qwark_coste_docs: %s", e)

        # NUEVO: Pendientes JO (excel + join companies_bc)
        try:
            df_pend_jo = self.source_repo.fetch_pendientes_jo_docs(
                company_display_name=params.company_name,
                project_no=params.project_no,
                d1=ext.month_start,
                d2=ext.month_end,
            )
            if df_pend_jo is not None and not df_pend_jo.empty:
                self.load_pendientes_jo.run(df_pend_jo, ctx)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("No se pudo cargar snapshot.pendientes_jo: %s", e)

        # Foto: Seguimiento Producción (excel_prod)
        try:
            df_seg = self.source_repo.fetch_seguimiento_produccion_docs(
                company_display_name=params.company_name,
                project_no=params.project_no,
                d1=ext.month_start,
                d2=ext.month_end,
            )
            if df_seg is not None and not df_seg.empty:
                self.load_seguimiento.run(df_seg, ctx)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("No se pudo cargar snapshot.seguimiento_produccion: %s", e)

        try:
            df_rle = self.source_repo.fetch_resource_ledger_entries(
                company_display_name=params.company_name,
                project_no=params.project_no,
                d1=ext.month_start,
                d2=ext.month_end,
            )
            if df_rle is not None and not df_rle.empty:
                # Reutilizamos ctx para snapshot_run meta
                self.load_rle.run(df_rle, ctx)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("No se pudo cargar snapshot.resource_ledger_entries: %s", e)

        return out_resumen

