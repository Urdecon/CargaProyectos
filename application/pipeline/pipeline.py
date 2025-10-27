# application/pipeline/pipeline.py
from __future__ import annotations
# application/pipeline/pipeline.py

from dataclasses import dataclass
from datetime import date
from typing import Optional

from interface_adapters.repositories.source_repository import SourceRepository
from interface_adapters.repositories.snapshot_repository import SnapshotRepository
from application.pipeline.steps.extract_resumen import ExtractResumenStep, ExtractOutput
from application.pipeline.steps.transform_resumen import TransformResumenStep, TransformOutput
from application.pipeline.steps.load_resumen import LoadResumenStep


@dataclass
class PipelineParams:
    company_name: Optional[str]
    project_no: Optional[str]
    year: int
    month: int


class ResumenSnapshotPipeline:
    """
    Orquesta los pasos:
      1) Extract
      2) Transform
      3) Load
    """
    def __init__(self, source_repo: SourceRepository, snapshot_repo: SnapshotRepository) -> None:
        self.source_repo = source_repo
        self.snapshot_repo = snapshot_repo

        self.extract = ExtractResumenStep(self.source_repo)
        self.transform = TransformResumenStep()
        # 🔧 IMPORTANTE: pasar el Engine, no el repo.
        self.load = LoadResumenStep(snap_engine=self.snapshot_repo.engine)

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
            "period_date": tr.last_day.date(),  # último día del mes
            "year": tr.year,
            "month": tr.month,
        }
        return self.load.run(tr.rows, ctx)
