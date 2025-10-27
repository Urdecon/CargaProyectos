# interface_adapters/controllers/resumen_controller.py
from __future__ import annotations
from application.pipeline.pipeline import ResumenSnapshotPipeline, PipelineParams


class ResumenController:
    def __init__(self, pipeline: ResumenSnapshotPipeline) -> None:
        self.pipeline = pipeline

    def snapshot_mes(self, company_name: str | None, project_no: str | None, year: int, month: int):
        return self.pipeline.run(PipelineParams(company_name, project_no, year, month))
