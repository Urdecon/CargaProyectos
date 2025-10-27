# interface_adapters/repositories/source_repository.py
from __future__ import annotations

from datetime import date
from typing import Optional, Iterable

import pandas as pd
from sqlalchemy.engine import Engine
from datetime import date, timedelta


def _norm_text_series(s: pd.Series) -> pd.Series:
    """
    Normaliza texto para comparaciones robustas:
      - NBSP → espacio
      - strip
      - casefold
    """
    if s is None:
        return s
    return (
        s.astype(str)
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
        .str.casefold()
    )


def _first_present(candidates: Iterable[str], existing_lower: set[str]) -> Optional[str]:
    """Devuelve el primer nombre de columna cuyo .lower() esté en existing_lower."""
    for c in candidates:
        if c.lower() in existing_lower:
            return c
    return None


class SourceRepository:
    """
    Repositorio de lectura desde dos orígenes PostgreSQL:

      • business_central  (bc_engine)
        - public.job_ledger_entries_bc
        - public.job_list_bc
        - public.companies_bc
        - public.facturas_qwark
        - public.job_task_lines_subform_bc
        - public.companies_qwark   (JOIN por company → name)
        - public.businessUnit_qwark (JOIN por bunisnessUnit/businessUnit → "Unidad de negocio")

      • excel_prod        (excel_engine)
        - public."DimProporcionales"
        - public."DimPendientesProduccion"
        - public."DimSeguimientoProduccion"
    """

    def __init__(self, *, bc_engine: Engine, excel_engine: Engine) -> None:
        self.bc_engine = bc_engine
        self.excel_engine = excel_engine

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    def _resolve_company_id(self, company_name: str, project_no: str) -> Optional[str]:
        sql = """
        SELECT j."CompanyId" AS company_id
        FROM public.job_list_bc j
        JOIN public.companies_bc c ON c."id" = j."CompanyId"
        WHERE c."name" = %(company)s AND j."No" = %(project)s
        LIMIT 1
        """
        df = pd.read_sql(sql, self.bc_engine, params={"company": company_name, "project": project_no})
        return df["company_id"].iloc[0] if not df.empty else None

    # ---------------------------------------------------------------------
    # Origen: business_central
    # ---------------------------------------------------------------------
    def fetch_fact_ledger_entries(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
        date_to: date,
    ) -> pd.DataFrame:
        sql = """
        SELECT
            fle."Posting_Date"::date                      AS posting_date,
            fle."Gen_Prod_Posting_Group"                  AS gppg,
            fle."Job_No"                                  AS job_no,
            fle."Job_Task_No"                             AS job_task_no,
            fle."Total_Cost_LCY"                          AS total_cost_lcy,
            fle."Line_Amount_LCY"                         AS line_amount_lcy,
            c."name"                                      AS company_name,
            j."CompanyId"                                 AS company_id,
            j."No"                                        AS project_no,
            j."Description"                               AS project_description,
            (j."No" || '_' || j."CompanyId")              AS project_id
        FROM public.job_ledger_entries_bc fle
        JOIN public.job_list_bc j
          ON j."No" = fle."Job_No" AND j."CompanyId" = fle."CompanyId"
        JOIN public.companies_bc c
          ON c."id" = j."CompanyId"
        WHERE fle."Posting_Date"::date <= %(date_to)s
          AND (%(company)s IS NULL OR c."name" = %(company)s)
          AND (%(project)s IS NULL OR j."No" = %(project)s)
        """
        return pd.read_sql(
            sql,
            self.bc_engine,
            params={"date_to": date_to, "company": company_display_name, "project": project_no},
        )

    def fetch_tasklines(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
    ) -> pd.DataFrame:
        sql = """
        SELECT
            t."Job_Task_No"                           AS job_task_no,
            t."Schedule_Total_Price"                  AS schedule_total_price,
            t."Schedule_Total_Cost"                   AS schedule_total_cost,
            j."No"                                    AS project_no,
            c."name"                                  AS company_name
        FROM public.job_task_lines_subform_bc t
        JOIN public.job_list_bc j
          ON (t."Job_No" || '_' || t."CompanyId") = (j."No" || '_' || j."CompanyId")
        JOIN public.companies_bc c
          ON c."id" = j."CompanyId"
        WHERE (%(company)s IS NULL OR c."name" = %(company)s)
          AND (%(project)s IS NULL OR j."No" = %(project)s)
        """
        return pd.read_sql(sql, self.bc_engine, params={"company": company_display_name, "project": project_no})

    # ---------------------------------------------------------------------
    # QWARK pendiente
    # ---------------------------------------------------------------------
    # interface_adapters/repositories/source_repository.py
    def fetch_qwark_pendiente(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            date_to: date,
    ) -> pd.DataFrame:
        """
        Facturas Qwark NO registradas hasta 'date_to'.
        - 'registrada' = True si document_no (upper) contiene 'CFR'; else False.
        - Se filtra base_amount > 0 y registrada == False.
        - JOIN con:
            · companies_qwark     → name AS company_name_bc (filtro de empresa)
            · businessUnit_qwark  → "Unidad de negocio" para derivar 'proyecto'
        - 'proyecto' = si hay posting_jobNo → lo usamos.
                       si no, de 'Unidad de negocio': PY_00 + primeros 4 dígitos (cuando existan).
        - Se devuelve **incluyendo** 'document_no' para trazabilidad.
        """
        # --- detección robusta de nombres reales de columnas en facturas_qwark ---
        cols_df = pd.read_sql("""
            SELECT lower(column_name) AS column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='facturas_qwark'
        """, self.bc_engine)

        cols_lower = set(c.lower() for c in cols_df["column_name"].tolist())

        def _first_present(cands: list[str], pool: set[str]) -> Optional[str]:
            for c in cands:
                if c.lower() in pool:
                    return c
            return None

        col_doc_date = _first_present(["documentDate", "document_date"], cols_lower) or "documentDate"
        col_doc_no = _first_present(["bc_documentNo", "bc_documentNo"], cols_lower) or "bc_documentNo"
        col_base_amount = _first_present(["baseAmount", "base_amount"], cols_lower) or "baseAmount"
        col_posting_job_no = _first_present(["posting_jobNo", "posting_job_no", "project_no"],
                                            cols_lower) or "posting_jobNo"
        col_posting_job_task_no = _first_present(
            ["posting_jobTaskNo", "posting_job_task_no", "job_task_no", "capitulo_codigo", "capitulo"],
            cols_lower,
        )
        col_company_in_fq = _first_present(["company", "companyid", "company_id"], cols_lower)
        col_bu_in_fq = _first_present(["bunisnessUnit", "businessUnit", "business_unit", "businessunit"], cols_lower)
        col_doc_orig_no= _first_present(["documentNo"], cols_lower)


        # nombres de tablas/columnas con comillas para SQL seguro
        def q(ident: str) -> str:
            return '"' + ident + '"'

        tbl_fq = 'public."facturas_qwark"'
        tbl_cq = 'public."companies_qwark"'
        tbl_bu = 'public."businessUnit_qwark"'

        sel_job_task = f", fq.{q(col_posting_job_task_no)} AS posting_job_task_no" if col_posting_job_task_no else ", NULL AS posting_job_task_no"
        sel_company_join = f"""
            LEFT JOIN {tbl_cq} cq
                   ON cq."id" = fq.{q(col_company_in_fq)}
        """ if col_company_in_fq else ""

        sel_bu_join = f"""
            LEFT JOIN {tbl_bu} bu
                   ON bu."id" = fq.{q(col_bu_in_fq)}
        """ if col_bu_in_fq else ""

        sql = f"""
            SELECT
                fq.{q(col_doc_date)}::date   AS fecha_documento,
                fq.{q(col_doc_no)}           AS document_no,
                fq.{q(col_base_amount)}      AS base_amount,
                fq.{q(col_posting_job_no)}   AS posting_job_no
                {sel_job_task},
                cq."name"                    AS company_name_bc,
                bu."Unidad de negocio"       AS unidad_negocio,
                fq.{q(col_bu_in_fq)}         AS business_unit_id,
                fq.{q(col_doc_orig_no)}         AS documentNo_orig
            FROM {tbl_fq} fq
            {sel_company_join}
            {sel_bu_join}
            WHERE fq.{q(col_doc_date)}::date <= %(date_to)s
        """


        df = pd.read_sql(sql, self.bc_engine, params={"date_to": date_to})

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "fecha_factura", "document_no", "registrada", "base_amount",
                    "job_task_no", "project_no", "proyecto",
                    "company_name", "company_name_bc", "business_unit_id"
                ]
            )

        # Renombres baseline
        df = df.rename(
            columns={
                "fecha_documento": "fecha_factura",
                "posting_job_no": "project_no",
                "posting_job_task_no": "job_task_no",
            }
        )

        # registrada: True si document_no contiene "CFR"
        doc = df["document_no"].fillna("").astype(str)
        df["registrada"] = doc.str.upper().str.contains("CFR", na=False)

        # Normalización de 'proyecto'
        # 1) si hay project_no → lo usamos
        df["proyecto"] = df["project_no"]

        # 2) si no hay project_no, derivamos desde 'unidad_negocio' (PY_00 + primeros 4 dígitos)
        def _derive_py_from_un(unidad: str) -> Optional[str]:
            if not unidad:
                return None
            # extrae primeros 4 dígitos consecutivos
            import re
            m = re.search(r"(\d{4})", str(unidad))
            if not m:
                return None
            return f"PY_00{m.group(1)}"

        mask_empty_job = df["proyecto"].isna() | (df["proyecto"].astype(str).str.strip() == "")
        if "unidad_negocio" in df.columns:
            df.loc[mask_empty_job, "proyecto"] = df.loc[mask_empty_job, "unidad_negocio"].apply(_derive_py_from_un)


        # Filtro de negocio: base_amount>0 y ~registrada
        df = df[(df["base_amount"] > 0) & (~df["registrada"])].copy()

        # Filtros por compañía/proyecto (usando company_name_bc y 'proyecto')
        if company_display_name:
            before = len(df)
            df = df[df["company_name_bc"] == company_display_name].copy()

        if project_no:
            before = len(df)
            # usamos 'proyecto' para filtrar (no 'project_no')
            df = df[df["proyecto"] == project_no].copy()

        # Columnas finales (incluyendo document_no para trazabilidad)
        keep = [
            "fecha_factura",
            "documentNo_orig",
            "registrada",
            "base_amount",
            "job_task_no",
            "project_no",
            "proyecto",
            "company_name_bc",
            "business_unit_id",
        ]

        # Compat: exponemos también 'company_name' para Transform (si lo usa)
        df["company_name"] = df.get("company_name_bc")

        # Orden y tipos
        for c in keep:
            if c not in df.columns:
                df[c] = None

        out = df[keep + ["company_name"]].reset_index(drop=True)

        return out

    # ---------------------------------------------------------------------
    # Origen: excel_prod
    # ---------------------------------------------------------------------
    def fetch_proporcionales(self, project_no: str, company_name: str) -> pd.DataFrame:
        company_id = self._resolve_company_id(company_name, project_no)
        if company_id is None:
            return pd.DataFrame(columns=["gastos_generales", "postventa"])

        sql = """
        SELECT
            dp."gastos generales" AS gastos_generales,
            dp."postventa"        AS postventa
        FROM public."DimProporcionales" dp
        WHERE dp."number" = %(project)s
          AND dp."CompanyId"::text = %(company_id)s
        """
        return pd.read_sql(
            sql,
            self.excel_engine,
            params={"project": project_no, "company_id": str(company_id)},
        )

    def _filter_mes_empresa_proyecto(
        self,
        df: pd.DataFrame,
        d1: date,
        d2: date,
        company_display_name: Optional[str],
        project_no: Optional[str],
        col_fecha: str = "fecha_produccion",
        col_empresa: str = "company_name",
        col_proyecto: str = "project_no",
    ) -> pd.DataFrame:
        if df.empty:
            return df

        if col_empresa in df.columns:
            df[col_empresa + "_norm"] = _norm_text_series(df[col_empresa])
        if col_proyecto in df.columns:
            df[col_proyecto + "_norm"] = _norm_text_series(df[col_proyecto])

        target_company = _norm_text_series(pd.Series([company_display_name]))[0] if company_display_name else None
        target_project = _norm_text_series(pd.Series([project_no]))[0] if project_no else None

        df[col_fecha] = pd.to_datetime(df[col_fecha], errors="coerce")
        mask = df[col_fecha].notna()
        mask &= (df[col_fecha].dt.date >= d1) & (df[col_fecha].dt.date <= d2)
        if target_company is not None and (col_empresa + "_norm") in df.columns:
            mask &= df[col_empresa + "_norm"] == target_company
        if target_project is not None and (col_proyecto + "_norm") in df.columns:
            mask &= df[col_proyecto + "_norm"] == target_project

        out = df.loc[mask].copy()
        drop_aux = [c for c in [col_empresa + "_norm", col_proyecto + "_norm"] if c in out.columns]
        if drop_aux:
            out.drop(columns=drop_aux, inplace=True)

        return out

    def fetch_pendientes_mes(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
        d1: date,
        d2: date,
    ) -> pd.DataFrame:
        sql = """
        SELECT
            d."fecha_produccion"          AS fecha_produccion,
            d."coste_pendiente"           AS coste_pendiente,
            COALESCE(d."capitulo_codigo"::text, d."capitulo"::text) AS job_task_no,
            d."proyecto"                  AS project_no,
            d."empresa"                   AS company_name
        FROM public."DimPendientesProduccion" d
        """
        df = pd.read_sql(sql, self.excel_engine)

        df = self._filter_mes_empresa_proyecto(
            df, d1, d2, company_display_name, project_no,
            col_fecha="fecha_produccion",
            col_empresa="company_name",
            col_proyecto="project_no",
        )

        want = ["fecha_produccion", "coste_pendiente", "job_task_no", "project_no", "company_name"]
        for c in want:
            if c not in df.columns:
                df[c] = None
        return df[want].reset_index(drop=True)

    def fetch_produccion_mes(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
        d1: date,
        d2: date,
    ) -> pd.DataFrame:
        sql = """
        SELECT
            d."fecha_produccion"          AS fecha_produccion,
            d."certificacion_pendiente"   AS certificacion_pendiente,
            d."resto_produccion"          AS resto_produccion,
            COALESCE(d."capitulo_codigo"::text, d."capitulo"::text) AS job_task_no,
            d."proyecto"                  AS project_no,
            d."empresa"                   AS company_name
        FROM public."DimSeguimientoProduccion" d
        """
        df = pd.read_sql(sql, self.excel_engine)

        df = self._filter_mes_empresa_proyecto(
            df, d1, d2, company_display_name, project_no,
            col_fecha="fecha_produccion",
            col_empresa="company_name",
            col_proyecto="project_no",
        )

        want = [
            "fecha_produccion",
            "certificacion_pendiente",
            "resto_produccion",
            "job_task_no",
            "project_no",
            "company_name",
        ]
        for c in want:
            if c not in df.columns:
                df[c] = None
        return df[want].reset_index(drop=True)

    # ───────────────────────── util: ventana de mes anterior ─────────────────────────
    @staticmethod
    def _prev_month_bounds(d2: date) -> tuple[date, date]:
        """
        A partir de d2 (fin del mes actual que estés usando en el filtro),
        devuelve (primer_dia_mes_anterior, ultimo_dia_mes_anterior).
        """
        last_day_prev = d2.replace(day=1) - timedelta(days=1)
        first_day_prev = last_day_prev.replace(day=1)
        return first_day_prev, last_day_prev

    # ───────────────────────── MES ANTERIOR: Pendientes ─────────────────────────
    def fetch_pendientes_mes_anterior(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,  # no se usa para la ventana; se mantiene firma para consistencia
            d2: date,
    ) -> pd.DataFrame:
        """
        Igual que fetch_pendientes_mes, pero filtrando por la ventana del MES ANTERIOR a d2.
        """
        sql = """
        SELECT
            d."fecha_produccion"          AS fecha_produccion,
            d."coste_pendiente"           AS coste_pendiente,
            COALESCE(d."capitulo_codigo"::text, d."capitulo"::text) AS job_task_no,
            d."proyecto"                  AS project_no,
            d."empresa"                   AS company_name
        FROM public."DimPendientesProduccion" d
        """
        df = pd.read_sql(sql, self.excel_engine)

        prev_d1, prev_d2 = self._prev_month_bounds(d2)

        df = self._filter_mes_empresa_proyecto(
            df, prev_d1, prev_d2, company_display_name, project_no,
            col_fecha="fecha_produccion",
            col_empresa="company_name",
            col_proyecto="project_no",
        )

        want = ["fecha_produccion", "coste_pendiente", "job_task_no", "project_no", "company_name"]
        for c in want:
            if c not in df.columns:
                df[c] = None
        return df[want].reset_index(drop=True)

    # ───────────────────────── MES ANTERIOR: Producción (cert. pendiente / resto) ─────────────────────────
    def fetch_produccion_mes_anterior(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,  # no se usa para la ventana; se mantiene firma para consistencia
            d2: date,
    ) -> pd.DataFrame:
        """
        Igual que fetch_produccion_mes, pero filtrando por la ventana del MES ANTERIOR a d2.
        """
        sql = """
        SELECT
            d."fecha_produccion"          AS fecha_produccion,
            d."certificacion_pendiente"   AS certificacion_pendiente,
            d."resto_produccion"          AS resto_produccion,
            COALESCE(d."capitulo_codigo"::text, d."capitulo"::text) AS job_task_no,
            d."proyecto"                  AS project_no,
            d."empresa"                   AS company_name
        FROM public."DimSeguimientoProduccion" d
        """
        df = pd.read_sql(sql, self.excel_engine)

        prev_d1, prev_d2 = self._prev_month_bounds(d2)

        df = self._filter_mes_empresa_proyecto(
            df, prev_d1, prev_d2, company_display_name, project_no,
            col_fecha="fecha_produccion",
            col_empresa="company_name",
            col_proyecto="project_no",
        )

        want = [
            "fecha_produccion",
            "certificacion_pendiente",
            "resto_produccion",
            "job_task_no",
            "project_no",
            "company_name",
        ]
        for c in want:
            if c not in df.columns:
                df[c] = None

        return df[want].reset_index(drop=True)

    def fetch_job_planning_lines(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
    ) -> pd.DataFrame:
        """
        Devuelve las líneas de planificación (job_planning_lines_bc) sin filtrar,
        con nombres de columnas estandarizados que usa TransformResumenStep:

            - Planning_Date
            - Gen_Prod_Posting_Group
            - Line_Amount
            - Job_No
            - Job_Task_No
            - CompanyId        (preferido para filtrar por compañía)
            - Company_Name     (fallback si no hay CompanyId)

        NOTA: la clase Transform aplica los filtros por fecha/compañía/proyecto,
        así que aquí no filtramos por d1/d2.
        """
        try:
            df = pd.read_sql('SELECT * FROM public."job_planning_lines_bc"', self.bc_engine)
        except Exception:
            # Si el nombre no está entrecomillado en PG:
            df = pd.read_sql("SELECT * FROM public.job_planning_lines_bc", self.bc_engine)

        if df is None or df.empty:
            # devolver DF con las columnas esperadas vacías
            return pd.DataFrame(
                columns=[
                    "Planning_Date",
                    "Gen_Prod_Posting_Group",
                    "Line_Amount",
                    "Job_No",
                    "Job_Task_No",
                    "CompanyId",
                    "Company_Name",
                ]
            )

        cols_lower = {c.lower(): c for c in df.columns}

        def pick(*cands: str) -> Optional[str]:
            for cc in cands:
                if cc.lower() in cols_lower:
                    return cols_lower[cc.lower()]
            return None

        col_date = pick("Planning_Date", "planning_date", "PlanningDate", "planningdate", "Date", "date")
        col_gppg = pick("Gen_Prod_Posting_Group", "gen_prod_posting_group", "GPPG", "gppg", "GenProdPostingGroup")
        col_amt = pick("Line_Amount", "line_amount", "Amount", "amount", "LineAmount")
        col_job = pick("Job_No", "job_no", "Project_No", "project_no")
        col_task = pick("Job_Task_No", "job_task_no", "Task_No", "task_no")
        col_cid = pick("CompanyId", "company_id", "Company_Id", "company", "Company")
        col_cnm = pick("Company_Name", "company_name", "Name", "name")

        out = pd.DataFrame()
        out["Planning_Date"] = df[col_date] if col_date else pd.NaT
        out["Gen_Prod_Posting_Group"] = df[col_gppg] if col_gppg else None
        out["Line_Amount"] = pd.to_numeric(df[col_amt], errors="coerce") if col_amt else 0.0
        out["Job_No"] = df[col_job] if col_job else None
        out["Job_Task_No"] = df[col_task] if col_task else None
        out["CompanyId"] = df[col_cid].astype(str) if col_cid else None
        out["Company_Name"] = df[col_cnm] if col_cnm else None

        # Normalizaciones ligeras
        if "CompanyId" in out and out["CompanyId"].notna().any():
            out["CompanyId"] = out["CompanyId"].astype(str).str.strip()

        return out

    def fetch_fle_until(self, date_to, company_name, project_no):
        pass
