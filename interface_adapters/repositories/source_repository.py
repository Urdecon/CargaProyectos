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
        - public.companies_qwark
        - public.businessUnit_qwark

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
    def fetch_qwark_pendiente(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            date_to: date,
    ) -> pd.DataFrame:
        """
        Facturas Qwark NO registradas hasta 'date_to'.
        Ver docstrings previos para el detalle.
        """
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

        df = df.rename(
            columns={
                "fecha_documento": "fecha_factura",
                "posting_job_no": "project_no",
                "posting_job_task_no": "job_task_no",
            }
        )

        doc = df["document_no"].fillna("").astype(str)
        df["registrada"] = doc.str.upper().str.contains("CFR", na=False)

        df["proyecto"] = df["project_no"]

        def _derive_py_from_un(unidad: str) -> Optional[str]:
            if not unidad:
                return None
            import re
            m = re.search(r"(\d{4})", str(unidad))
            if not m:
                return None
            return f"PY_00{m.group(1)}"

        mask_empty_job = df["proyecto"].isna() | (df["proyecto"].astype(str).str.strip() == "")
        if "unidad_negocio" in df.columns:
            df.loc[mask_empty_job, "proyecto"] = df.loc[mask_empty_job, "unidad_negocio"].apply(_derive_py_from_un)

        df = df[(df["base_amount"] > 0) & (~df["registrada"])].copy()

        if company_display_name:
            df = df[df["company_name_bc"] == company_display_name].copy()
        if project_no:
            df = df[df["proyecto"] == project_no].copy()

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

        df["company_name"] = df.get("company_name_bc")

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

    # ───────────────────────── MES ANTERIOR: Producción ─────────────────────────
    def fetch_produccion_mes_anterior(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,  # no se usa; firma por consistencia
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
        con nombres de columnas estandarizados.
        """
        try:
            df = pd.read_sql('SELECT * FROM public."job_planning_lines_bc"', self.bc_engine)
        except Exception:
            df = pd.read_sql("SELECT * FROM public.job_planning_lines_bc", self.bc_engine)

        if df is None or df.empty:
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

        if "CompanyId" in out and out["CompanyId"].notna().any():
            out["CompanyId"] = out["CompanyId"].astype(str).str.strip()

        return out

    # ───────────────────────── FACTURAS POR CAPÍTULO (tabla final) ─────────────────────────
    def fetch_facturas_por_capitulo(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,
            d2: date,
    ) -> pd.DataFrame:
        """
        Tabla 'facturas por capítulo'.

        LEFT:  purchase_invoice_lines_bc  (líneas)
        JOIN:  purchase_invoices_bc       (cabecera)      ON (id = InvoiceId AND CompanyId)
        JOIN:  job_ledger_entries_bc      (coste por cap) ON CompanyId||'|'||number = CompanyId||'|'||Document_No

        Filtros:
          - Empresa por companies_bc.name
          - Proyecto por job_ledger_entries_bc.Job_No
          - Fecha por purchase_invoices_bc.invoiceDate ∈ [d1, d2]
          - Excluir CERTIFICACIONES en job_ledger_entries_bc
        """

        # --- detectar nombres reales de columnas (case-insensitive) ---
        def _cols(table: str) -> dict:
            q = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%(t)s
            """
            dfc = pd.read_sql(q, self.bc_engine, params={"t": table})
            return {c.lower(): c for c in dfc["column_name"].tolist()}

        cols_lines = _cols("purchase_invoice_lines_bc")
        cols_head = _cols("purchase_invoices_bc")

        def pick(cols_map: dict, *cands: str) -> Optional[str]:
            for c in cands:
                lc = c.lower()
                if lc in cols_map:
                    return cols_map[lc]
            return None

        # purchase_invoice_lines_bc
        pil_company = pick(cols_lines, "CompanyId", "company_id", "companyid", "company")
        pil_invoiceid = pick(cols_lines, "InvoiceId", "invoiceId", "invoice_id", "invoiceid", "id_invoice")
        if pil_company is None or pil_invoiceid is None:
            raise RuntimeError("purchase_invoice_lines_bc: no se detectó CompanyId/InvoiceId.")

        # purchase_invoices_bc
        pi_id = pick(cols_head, "id", "Id", "ID")
        pi_company = pick(cols_head, "CompanyId", "company_id", "companyid", "company")
        pi_number = pick(cols_head, "number", "Number", "invoice_number", "Invoice_Number")
        pi_vendor = pick(cols_head, "vendorName", "vendor_name", "VendorName")
        pi_date = pick(cols_head, "invoiceDate", "invoice_date", "InvoiceDate")
        if pi_id is None or pi_company is None or pi_number is None or pi_vendor is None or pi_date is None:
            raise RuntimeError("purchase_invoices_bc: faltan columnas id/CompanyId/number/vendorName/invoiceDate.")

        # helper para comillas
        def q(x: str) -> str:
            return f'"{x}"'

        # SQL robusto con nombres detectados
        sql = f"""
        WITH base AS (
            SELECT
                pil.{q(pil_company)}        AS company_id,
                pil.{q(pil_invoiceid)}      AS invoice_id,
                pi.{q(pi_number)}           AS numero_factura,
                pi.{q(pi_vendor)}           AS proveedor,
                pi.{q(pi_date)}::date       AS fecha_factura
            FROM public.purchase_invoice_lines_bc pil
            LEFT JOIN public.purchase_invoices_bc pi
              ON pi.{q(pi_id)} = pil.{q(pil_invoiceid)}
             AND pi.{q(pi_company)} = pil.{q(pil_company)}
        ),
        base_filt AS (
            SELECT b.*
            FROM base b
            JOIN public.companies_bc c ON c."id" = b.company_id
            WHERE (%(company)s IS NULL OR c."name" = %(company)s)
              AND (%(d1)s IS NULL OR b.fecha_factura >= %(d1)s)
              AND (%(d2)s IS NULL OR b.fecha_factura <= %(d2)s)
        )
        SELECT
            j."Job_Task_No"::text                         AS capitulo,
            SUM(j."Total_Cost_LCY")                       AS coste_registrado_a_origen,
            bf.numero_factura                             AS numero_factura,
            bf.proveedor                                  AS proveedor,
            bf.fecha_factura                              AS fecha_factura,
            bf.company_id                                 AS company_id,
            bf.invoice_id                                 AS invoice_id,
            j."Job_No"                                    AS job_no
        FROM base_filt bf
        LEFT JOIN public.job_ledger_entries_bc j
          ON (bf.company_id::text || '|' || COALESCE(bf.numero_factura::text, ''))
           = (j."CompanyId"::text || '|' || COALESCE(j."Document_No"::text, ''))
        WHERE (%(project)s IS NULL OR j."Job_No" = %(project)s)
          AND (j."Gen_Prod_Posting_Group" IS NULL OR j."Gen_Prod_Posting_Group" <> 'CERTIFICACIONES')
        GROUP BY
            j."Job_Task_No",
            bf.numero_factura, bf.proveedor, bf.fecha_factura, bf.company_id, bf.invoice_id, j."Job_No"
        HAVING j."Job_Task_No" IS NOT NULL
        ORDER BY
            bf.fecha_factura, bf.numero_factura, j."Job_Task_No";
        """

        df = pd.read_sql(
            sql,
            self.bc_engine,
            params={
                "company": company_display_name,
                "project": project_no,
                "d1": d1,
                "d2": d2,
            },
        )

        # Normalización final
        df = df.rename(columns={
            "company_id": "CompanyId",
            "invoice_id": "id",
            "job_no": "Job_No",
        })
        df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce").dt.date
        df["coste_registrado_a_origen"] = pd.to_numeric(df["coste_registrado_a_origen"], errors="coerce")

        return df

    def fetch_qwark_coste_docs(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,
            d2: date,
    ) -> pd.DataFrame:
        """
        Devuelve el detalle de los documentos que conforman el Coste QWARK, sin agrupar por capítulo:
          - documentNo, documentDate, vendorName, posting_description,
            bc_draftDocumentNo, posting_jobTaskNo, baseAmount,
            proyecto, company_name, company_id

        Filtros:
          · fecha_documento ∈ [d1, d2]
          · baseAmount > 0
          · ~registrada (document_no contiene 'CFR' → registrada)
          · excluir documentos con 'RET' (en documentNo y/o bc_draftDocumentNo)
          · filtra por empresa y proyecto (como en coste QWARK agregado)

        NOTA: heredamos la derivación de 'proyecto' desde job o unidad de negocio como en fetch_qwark_pendiente.
        """
        # Descubrimos columnas reales
        cols_df = pd.read_sql("""
               SELECT lower(column_name) AS column_name
               FROM information_schema.columns
               WHERE table_schema='public' AND table_name='facturas_qwark'
           """, self.bc_engine)
        cols_lower = set(cols_df["column_name"].tolist())

        def pick(*cands: str) -> Optional[str]:
            for cc in cands:
                if cc.lower() in cols_lower:
                    return cc
            return None

        c_doc_date = pick("documentDate", "document_date")
        c_doc_no = pick("documentNo", "document_no")
        c_vendor = pick("vendorName", "vendor_name")
        c_post_desc = pick("posting_description", "postingDescription")
        c_draft_no = pick("bc_draftDocumentNo", "draftDocumentNo", "bc_draft_document_no")
        c_job_no = pick("posting_jobNo", "posting_jobno", "project_no")
        c_job_task = pick("posting_jobTaskNo", "posting_job_task_no", "job_task_no")
        c_base = pick("baseAmount", "base_amount")
        c_company = pick("company", "companyid", "company_id")
        c_bu = pick("bunisnessUnit", "businessUnit", "business_unit", "businessunit")

        # columnas auxiliares ya usadas en otro fetch
        c_doc_no_orig = pick("bc_documentNo", "bc_documentno")  # por si hay que depurar CFR/RET
        tbl_fq = 'public."facturas_qwark"'
        tbl_cq = 'public."companies_qwark"'
        tbl_bu = 'public."businessUnit_qwark"'

        # SELECT
        sql = f"""
               SELECT
                   fq."{c_doc_date}"::date            AS fecha_factura,
                   fq."{c_doc_no}"                     AS document_no,
                   fq."{c_vendor}"                     AS vendor_name,
                   {"fq." + f'"{c_post_desc}"' if c_post_desc else "NULL"} AS posting_description,
                   {"fq." + f'"{c_draft_no}"' if c_draft_no else "NULL"}   AS bc_draft_document_no,
                   {"fq." + f'"{c_job_task}"' if c_job_task else "NULL"}   AS job_task_no,
                   fq."{c_base}"                       AS base_amount,
                   fq."{c_job_no}"                     AS posting_job_no,
                   cq."name"                           AS company_name,
                   fq."{c_company}"::text              AS company_id,
                   {"fq." + f'"{c_bu}"' if c_bu else "NULL"} AS unidad_negocio,
                   {"fq." + f'"{c_doc_no_orig}"' if c_doc_no_orig else "NULL"} AS document_no_orig
               FROM {tbl_fq} fq
               LEFT JOIN {tbl_cq} cq
                      ON cq."id" = fq."{c_company}"
               {"LEFT JOIN " + tbl_bu + f' bu ON bu."id" = fq."{c_bu}"' if c_bu else ""}
               WHERE fq."{c_doc_date}"::date >= %(d1)s
                 AND fq."{c_doc_date}"::date <= %(d2)s
           """

        df = pd.read_sql(sql, self.bc_engine, params={"d1": d1, "d2": d2})
        if df.empty:
            return pd.DataFrame(columns=[
                "company_id", "company_name", "project_no", "fecha_factura", "document_no",
                "vendor_name", "posting_description", "bc_draft_document_no", "job_task_no", "base_amount"
            ])

        # registrada por patrón "CFR" en document_no_orig (si no, en document_no)
        base_doc = df["document_no_orig"].fillna(df["document_no"]).astype(str)
        df["registrada"] = base_doc.str.upper().str.contains("CFR", na=False)

        # excluir negativos y registradas
        df = df[(df["base_amount"] > 0) & (~df["registrada"])].copy()

        # excluir 'RET' (en document_no_orig y en draft si existe)
        def _has_ret(s: pd.Series) -> pd.Series:
            return s.fillna("").astype(str).str.upper().str.contains("RET", na=False)

        mask_ret = _has_ret(df["document_no"])
        if "document_no_orig" in df.columns:
            mask_ret = mask_ret | _has_ret(df["document_no_orig"])
        if "bc_draft_document_no" in df.columns:
            mask_ret = mask_ret | _has_ret(df["bc_draft_document_no"])
        df = df[~mask_ret].copy()

        # derivar proyecto como en fetch_qwark_pendiente
        df["project_no"] = df["posting_job_no"]

        def derive_from_un(un):
            import re
            if pd.isna(un): return None
            m = re.search(r"(\d{4})", str(un))
            return f"PY_00{m.group(1)}" if m else None

        mask_empty = df["project_no"].isna() | (df["project_no"].astype(str).str.strip() == "")
        if "unidad_negocio" in df.columns:
            df.loc[mask_empty, "project_no"] = df.loc[mask_empty, "unidad_negocio"].apply(derive_from_un)

        # filtros por empresa y proyecto
        if company_display_name:
            df = df[df["company_name"] == company_display_name].copy()
        if project_no:
            df = df[df["project_no"] == project_no].copy()

        # ordenar y columnas finales
        keep = [
            "company_id", "company_name", "project_no", "fecha_factura", "document_no",
            "vendor_name", "posting_description", "bc_draft_document_no", "job_task_no", "base_amount"
        ]
        for k in keep:
            if k not in df.columns:
                df[k] = None

        df = df[keep].sort_values(["fecha_factura", "document_no"]).reset_index(drop=True)
        return df

    def fetch_pendientes_jo_docs(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
        d1: date,
        d2: date,
    ) -> pd.DataFrame:
        """
        Foto de pendientes (DimPendientesProduccion @ excel_prod) con:
          - capitulo, proyecto, coste_pendiente, observaciones, proveedor
          - fecha = fecha_seguimiento
          - company_id vía join por nombre con companies_bc (BC)

        Filtros:
          · fecha_seguimiento ∈ [d1, d2]
          · por empresa (si company_display_name)
          · por proyecto (si project_no)

        Devuelve columnas:
          company_id, company_name, project_no, fecha_seguimiento,
          capitulo, coste_pendiente, observaciones, proveedor
        """
        # ── 1) Leer DimPendientesProduccion desde excel_prod
        try:
            dim_df = pd.read_sql(
                'SELECT * FROM public."DimPendientesProduccion"',
                self.excel_engine,
            )
        except Exception:
            # por si la tabla no estuviera entrecomillada
            dim_df = pd.read_sql(
                "SELECT * FROM public.DimPendientesProduccion",
                self.excel_engine,
            )

        if dim_df is None or dim_df.empty:
            return pd.DataFrame(columns=[
                "company_id","company_name","project_no","fecha_seguimiento",
                "capitulo","coste_pendiente","observaciones","proveedor"
            ])

        cols_lo = {c.lower(): c for c in dim_df.columns}

        def pick_dim(*cands: str) -> Optional[str]:
            for cc in cands:
                if cc.lower() in cols_lo:
                    return cols_lo[cc.lower()]
            return None

        c_fecha_seg  = pick_dim("fecha_seguimiento", "fecha_seguimiento_jo", "fecha_seguim", "fecha")
        c_cap_cod    = pick_dim("capitulo_codigo", "capitulo_cod", "codigo_capitulo")
        c_cap_txt    = pick_dim("capitulo", "cap")
        c_proyecto   = pick_dim("proyecto", "project_no", "job_no")
        c_empresa    = pick_dim("empresa", "company_name", "company")
        c_coste_pen  = pick_dim("coste_pendiente", "costePendiente", "pendiente_coste")
        c_obs        = pick_dim("observaciones", "observacion", "obs")
        c_proveedor  = pick_dim("proveedor", "vendor", "supplier")

        # Normalizar campos esenciales
        if c_fecha_seg:
            dim_df[c_fecha_seg] = pd.to_datetime(dim_df[c_fecha_seg], errors="coerce")
        else:
            # sin fecha no podemos filtrar por ventana: devolvemos vacío
            return pd.DataFrame(columns=[
                "company_id","company_name","project_no","fecha_seguimiento",
                "capitulo","coste_pendiente","observaciones","proveedor"
            ])

        # Ventana de fechas
        mask = dim_df[c_fecha_seg].notna() & (dim_df[c_fecha_seg].dt.date >= d1) & (dim_df[c_fecha_seg].dt.date <= d2)

        # Filtros por empresa y proyecto (si existen columnas)
        if company_display_name and c_empresa:
            mask &= dim_df[c_empresa].astype(str).str.strip() == company_display_name
        if project_no and c_proyecto:
            mask &= dim_df[c_proyecto].astype(str).str.strip() == project_no

        dim_f = dim_df.loc[mask].copy()

        # capitulo = COALESCE(capitulo_codigo, capitulo) :: text
        dim_f["capitulo"] = None
        if c_cap_cod and c_cap_cod in dim_f.columns:
            dim_f["capitulo"] = dim_f[c_cap_cod].astype(str)
        if c_cap_txt and c_cap_txt in dim_f.columns:
            dim_f["capitulo"] = dim_f["capitulo"].fillna(dim_f[c_cap_txt].astype(str))
        dim_f["capitulo"] = dim_f["capitulo"].fillna("None").str.strip().replace("", "None")

        # columnas limpias
        dim_f["fecha_seguimiento"] = dim_f[c_fecha_seg].dt.date
        dim_f["project_no"] = dim_f[c_proyecto] if c_proyecto else None
        dim_f["company_name"] = dim_f[c_empresa] if c_empresa else None
        dim_f["coste_pendiente"] = pd.to_numeric(dim_f[c_coste_pen], errors="coerce") if c_coste_pen else 0.0
        dim_f["observaciones"] = dim_f[c_obs] if c_obs else None
        dim_f["proveedor"] = dim_f[c_proveedor] if c_proveedor else None

        # ── 2) Traer companies_bc (id, name) desde BC y unir por nombre
        comp_df = pd.read_sql(
            'SELECT "id"::text AS company_id, "name" AS company_name FROM public.companies_bc',
            self.bc_engine,
        )
        dim_f = dim_f.merge(comp_df, how="left", on="company_name")

        keep = [
            "company_id","company_name","project_no","fecha_seguimiento",
            "capitulo","coste_pendiente","observaciones","proveedor"
        ]
        for k in keep:
            if k not in dim_f.columns:
                dim_f[k] = None

        out = dim_f[keep].reset_index(drop=True)
        return out

    def fetch_seguimiento_produccion_docs(
        self,
        company_display_name: Optional[str],
        project_no: Optional[str],
        d1: date,
        d2: date,
    ) -> pd.DataFrame:
        """
        Foto de seguimiento de producción (DimSeguimientoProduccion @ excel_prod) con:
          - empresa, fecha_produccion (para filtrar), proyecto, capitulo,
            certificacion_pendiente, resto_produccion, observaciones
          - company_id vía join con companies_bc por nombre

        Filtros:
          · fecha_produccion ∈ [d1, d2]
          · por empresa (si company_display_name)
          · por proyecto (si project_no)

        Devuelve columnas:
          company_id, company_name, project_no, fecha_produccion,
          capitulo, certificacion_pendiente, resto_produccion, observaciones
        """
        # 1) Leer DimSeguimientoProduccion desde excel_prod
        try:
            dim_df = pd.read_sql(
                'SELECT * FROM public."DimSeguimientoProduccion"',
                self.excel_engine,
            )
        except Exception:
            dim_df = pd.read_sql(
                "SELECT * FROM public.DimSeguimientoProduccion",
                self.excel_engine,
            )

        if dim_df is None or dim_df.empty:
            return pd.DataFrame(columns=[
                "company_id","company_name","project_no","fecha_produccion",
                "capitulo","certificacion_pendiente","resto_produccion","observaciones"
            ])

        cols_lo = {c.lower(): c for c in dim_df.columns}

        def pick_dim(*cands: str) -> Optional[str]:
            for cc in cands:
                if cc.lower() in cols_lo:
                    return cols_lo[cc.lower()]
            return None

        c_empresa   = pick_dim("empresa", "company_name", "company")
        c_fecha     = pick_dim("fecha_produccion", "fecha", "date")
        c_proyecto  = pick_dim("proyecto", "project_no", "job_no")
        c_cap_cod   = pick_dim("capitulo_codigo", "capitulo_cod", "codigo_capitulo")
        c_cap_txt   = pick_dim("capitulo", "cap")
        # nota: el origen puede llamarse 'certificacion_pendinte' (typo) o 'certificacion_pendiente'
        c_cert_pen  = pick_dim("certificacion_pendiente", "certificacion_pendinte")
        c_resto     = pick_dim("resto_produccion", "resto")
        c_obs       = pick_dim("observaciones", "observacion", "obs")

        # Normalizar fecha para filtrar
        if c_fecha:
            dim_df[c_fecha] = pd.to_datetime(dim_df[c_fecha], errors="coerce")
        else:
            return pd.DataFrame(columns=[
                "company_id","company_name","project_no","fecha_produccion",
                "capitulo","certificacion_pendiente","resto_produccion","observaciones"
            ])

        # Ventana de fechas
        mask = dim_df[c_fecha].notna() & (dim_df[c_fecha].dt.date >= d1) & (dim_df[c_fecha].dt.date <= d2)

        # Filtros por empresa/proyecto
        if company_display_name and c_empresa:
            mask &= dim_df[c_empresa].astype(str).str.strip() == company_display_name
        if project_no and c_proyecto:
            mask &= dim_df[c_proyecto].astype(str).str.strip() == project_no

        df = dim_df.loc[mask].copy()

        # capitulo = COALESCE(capitulo_codigo, capitulo)
        df["capitulo"] = None
        if c_cap_cod and c_cap_cod in df.columns:
            df["capitulo"] = df[c_cap_cod].astype(str)
        if c_cap_txt and c_cap_txt in df.columns:
            df["capitulo"] = df["capitulo"].fillna(df[c_cap_txt].astype(str))
        df["capitulo"] = df["capitulo"].fillna("None").str.strip().replace("", "None")

        # columnas limpias
        df["fecha_produccion"] = df[c_fecha].dt.date
        df["project_no"] = df[c_proyecto] if c_proyecto else None
        df["company_name"] = df[c_empresa] if c_empresa else None
        df["certificacion_pendiente"] = pd.to_numeric(df[c_cert_pen], errors="coerce") if c_cert_pen else 0.0
        df["resto_produccion"] = pd.to_numeric(df[c_resto], errors="coerce") if c_resto else 0.0
        df["observaciones"] = df[c_obs] if c_obs else None

        # 2) Join con companies_bc para company_id
        comp_df = pd.read_sql(
            'SELECT "id"::text AS company_id, "name" AS company_name FROM public.companies_bc',
            self.bc_engine,
        )
        df = df.merge(comp_df, how="left", on="company_name")

        keep = [
            "company_id","company_name","project_no","fecha_produccion",
            "capitulo","certificacion_pendiente","resto_produccion","observaciones"
        ]
        for k in keep:
            if k not in df.columns:
                df[k] = None

        return df[keep].reset_index(drop=True)

    def fetch_resource_ledger_entries(
            self,
            company_display_name: Optional[str],
            project_no: Optional[str],
            d1: date,
            d2: date,
    ) -> pd.DataFrame:
        """
        Foto de resource_ledger_entries_bc filtrada por Posting_Date ∈ [d1, d2].
        Devuelve columnas estandarizadas:
          - posting_date, document_no, description, job_no,
            global_dimension_1_code, global_dimension_2_code,
            resource_no, total_cost, company_id
        Aplica filtro por empresa (companies_bc.name) y proyecto (job_no).
        """
        # Mapas de columnas tolerantes a nombres
        cols_df = pd.read_sql(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='resource_ledger_entries_bc'
            """,
            self.bc_engine
        )
        lo = {c.lower(): c for c in cols_df["column_name"].tolist()}

        def pick(*cands: str) -> Optional[str]:
            for c in cands:
                if c.lower() in lo:
                    return lo[c.lower()]
            return None

        col_posting_date = pick("Posting_Date", "posting_date", "Date", "date")
        col_document_no = pick("Document_No", "document_no")
        col_descr = pick("Description", "description")
        col_job_no = pick("Job_No", "job_no", "Job", "job")
        col_dim1 = pick("Global_Dimension_1_Code", "global_dimension_1_code")
        col_dim2 = pick("Global_Dimension_2_Code", "global_dimension_2_code")
        col_res_no = pick("Resource_No", "resource_no")
        col_total_cost = pick("Total_Cost", "total_cost", "Total_Cost_LCY", "total_cost_lcy")
        col_company_id = pick("CompanyId", "company_id")

        # Armamos SQL con joins mínimos para filtrar por empresa
        sql = f"""
        SELECT
            r."{col_posting_date}"::date            AS posting_date,
            r."{col_document_no}"                   AS document_no,
            r."{col_descr}"                         AS description,
            r."{col_job_no}"                        AS job_no,
            r."{col_dim1}"                          AS global_dimension_1_code,
            r."{col_dim2}"                          AS global_dimension_2_code,
            r."{col_res_no}"                        AS resource_no,
            r."{col_total_cost}"                    AS total_cost,
            r."{col_company_id}"::text              AS company_id
        FROM public.resource_ledger_entries_bc r
        LEFT JOIN public.companies_bc c
          ON c."id"::text = r."{col_company_id}"::text
        WHERE r."{col_posting_date}"::date >= %(d1)s
          AND r."{col_posting_date}"::date <= %(d2)s
          AND (%(company)s IS NULL OR c."name" = %(company)s)
          AND (%(project)s IS NULL OR r."{col_job_no}" = %(project)s)
        """
        df = pd.read_sql(
            sql,
            self.bc_engine,
            params={"d1": d1, "d2": d2, "company": company_display_name, "project": project_no},
        )

        # Tipos y columnas finales
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "posting_date", "document_no", "description", "job_no",
                    "global_dimension_1_code", "global_dimension_2_code",
                    "resource_no", "total_cost", "company_id"
                ]
            )

        df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce")
        return df[
            [
                "posting_date", "document_no", "description", "job_no",
                "global_dimension_1_code", "global_dimension_2_code",
                "resource_no", "total_cost", "company_id"
            ]
        ].reset_index(drop=True)




