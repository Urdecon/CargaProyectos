# application/pipeline/steps/transform_resumen.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable, Set

import pandas as pd

from application.pipeline.steps.extract_resumen import ExtractOutput


@dataclass
class TransformOutput:
    rows: pd.DataFrame
    company_name: Optional[str]
    project_no: Optional[str]
    year: int
    month: int
    last_day: pd.Timestamp


class TransformResumenStep:
    """
    • Mantiene cálculos previos.
    • certificacion_mes_actual y certificacion_mes_anterior (ahora desde job_planning_lines).
    • coste_qwark pendiente por capítulo (baseAmount>0 & ~registrada) ACUMULADO ≤ fecha corte.
    • contratos y planificado desde job_task_lines_subform_bc.
    • Añade métricas de PRODUCCIÓN del MES ANTERIOR:
        - certificacion_pendiente_mes_anterior
        - resto_produccion_mes_anterior
        - produccion_mes_anterior = certificacion_mes_anterior
    • Proporcionales:
        - % viene en tanto por 100 (8 -> 8% -> 0.08; 0.5 -> 0.5% -> 0.005)
        - coste_total_a_origen = % * total(produccion_a_origen)
        - coste_bc_a_origen    = igual que coste_total_a_origen (según petición)
        - coste_bc_mes_anterior = % * total(produccion_mes_anterior)
        - coste_bc_mes_actual   = % * total(certificacion_mes_actual)
        - resto de columnas = 0
    """

    def run(self, ext: ExtractOutput) -> TransformOutput:
        # Copias
        fle = ext.fle.copy()
        qwark = ext.qwark.copy()
        pend = ext.pendientes_mes.copy()
        prod = ext.produccion_mes.copy()
        task = ext.tasklines.copy()
        prop = ext.proporcionales.copy()

        # (opcionales) datasets del mes anterior si los expone el extract
        prod_prev = getattr(ext, "produccion_mes_anterior", pd.DataFrame())

        # Fechas
        def _to_ts(df: pd.DataFrame, cols: Iterable[str]) -> None:
            for c in cols:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")

        last_day_ts = pd.to_datetime(ext.period_date)
        month_start_ts = pd.to_datetime(ext.month_start)
        month_end_ts = pd.to_datetime(ext.month_end)

        _to_ts(fle, ["posting_date"])
        _to_ts(qwark, ["fecha_factura"])
        _to_ts(pend, ["fecha_produccion"])
        _to_ts(prod, ["fecha_produccion"])
        if not prod_prev.empty:
            _to_ts(prod_prev, ["fecha_produccion"])

        key_company = ext.company_name
        key_project = ext.project_no

        # ───────── util: normalizar capítulo ─────────
        def _norm_cap_series(series: pd.Series) -> pd.Series:
            s = series.astype("string")
            s = s.fillna("").str.strip()
            s = s.replace("", "None")
            return s

        # ───────── COSTE BC (sin certificaciones) ─────────
        fle_non_cert = fle[(fle["gppg"] != "CERTIFICACIONES") & (fle["posting_date"] <= last_day_ts)].copy()

        coste_bc_origen_cap = (
            fle_non_cert.groupby(["company_name", "project_no", "job_task_no"], dropna=False)["total_cost_lcy"]
            .sum()
            .reset_index()
            .rename(columns={"total_cost_lcy": "coste_bc_a_origen"})
        )
        coste_bc_origen_cap_s = (
            coste_bc_origen_cap[
                (coste_bc_origen_cap["company_name"] == key_company)
                & (coste_bc_origen_cap["project_no"] == key_project)
            ]
            .set_index("job_task_no")["coste_bc_a_origen"]
        )

        fle_mes_actual = fle[
            (fle["gppg"] != "CERTIFICACIONES")
            & (fle["posting_date"] >= month_start_ts)
            & (fle["posting_date"] <= month_end_ts)
        ].copy()
        coste_bc_mes_actual_cap_s = (
            fle_mes_actual.groupby(["company_name", "project_no", "job_task_no"], dropna=False)["total_cost_lcy"]
            .sum()
            .reset_index()
        )
        coste_bc_mes_actual_cap_s = (
            coste_bc_mes_actual_cap_s[
                (coste_bc_mes_actual_cap_s["company_name"] == key_company)
                & (coste_bc_mes_actual_cap_s["project_no"] == key_project)
            ].set_index("job_task_no")["total_cost_lcy"]
        )

        # ───────── CERTIFICACIONES DESDE JOB_PLANNING_LINES ─────────
        jpl = getattr(ext, "planning_lines", pd.DataFrame()).copy()

        def _pick(df_cols: list, *cands: str) -> Optional[str]:
            lo = {c.lower(): c for c in df_cols}
            for cand in cands:
                if cand.lower() in lo:
                    return lo[cand.lower()]
            return None

        if not jpl.empty:
            date_col = _pick(list(jpl.columns), "Planning_Date", "planning_date")
            gppg_col = _pick(list(jpl.columns), "Gen_Prod_Posting_Group", "gen_prod_posting_group")
            amt_col  = _pick(list(jpl.columns), "Line_Amount", "line_amount")
            proj_col = _pick(list(jpl.columns), "Job_No", "job_no", "project_no")
            cap_col  = _pick(list(jpl.columns), "Job_Task_No", "job_task_no")
            comp_id_col   = _pick(list(jpl.columns), "CompanyId", "company_id")
            comp_name_col = _pick(list(jpl.columns), "Company_Name", "company_name")

            if date_col:
                _to_ts(jpl, [date_col])

            mask = pd.Series(True, index=jpl.index)
            if comp_id_col and ext.company_id is not None:
                mask &= jpl[comp_id_col] == ext.company_id
            elif comp_name_col and key_company:
                mask &= jpl[comp_name_col] == key_company
            if proj_col and key_project:
                mask &= jpl[proj_col] == key_project
            if gppg_col:
                mask &= jpl[gppg_col] == "CERTIFICACIONES"

            jpl_f = jpl.loc[mask].copy()

            # A ORIGEN (YTD del año seleccionado) ≤ última fecha (mes)
            if date_col:
                jpl_ytd = jpl_f[
                    (jpl_f[date_col].dt.year == ext.selected_year) &
                    (jpl_f[date_col] <= last_day_ts)
                ].copy()
            else:
                jpl_ytd = jpl_f.copy()

            # Mes actual
            if date_col:
                jpl_mes = jpl_f[
                    (jpl_f[date_col] >= month_start_ts) &
                    (jpl_f[date_col] <= month_end_ts)
                ].copy()
            else:
                jpl_mes = jpl_f.iloc[0:0].copy()

            if cap_col and amt_col:
                cert_cap = jpl_ytd.groupby(cap_col, dropna=False)[amt_col].sum(min_count=1)
                cert_mes_actual_cap = jpl_mes.groupby(cap_col, dropna=False)[amt_col].sum(min_count=1)
            else:
                cert_cap = pd.Series(dtype=float)
                cert_mes_actual_cap = pd.Series(dtype=float)

            cert_mes_anterior_cap_s = cert_cap.subtract(cert_mes_actual_cap, fill_value=0)

            # Unificar nombre de índice a 'job_task_no'
            if isinstance(cert_cap.index, pd.Index) and cert_cap.index.name != "job_task_no":
                cert_cap.index.name = "job_task_no"
            if isinstance(cert_mes_actual_cap.index, pd.Index) and cert_mes_actual_cap.index.name != "job_task_no":
                cert_mes_actual_cap.index.name = "job_task_no"
            if isinstance(cert_mes_anterior_cap_s.index, pd.Index) and cert_mes_anterior_cap_s.index.name != "job_task_no":
                cert_mes_anterior_cap_s.index.name = "job_task_no"
        else:
            # Fallback vacío si no hay planning_lines
            cert_cap = pd.Series(dtype=float)
            cert_mes_actual_cap = pd.Series(dtype=float)
            cert_mes_anterior_cap_s = pd.Series(dtype=float)

        # Producción actual (viene de extract)
        cert_pend_cap = (
            prod.groupby("job_task_no", dropna=False)["certificacion_pendiente"].sum()
            if not prod.empty and "job_task_no" in prod and "certificacion_pendiente" in prod
            else pd.Series(dtype=float)
        )
        resto_cap = (
            prod.groupby("job_task_no", dropna=False)["resto_produccion"].sum()
            if not prod.empty and "job_task_no" in prod and "resto_produccion" in prod
            else pd.Series(dtype=float)
        )

        # Mes anterior (si llega de extract)
        if not prod_prev.empty:
            prev_cert_pend_cap_s = (
                prod_prev.groupby("job_task_no", dropna=False)["certificacion_pendiente"].sum()
                if "job_task_no" in prod_prev and "certificacion_pendiente" in prod_prev
                else pd.Series(dtype=float)
            )
            prev_resto_cap_s = (
                prod_prev.groupby("job_task_no", dropna=False)["resto_produccion"].sum()
                if "job_task_no" in prod_prev and "resto_produccion" in prod_prev
                else pd.Series(dtype=float)
            )
        else:
            prev_cert_pend_cap_s = pd.Series(dtype=float)
            prev_resto_cap_s = pd.Series(dtype=float)

        # ───────── QWARK (acumulado ≤ fecha corte) ─────────
        if not qwark.empty:
            proj_col_q = "proyecto" if "proyecto" in qwark.columns else "project_no"
            qwark_until_cut = qwark[qwark["fecha_factura"] <= last_day_ts].copy()

            filt_q = qwark_until_cut[
                (qwark_until_cut["base_amount"] > 0)
                & (~qwark_until_cut["registrada"])
                & ((qwark_until_cut[proj_col_q] == key_project) if key_project else True)
                & ((qwark_until_cut["company_name"] == key_company) if key_company else True)
            ].copy()

            # ── CAMBIO CLAVE: mantener facturas sin capítulo como 'None' (literal) ──
            if "job_task_no" not in filt_q.columns:
                filt_q["job_task_no"] = "None"
            else:
                filt_q["job_task_no"] = (
                    filt_q["job_task_no"]
                    .astype("string")
                    .fillna("None")
                    .str.strip()
                    .replace("", "None")
                )

            # Excluir documentos tipo RET (si tienes el paso previo, consérvalo donde lo hagas)

            qwark_cap_s = (
                filt_q.groupby("job_task_no", dropna=False)["base_amount"].sum()
                if not filt_q.empty and "base_amount" in filt_q.columns
                else pd.Series(dtype=float)
            )

        else:
            qwark_cap_s = pd.Series(dtype=float)

        # ───────── CONTRATOS y PLANIFICADO (tasklines) ─────────
        if not task.empty:
            cols_lower = {c.lower(): c for c in task.columns}

            def pick(*cands: str) -> Optional[str]:
                for cc in cands:
                    if cc.lower() in cols_lower:
                        return cols_lower[cc.lower()]
                return None

            end_col = pick("End_Date", "end_date")
            comp_id_col = pick("CompanyId", "company_id")
            comp_name_col = pick("company_name", "Company_Name")
            jobno_col = pick("Job_No", "project_no", "job_no")
            cap_col = pick("Job_Task_No", "job_task_no")
            price_col = pick("Schedule_Total_Price", "schedule_total_price")
            cost_col = pick("Schedule_Total_Cost", "schedule_total_cost")

            if end_col:
                _to_ts(task, [end_col])

            mask = pd.Series(True, index=task.index)
            if end_col:
                mask &= task[end_col].notna() & (task[end_col] <= last_day_ts)

            if comp_id_col and ext.company_id is not None:
                mask &= task[comp_id_col] == ext.company_id
            elif comp_name_col and key_company:
                mask &= task[comp_name_col] == key_company

            if jobno_col and key_project:
                mask &= task[jobno_col] == key_project

            task_filt = task.loc[mask].copy()

            if cap_col and cap_col in task_filt.columns:
                task_filt[cap_col] = _norm_cap_series(task_filt[cap_col])

            if cap_col and price_col and cap_col in task_filt.columns and price_col in task_filt.columns:
                contratos_cap_s = task_filt.groupby(cap_col, dropna=False)[price_col].sum(min_count=1)
            else:
                contratos_cap_s = pd.Series(dtype=float)

            if cap_col and cost_col and cap_col in task_filt.columns and cost_col in task_filt.columns:
                planificado_cap_s = task_filt.groupby(cap_col, dropna=False)[cost_col].sum(min_count=1)
            else:
                planificado_cap_s = pd.Series(dtype=float)
        else:
            contratos_cap_s = pd.Series(dtype=float)
            planificado_cap_s = pd.Series(dtype=float)

        # ───────── Capítulos (unión de fuentes) ─────────
        cap_set: Set[str] = set()

        def _add_caps(df: pd.DataFrame, col: str = "job_task_no"):
            if col in df.columns and not df.empty:
                caps = (
                    df[col]
                    .astype("string")
                    .fillna("None")
                    .str.strip()
                    .replace("", "None")
                )
                cap_set.update(caps.tolist())

        _add_caps(fle, "job_task_no")
        _add_caps(prod, "job_task_no")
        _add_caps(pend, "job_task_no")
        _add_caps(qwark, "job_task_no")
        if not task.empty:
            for c in ("Job_Task_No", "job_task_no"):
                if c in task.columns:
                    _add_caps(task, c)
        if not prod_prev.empty and "job_task_no" in prod_prev.columns:
            _add_caps(prod_prev, "job_task_no")

        capitulos = sorted(c for c in cap_set if isinstance(c, str) and len(c) > 0)

        rows: list[dict] = []

        # ───────── Filas Directos (detalle por capítulo) ─────────
        for cap in capitulos:
            v_coste_bc_origen = float(coste_bc_origen_cap_s.get(cap, 0.0))
            v_coste_bc_mes_actual = float(coste_bc_mes_actual_cap_s.get(cap, 0.0))
            v_coste_bc_mes_anterior = v_coste_bc_origen - v_coste_bc_mes_actual

            v_pendiente = float(
                pend.loc[
                    (_norm_cap_series(pend.get("job_task_no", pd.Series(dtype="string")).astype("string")) == cap),
                    "coste_pendiente",
                ].sum()
            ) if not pend.empty and "job_task_no" in pend else 0.0

            v_qwark = float(qwark_cap_s.get(cap, 0.0)) if isinstance(qwark_cap_s, pd.Series) else 0.0

            v_cert_cap = float(cert_cap.get(cap, 0.0)) if not cert_cap.empty else 0.0
            v_cert_mes_actual = float(cert_mes_actual_cap.get(cap, 0.0)) if not cert_mes_actual_cap.empty else 0.0
            v_cert_mes_anterior = float(cert_mes_anterior_cap_s.get(cap, 0.0)) if not cert_mes_anterior_cap_s.empty else 0.0

            v_cert_pend_cap = float(cert_pend_cap.get(cap, 0.0)) if not cert_pend_cap.empty else 0.0
            v_resto_cap = float(resto_cap.get(cap, 0.0)) if not resto_cap.empty else 0.0
            v_prod_cap = v_cert_cap + v_cert_pend_cap + v_resto_cap

            # Mes anterior: PRODUCCIÓN MES ANTERIOR = CERTIFICACIÓN MES ANTERIOR
            v_prev_cert_pend = float(prev_cert_pend_cap_s.get(cap, 0.0)) if isinstance(prev_cert_pend_cap_s, pd.Series) else 0.0
            v_prev_resto = float(prev_resto_cap_s.get(cap, 0.0)) if isinstance(prev_resto_cap_s, pd.Series) else 0.0
            v_prev_prod_total = v_cert_mes_anterior

            v_contratos = float(contratos_cap_s.get(cap, 0.0)) if isinstance(contratos_cap_s, pd.Series) else 0.0
            v_planificado = float(planificado_cap_s.get(cap, 0.0)) if isinstance(planificado_cap_s, pd.Series) else 0.0

            v_coste_total_origen = v_coste_bc_origen + (v_pendiente or 0.0)

            rows.append(
                {
                    "company_id": ext.company_id,
                    "company_name": ext.company_name,
                    "project_no": ext.project_no,
                    "year": ext.selected_year,
                    "month": ext.month_end.month,
                    "grupo": "Directos",
                    "linea": cap,
                    "nivel": "Detalle",

                    "coste_bc_a_origen": round(v_coste_bc_origen, 2),
                    "coste_bc_mes_actual": round(v_coste_bc_mes_actual, 2),
                    "coste_bc_mes_anterior": round(v_coste_bc_mes_anterior, 2),
                    "coste_qwark": round(v_qwark, 2),
                    "coste_pendiente": round(v_pendiente, 2),
                    "coste_total_a_origen": round(v_coste_total_origen, 2),

                    "certificacion_a_origen": round(v_cert_cap, 2),
                    "certificacion_mes_actual": round(v_cert_mes_actual, 2),
                    "certificacion_mes_anterior": round(v_cert_mes_anterior, 2),

                    "certificacion_pendiente": round(v_cert_pend_cap, 2),
                    "resto_produccion": round(v_resto_cap, 2),
                    "produccion_a_origen": round(v_prod_cap, 2),

                    # mes anterior
                    "certificacion_pendiente_mes_anterior": round(v_prev_cert_pend, 2),
                    "resto_produccion_mes_anterior": round(v_prev_resto, 2),
                    "produccion_mes_anterior": round(v_prev_prod_total, 2),

                    "contratos": round(v_contratos, 2),
                    "planificado": round(v_planificado, 2),

                    # KPIs placeholder
                    "resultado": 0.0,
                    "pct_resultado": 0.0,
                    "pct_avance_venta_contrato": 0.0,
                    "pct_avance_coste_planificado": 0.0,
                }
            )

        # ───────── Proporcionales calculados ─────────

        def _get_pct(df: pd.DataFrame, col: str) -> float:
            """
            Lee un porcentaje en tanto por 100 desde ext.proporcionales.
            Ejemplos: 8  -> 0.08
                      0.5 -> 0.005
                      12.75 -> 0.1275
            """
            if df is None or df.empty or col not in df.columns:
                return 0.0
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if s.empty:
                return 0.0
            raw = float(s.iloc[0])
            pct = raw / 100.0  # llega en tanto por 100
            if pct < 0:
                pct = 0.0
            if pct > 1:
                pct = 1.0
            return pct

        pct_gastos = _get_pct(prop, "gastos_generales")
        pct_postv = _get_pct(prop, "postventa")

        # Totales base (solo Directos)
        df_directos = pd.DataFrame(rows)
        if not df_directos.empty:
            total_prod_origen = float(df_directos.loc[df_directos["grupo"] == "Directos", "produccion_a_origen"].sum())
            total_prod_prev = float(df_directos.loc[df_directos["grupo"] == "Directos", "produccion_mes_anterior"].sum())
            total_cert_mes_actual = float(
                df_directos.loc[df_directos["grupo"] == "Directos", "certificacion_mes_actual"].sum()
            )
        else:
            total_prod_origen = 0.0
            total_prod_prev = 0.0
            total_cert_mes_actual = 0.0

        def _fila_prop_calc(nombre: str, pct: float):
            ctao  = round(pct * total_prod_origen, 2)       # coste_total_a_origen
            cbca  = ctao                                     # coste_bc_a_origen (mismo valor, por petición)
            cbcma = round(pct * total_cert_mes_actual, 2)    # coste_bc_mes_actual
            cbcm1 = round(pct * total_prod_prev, 2)          # coste_bc_mes_anterior

            return {
                "company_id": ext.company_id,
                "company_name": ext.company_name,
                "project_no": ext.project_no,
                "year": ext.selected_year,
                "month": ext.month_end.month,
                "grupo": "Proporcionales",
                "linea": nombre,
                "nivel": "Detalle",

                "coste_bc_a_origen": cbca,
                "coste_bc_mes_actual": cbcma,
                "coste_bc_mes_anterior": cbcm1,
                "coste_qwark": 0.0,
                "coste_pendiente": 0.0,
                "coste_total_a_origen": ctao,

                "certificacion_a_origen": 0.0,
                "certificacion_mes_actual": 0.0,
                "certificacion_mes_anterior": 0.0,
                "certificacion_pendiente": 0.0,
                "resto_produccion": 0.0,
                "produccion_a_origen": 0.0,

                "certificacion_pendiente_mes_anterior": 0.0,
                "resto_produccion_mes_anterior": 0.0,
                "produccion_mes_anterior": 0.0,

                "resultado": 0.0,
                "pct_resultado": 0.0,
                "contratos": 0.0,
                "planificado": 0.0,
                "pct_avance_venta_contrato": 0.0,
                "pct_avance_coste_planificado": 0.0,
            }

        rows.append(_fila_prop_calc("Gastos generales", pct_gastos))
        rows.append(_fila_prop_calc("Postventa", pct_postv))

        df = pd.DataFrame(rows)

        # Tipado numérico
        float_cols = [
            "coste_bc_a_origen",
            "coste_bc_mes_actual",
            "coste_bc_mes_anterior",
            "coste_qwark",
            "coste_pendiente",
            "coste_total_a_origen",
            "produccion_a_origen",
            "resultado",
            "contratos",
            "planificado",
            "certificacion_a_origen",
            "certificacion_mes_actual",
            "certificacion_mes_anterior",
            "certificacion_pendiente",
            "resto_produccion",
            "certificacion_pendiente_mes_anterior",
            "resto_produccion_mes_anterior",
            "produccion_mes_anterior",
            "pct_resultado",
            "pct_avance_venta_contrato",
            "pct_avance_coste_planificado",
        ]
        for c in float_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype("Float64")

        return TransformOutput(
            rows=df,
            company_name=ext.company_name,
            project_no=ext.project_no,
            year=ext.selected_year,
            month=ext.month_end.month,
            last_day=pd.to_datetime(ext.period_date),
        )
