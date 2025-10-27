
# GapFill: Relleno de proyectos faltantes por mes

Este add-on detecta qué **proyectos** (clave: `company_name` + `project_no`) faltan en
`snapshot.resumen_seguimiento` para un **mes concreto** (usando `period_date = último día del mes`)
y ejecuta tu pipeline de *snapshot* para cada uno de ellos.

## Instalación (copiar dentro del repo existente)

Copia las carpetas/archivos de este paquete en la raíz del proyecto (junto a `sanpshot/`).
Estructura nueva:
```
sanpshot/
  application/
    gapfill/
      __init__.py
      gapfill.py
  scripts/
    fill_missing_projects.py
  README_GAPFILL.md
```

## Uso

Requiere las mismas variables de entorno (`.env`) y conexiones que tu `main.py` original.

**Ejemplos:**

```bash
# Detectar (sin ejecutar) los proyectos que faltan en octubre 2025
python -m sanpshot.scripts.fill_missing_projects --year 2025 --month 10 --dry-run

# Ejecutar el relleno para toda la organización
python -m sanpshot.scripts.fill_missing_projects --year 2025 --month 10

# Solo para una empresa concreta (debe coincidir con companies_bc.name)
python -m sanpshot.scripts.fill_missing_projects --year 2025 --month 10 --company "Construcciones Urdecon S.A."
```

## Cómo funciona

1. **Esperados**: busca proyectos con actividad en `job_ledger_entries_bc` hasta el último día del mes.
2. **Existentes**: obtiene pares `(company_name, project_no)` ya presentes en `snapshot.resumen_seguimiento`
   para `period_date = último día del mes`.
3. **Diferencia**: los que faltan se procesan con tu `ResumenSnapshotPipeline` (igual que `main.py`).

La **clave única** práctica aquí es el par `(company_name, project_no)` para un `period_date` dado, aunque
la tabla final tenga una clave más granular `(company_name, project_no, period_date, grupo, capitulo, nivel)`.

## Notas

- Si la BD `PG_DATABASE` no existe, se crea (igual que en tu `main.py`).
- El add-on solo **llama** a tu pipeline; no cambia ninguna tabla/DDL existentes.
- Puedes integrarlo en tu scheduler (Task Scheduler/cron) mensualmente tras cerrar el mes.
