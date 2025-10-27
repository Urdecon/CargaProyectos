
# sanpshot/scripts/fill_missing_projects.py
from __future__ import annotations

import argparse
import logging
import os

from application.gapfill.gapfill import GapFillService

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Rellenar proyectos faltantes en snapshot.resumen_seguimiento para un mes dado")
    p.add_argument("--year", type=int, required=True, help="Año (YYYY)")
    p.add_argument("--month", type=int, required=True, help="Mes (1-12)")
    p.add_argument("--company", type=str, default=None, help="Nombre visible de empresa (companies_bc.name). Si no se indica, todas.")
    p.add_argument("--dry-run", action="store_true", help="Solo detectar, no ejecutar snapshots")
    return p.parse_args()

def main() -> None:
    args = parse_args()
    svc = GapFillService()
    result = svc.fill_missing(args.year, args.month, company=args.company, dry_run=args.dry_run)

    missing = result.get("missing")
    run = result.get("run", 0)
    if missing is not None:
        try:
            # Muestra un resumen corto en consola
            print("\n=== PROYECTOS FALTANTES ===")
            if len(missing) == 0:
                print("(ninguno)")
            else:
                for _, row in missing.iterrows():
                    print(f"- {row['company_name']} · {row['project_no']}")
        except Exception:
            pass

    print(f"\nHecho. Detectados {0 if missing is None else len(missing)} faltantes. Ejecutados {run} snapshots.")

if __name__ == "__main__":
    main()
