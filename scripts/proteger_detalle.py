#!/usr/bin/env python3
"""
proteger_detalle.py — recupera razón social / CUIT / monto borrados de contratos_detalle.csv
===========================================================================================

Problema (ver docs/REFORMAS_P0.md):
  scraper_detalle_contratos.py --csv vuelve a scrapear las filas sin `moneda`
  (todas las anteriores al lote 3) y, si COMPR.AR no devuelve el proceso,
  reemplaza la fila entera por una vacía. La corrida del 2026-10-14 borró así
  30 proveedores que ya estaban bien (EJES SA, MSLI Latam, CHAER SRL…) y JGM
  quedó con 0 contratos con monto en el portal.

Qué hace este script (no toca el scraper):
  Para cada proceso que HOY no tiene ni razón social ni monto, busca en el
  historial de git del CSV la versión más reciente que sí los tenía y restaura
  ese grupo de campos completo (monto, razón social, CUIT, moneda, fecha de
  perfeccionamiento) desde un mismo commit, para no mezclar datos de fuentes
  distintas. Nunca pisa un valor no vacío.

  Deja la lista de lo restaurado en contratos_detalle_recuperados.csv
  (proceso, campo, commit de origen) para auditoría.

Uso:
  python scripts/proteger_detalle.py                 # aplica (últimos 120 commits)
  python scripts/proteger_detalle.py --dry-run       # solo informa
  python scripts/proteger_detalle.py --commits 400
"""

import argparse
import csv
import io
import os
import subprocess
import sys

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_DEFAULT = "contratos_detalle.csv"
REPORTE_DEFAULT = "contratos_detalle_recuperados.csv"
GRUPO = ["monto_adjudicado", "proveedor_razon", "proveedor_cuit", "moneda", "fecha_perfeccionamiento"]


def _vacio(v) -> bool:
    return (v is None) or (str(v).strip() in ("", "0", "0.0", "0.00", "nan", "None"))


def fila_sin_datos(fila: dict) -> bool:
    return _vacio(fila.get("proveedor_razon")) and _vacio(fila.get("monto_adjudicado"))


def fila_con_datos(fila: dict) -> bool:
    return not _vacio(fila.get("proveedor_razon")) or not _vacio(fila.get("monto_adjudicado"))


def recuperar(filas: list[dict], snapshots: list[tuple[str, list[dict]]], columnas: list[str]):
    """Restaura en `filas` (in place) los grupos de campos perdidos.

    snapshots: [(commit, filas_de_esa_version)], del más nuevo al más viejo.
    Devuelve [(numero_proceso, campo, commit, valor)] de lo restaurado."""
    campos = [c for c in GRUPO if c in columnas]
    indices = []
    for sha, filas_snap in snapshots:
        indices.append((sha, {(f.get("numero_proceso") or "").strip(): f for f in filas_snap}))

    restaurado = []
    for fila in filas:
        nup = (fila.get("numero_proceso") or "").strip()
        if not nup or not fila_sin_datos(fila):
            continue
        for sha, idx in indices:
            vieja = idx.get(nup)
            if vieja and fila_con_datos(vieja):
                for c in campos:
                    if _vacio(fila.get(c)) and not _vacio(vieja.get(c)):
                        fila[c] = vieja[c]
                        restaurado.append((nup, c, sha[:10], vieja[c]))
                break
    return restaurado


def _git(*args) -> str:
    return subprocess.run(["git", *args], cwd=RAIZ, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", check=True).stdout


def snapshots_git(ruta_rel: str, n: int) -> list[tuple[str, list[dict]]]:
    try:
        shas = _git("log", f"-n{n}", "--format=%H", "--", ruta_rel).split()
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[WARN] sin historial git: {e}")
        return []
    out = []
    for sha in shas:
        try:
            texto = _git("show", f"{sha}:{ruta_rel}")
        except subprocess.CalledProcessError:
            continue
        out.append((sha, list(csv.DictReader(io.StringIO(texto.lstrip("﻿"))))))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--csv", default=CSV_DEFAULT)
    ap.add_argument("--reporte", default=REPORTE_DEFAULT)
    ap.add_argument("--commits", type=int, default=120)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    ruta = os.path.join(RAIZ, a.csv)
    with open(ruta, encoding="utf-8-sig", newline="") as f:
        lector = csv.DictReader(f)
        columnas = list(lector.fieldnames or [])
        filas = list(lector)

    antes = sum(1 for f in filas if not _vacio(f.get("proveedor_razon")))
    restaurado = recuperar(filas, snapshots_git(a.csv.replace("\\", "/"), a.commits), columnas)
    procesos = sorted({r[0] for r in restaurado})
    despues = sum(1 for f in filas if not _vacio(f.get("proveedor_razon")))

    print(f"Filas: {len(filas)} | con razón social antes: {antes} → después: {despues} "
          f"| procesos restaurados: {len(procesos)}")
    for nup in procesos[:15]:
        razon = next((r[3] for r in restaurado if r[0] == nup and r[1] == "proveedor_razon"), "")
        print(f"  {nup}: {razon}")

    resumen = os.getenv("GITHUB_STEP_SUMMARY")
    if resumen:
        with open(resumen, "a", encoding="utf-8") as f:
            f.write(f"## Protección de detalle\n\nProcesos restaurados desde el historial: "
                    f"**{len(procesos)}** (razón social {antes} → {despues}).\n")

    if a.dry_run or not restaurado:
        return 0

    with open(ruta, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columnas, extrasaction="ignore")
        w.writeheader()
        w.writerows(filas)
    with open(os.path.join(RAIZ, a.reporte), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["numero_proceso", "campo", "commit_origen", "valor"])
        w.writerows(restaurado)
    return 0


if __name__ == "__main__":
    sys.exit(main())
