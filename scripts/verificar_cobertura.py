#!/usr/bin/env python3
"""
verificar_cobertura.py — alarma cuando el portal pierde razón social / montos
=============================================================================

JGM pasó de 9 contratos con monto a 0 sin que nada fallara: el pipeline
terminaba "OK" y el agente IA respondía "no encontré coincidencias". Este
script mide cuántos contratos tienen razón social y monto por área y sale con
código 1 (job en rojo → mail de GitHub) cuando:

  - un área tiene 0 contratos con monto, o
  - la cantidad con monto / con razón social cae más de --umbral (default 30 %)
    respecto de la versión anterior del archivo en git (--contra, default HEAD~1).

Uso:
  python scripts/verificar_cobertura.py
  python scripts/verificar_cobertura.py --contra origin/main~5 --umbral 0.2
  python scripts/verificar_cobertura.py --solo-informe      # nunca falla
"""

import argparse
import csv
import io
import json
import os
import subprocess
import sys

RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = "src/frontend/data"
ARCHIVOS = {
    "jgm": f"{DATA}/contratos_jgm.json",
    "sgp": f"{DATA}/contratos_sgp.json",
    "presidencia": f"{DATA}/contratos_presidencia.json",
    "jgm_ampliada": f"{DATA}/contratos_jgm_ampliada.json",
}
DETALLE = "contratos_detalle.csv"


def _monto(c) -> float:
    try:
        v = float(c.get("monto_adjudicado") or 0)
        return v if v == v else 0.0
    except (TypeError, ValueError):
        return 0.0


def tiene_razon(c: dict) -> bool:
    prov = str(c.get("proveedor") or "").strip()
    obj = str(c.get("objeto") or "").strip()
    if not prov:
        return False
    if str(c.get("cuit") or "").strip():
        return True
    return not (obj and obj.startswith(prov[:40]))


def medir_contratos(data) -> dict:
    data = data if isinstance(data, list) else []
    return {"contratos": len(data),
            "con_monto": sum(1 for c in data if _monto(c)),
            "con_razon": sum(1 for c in data if tiene_razon(c))}


def medir_detalle(texto: str) -> dict:
    filas = list(csv.DictReader(io.StringIO(texto.lstrip("﻿"))))
    return {"contratos": len(filas),
            "con_monto": sum(1 for f in filas if str(f.get("monto_adjudicado") or "").strip()),
            "con_razon": sum(1 for f in filas if str(f.get("proveedor_razon") or "").strip())}


def _git_show(rev: str, ruta: str):
    try:
        return subprocess.run(["git", "show", f"{rev}:{ruta}"], cwd=RAIZ, capture_output=True,
                              text=True, encoding="utf-8", errors="replace", check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _leer(ruta: str):
    p = os.path.join(RAIZ, ruta)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8-sig") as f:
        return f.read()


def evaluar(actual: dict, previo: dict | None, umbral: float, exigir_monto: bool) -> list[str]:
    """Devuelve la lista de problemas (vacía = OK) para una fuente."""
    problemas = []
    if exigir_monto and actual["contratos"] and actual["con_monto"] == 0:
        problemas.append("0 contratos con monto")
    if previo:
        for k in ("con_monto", "con_razon"):
            if previo[k] and actual[k] < previo[k] * (1 - umbral):
                problemas.append(f"{k} cayó {previo[k]} → {actual[k]}")
    return problemas


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contra", default="HEAD~1")
    ap.add_argument("--umbral", type=float, default=0.30)
    ap.add_argument("--solo-informe", action="store_true")
    a = ap.parse_args(argv)

    filas, fallas = [], []
    fuentes = [(k, v, medir_contratos, True) for k, v in ARCHIVOS.items()]
    fuentes.append(("detalle_csv", DETALLE, medir_detalle, False))
    for nombre, ruta, medir, es_json in fuentes:
        txt = _leer(ruta)
        if txt is None:
            continue
        actual = medir(json.loads(txt)) if es_json else medir(txt)
        prev_txt = _git_show(a.contra, ruta)
        previo = None
        if prev_txt:
            try:
                previo = medir(json.loads(prev_txt)) if es_json else medir(prev_txt)
            except ValueError:
                previo = None
        # La ampliada todavía no tiene montos para la mayoría de los SAF: no se exige.
        problemas = evaluar(actual, previo, a.umbral, exigir_monto=es_json and nombre != "jgm_ampliada")
        filas.append((nombre, actual, previo, problemas))
        fallas += [f"{nombre}: {p}" for p in problemas]

    lineas = ["| Fuente | Contratos | Con razón social | Con monto | Antes (monto) | Estado |",
              "|---|---|---|---|---|---|"]
    for nombre, act, prev, probs in filas:
        lineas.append(f"| {nombre} | {act['contratos']} | {act['con_razon']} | {act['con_monto']} | "
                      f"{prev['con_monto'] if prev else '—'} | {'⚠ ' + '; '.join(probs) if probs else 'OK'} |")
    informe = "\n".join(lineas)
    print(informe)
    resumen = os.getenv("GITHUB_STEP_SUMMARY")
    if resumen:
        with open(resumen, "a", encoding="utf-8") as f:
            f.write("## Cobertura de datos de contratos\n\n" + informe + "\n")

    if fallas:
        print("\nREGRESIÓN DE COBERTURA:\n  " + "\n  ".join(fallas))
        return 0 if a.solo_informe else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
