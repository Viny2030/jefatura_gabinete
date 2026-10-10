#!/usr/bin/env python3
"""
corregir_personal.py
====================
Recalcula campos derivados de src/frontend/data/personal_{jgm,sgp,presidencia}.json
sin volver a descargar la nómina (sirve aunque Mapa del Estado esté bloqueado):

  - fecha_ingreso   → fecha real de la norma de designación (URL del BORA).
                      Antes quedaba el 1/1 del año del decreto.
  - jgm_al_ingreso  → gestión de JGM en esa fecha (incluye Santilli).
  - vacante         → True si el cargo no tiene titular informado.
  - sueldo_bruto_estimado_ars → None para cargos vacantes y para
                      Presidente / Vicepresidente (no hay estimación por escala).

Es idempotente: se puede correr todos los días.

Uso:
  python scripts/corregir_personal.py
"""

import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gestiones import gestion_para_norma  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(BASE, "src", "frontend", "data")
RAMAS = ["jgm", "sgp", "presidencia"]
SIN_ESTIMACION = {"presidente", "vicepresidente"}


def corregir(registro):
    r = dict(registro)
    vacante = not (r.get("apellido") or r.get("nombre"))
    r["vacante"] = vacante

    fecha, gestion = gestion_para_norma(r.get("norma_designacion"))
    r["fecha_ingreso"] = fecha
    r["jgm_al_ingreso"] = gestion

    jer = str(r.get("jerarquia") or "").strip().lower()
    if vacante or jer in SIN_ESTIMACION:
        r["sueldo_bruto_estimado_ars"] = None
    return r


def main():
    for rama in RAMAS:
        ruta = os.path.join(DATA, f"personal_{rama}.json")
        if not os.path.exists(ruta):
            print(f"[{rama}] no existe {ruta}, se omite")
            continue
        with open(ruta, encoding="utf-8") as f:
            datos = json.load(f)
        antes = Counter(r.get("jgm_al_ingreso") for r in datos)
        nuevos = [corregir(r) for r in datos]
        despues = Counter(r["jgm_al_ingreso"] for r in nuevos)
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(nuevos, f, ensure_ascii=False, indent=2)
        vac = sum(r["vacante"] for r in nuevos)
        con_fecha = sum(1 for r in nuevos if r["fecha_ingreso"])
        print(f"[{rama}] {len(nuevos)} registros · {vac} vacantes · {con_fecha} con fecha de designación")
        print(f"         gestión antes:   {dict(antes)}")
        print(f"         gestión después: {dict(despues)}")


if __name__ == "__main__":
    main()
