"""
generar_presupuesto_jgm.py
===========================
Genera src/frontend/data/presupuesto_jgm.json a partir del crédito
presupuestario oficial 2026 (Presupuesto Abierto / MECON), filtrado a la
Jefatura de Gabinete de Ministros (jurisdicción 25).

Fuente: https://dgsiaf-repo.mecon.gob.ar/repository/pa/datasets/2026/credito-anual-2026.zip
(Dirección General de Sistemas de Información Financiera, Ministerio de
Economía — dataset público, se regenera diariamente con la ejecución
acumulada del ejercicio en curso). Misma fuente que ya usa el proyecto
hermano "Ajuste" (Monitor de Ajuste Presupuestario).

Unidad del CSV: MILLONES de ARS.

Uso:
    python scripts/generar_presupuesto_jgm.py
    python scripts/generar_presupuesto_jgm.py --output-dir src/frontend/data
"""

import argparse
import csv
import io
import json
import logging
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

URL_ANUAL = "https://dgsiaf-repo.mecon.gob.ar/repository/pa/datasets/2026/credito-anual-2026.zip"
UNIDAD = "millones_ars"
JURISDICCION_JGM = "25"
JURISDICCION_DESC_JGM = "Jefatura de Gabinete de Ministros"

COLS_CREDITO = [
    "credito_presupuestado",
    "credito_vigente",
    "credito_comprometido",
    "credito_devengado",
    "credito_pagado",
]

TOP_PROGRAMAS = 15


def descargar_zip(url: str) -> bytes:
    log.info("Descargando %s ...", url)
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (MonitorEjecutivoJGM/1.0; transparencia publica)"})
    with urlopen(req, timeout=90) as r:
        data = r.read()
    log.info("  -> %.1f MB descargados", len(data) / 1e6)
    return data


def leer_csv_de_zip(data: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        nombre = zf.namelist()[0]
        log.info("  -> leyendo %s", nombre)
        with zf.open(nombre) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            rows = [r for r in reader if r.get("jurisdiccion_id") == JURISDICCION_JGM]
    log.info("  -> %d filas de JGM (jurisdiccion_id=%s)", len(rows), JURISDICCION_JGM)
    return rows


def parse_monto(s: str) -> float:
    if not s:
        return 0.0
    return float(s.replace(",", "."))


def construir_resumen(rows: list[dict]) -> dict:
    if not rows:
        raise RuntimeError("No se encontraron filas para jurisdiccion_id=25 (JGM) en el dataset.")

    totales = defaultdict(float)
    for r in rows:
        for col in COLS_CREDITO:
            totales[col] += parse_monto(r.get(col, ""))

    pct_ejecucion = (
        round(totales["credito_devengado"] / totales["credito_vigente"] * 100, 1)
        if totales["credito_vigente"] else 0.0
    )
    pct_pagado = (
        round(totales["credito_pagado"] / totales["credito_vigente"] * 100, 1)
        if totales["credito_vigente"] else 0.0
    )

    por_inciso = defaultdict(lambda: defaultdict(float))
    for r in rows:
        key = r.get("inciso_desc", "Sin clasificar")
        for col in COLS_CREDITO:
            por_inciso[key][col] += parse_monto(r.get(col, ""))
    lista_inciso = [
        {"inciso": k, **{c: round(v[c], 1) for c in COLS_CREDITO}}
        for k, v in por_inciso.items()
    ]
    lista_inciso.sort(key=lambda x: -x["credito_vigente"])

    por_programa = defaultdict(lambda: defaultdict(float))
    meta_programa = {}
    for r in rows:
        key = (r.get("programa_id", ""), r.get("programa_desc", "Sin clasificar"))
        meta_programa[key] = r.get("servicio_desc", "")
        for col in COLS_CREDITO:
            por_programa[key][col] += parse_monto(r.get(col, ""))
    lista_programa = [
        {
            "programa_id": pid,
            "programa": pdesc,
            "servicio": meta_programa[(pid, pdesc)],
            **{c: round(v[c], 1) for c in COLS_CREDITO},
        }
        for (pid, pdesc), v in por_programa.items()
    ]
    lista_programa.sort(key=lambda x: -x["credito_vigente"])
    lista_programa = lista_programa[:TOP_PROGRAMAS]

    fecha_actualizacion = next(
        (r.get("ultima_actualizacion_fecha") for r in rows if r.get("ultima_actualizacion_fecha")),
        None,
    )

    return {
        "jurisdiccion_id": JURISDICCION_JGM,
        "jurisdiccion": JURISDICCION_DESC_JGM,
        "ejercicio": 2026,
        "unidad": UNIDAD,
        "totales": {c: round(totales[c], 1) for c in COLS_CREDITO},
        "pct_ejecucion_devengado": pct_ejecucion,
        "pct_ejecucion_pagado": pct_pagado,
        "por_inciso": lista_inciso,
        "top_programas": lista_programa,
        "fuente": "Presupuesto Abierto — Dirección General de Sistemas de Información Financiera (MECON)",
        "fuente_url": "https://www.presupuestoabierto.gob.ar",
        "dataset_url": URL_ANUAL,
        "fecha_actualizacion_fuente": fecha_actualizacion,
        "generado": datetime.now().isoformat(timespec="seconds"),
        "nota": (
            "Créditos presupuestarios y ejecución acumulada del ejercicio 2026 en millones de "
            "pesos corrientes, tal como los publica el Ministerio de Economía. La ejecución "
            "(% devengado / % pagado) es acumulada desde el inicio del ejercicio, no del mes en curso."
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Genera presupuesto_jgm.json desde el dataset oficial 2026")
    parser.add_argument("--output-dir", type=Path, default=Path("src/frontend/data"))
    parser.add_argument("--zip-local", type=Path, default=None, help="Usar un ZIP ya descargado en vez de bajarlo")
    args = parser.parse_args()

    try:
        data = args.zip_local.read_bytes() if args.zip_local else descargar_zip(URL_ANUAL)
    except URLError as e:
        log.error("No se pudo descargar el dataset: %s", e)
        raise SystemExit(1)

    rows = leer_csv_de_zip(data)
    resumen = construir_resumen(rows)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / "presupuesto_jgm.json"
    out_path.write_text(json.dumps(resumen, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Guardado: %s", out_path)
    log.info(
        "JGM 2026 — Vigente: %.1f M ARS | Devengado: %.1f%% | Pagado: %.1f%%",
        resumen["totales"]["credito_vigente"],
        resumen["pct_ejecucion_devengado"],
        resumen["pct_ejecucion_pagado"],
    )


if __name__ == "__main__":
    main()
