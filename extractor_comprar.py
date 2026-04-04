"""
extractor_comprar.py
====================
Extractor de contratos y licitaciones de la Jefatura de Gabinete (SAF 25).

Fuente real: infra.datos.gob.ar/catalog/jgm/dataset/4/
  - Convocatorias: distribution/4.{N}/download/convocatorias-{anio}.csv
  - Adjudicaciones: distribution/4.{N}/download/adjudicaciones-{anio}.csv
  - Proveedores:   distribution/2.11/download/proveedores.csv (catalogo modernizacion)

Columnas clave en adjudicaciones:
  Nro SAF | Descripcion SAF | Nro UOC | CUIT | Descripción Proveedor | Monto | Tipo de Procedimiento

El SAF 25 = Jefatura de Gabinete de Ministros.

Uso:
    python extractor_comprar.py
    python extractor_comprar.py --anio 2024
"""

import argparse
import csv
import io
import json
import os
import re
from datetime import datetime

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JefaturaMonitor/1.0)"}
TIMEOUT = 90
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")

# SAF 25 = Jefatura de Gabinete
JGM_SAF = {"25"}
JGM_TERMINOS = ["jefatura de gabinete", "jgm", "innovacion publica", "modernizacion"]

# URLs reales del catalogo jgm en infra.datos.gob.ar
# El numero de distribution cambia por anio — mapeamos los conocidos
URL_CONVOCATORIAS = {
    2025: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.37/download/convocatorias-2025.csv",
    2024: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.35/download/convocatorias-2024.csv",
    2023: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.33/download/convocatorias-2023.csv",
    2022: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.31/download/convocatorias-2022.csv",
    2021: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.28/download/convocatorias-2021.csv",
    2020: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.24/download/convocatorias-2020.csv",
}

URL_ADJUDICACIONES = {
    2025: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.38/download/adjudicaciones-2025.csv",
    2024: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.36/download/adjudicaciones-2024.csv",
    2023: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.34/download/adjudicaciones-2023.csv",
    2022: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.32/download/adjudicaciones-2022.csv",
    2021: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.29/download/adjudicaciones-2021.csv",
    2020: "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution/4.20/download/adjudicaciones-2020.csv",
}

URL_PROVEEDORES = "https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/distribution/2.11/download/proveedores.csv"


def _es_jgm(row):
    saf = str(row.get("Nro SAF") or row.get("nro_saf") or row.get("unidad_operativa") or "").strip()
    desc = (row.get("Descripcion SAF") or row.get("descripcion_saf") or row.get("organismo") or "").lower()
    if saf in JGM_SAF:
        return True
    return any(t in desc for t in JGM_TERMINOS)


def _float(v):
    try:
        return float(str(v).replace(".", "").replace(",", ".").strip() or "0")
    except ValueError:
        return 0.0


def fetch_csv(url):
    try:
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if res.status_code == 404:
            print("[WARN] 404: " + url[:80])
            return []
        res.raise_for_status()
        # El CSV usa ; como separador
        content = res.content.decode("utf-8", errors="replace")
        sep = ";" if content.count(";") > content.count(",") else ","
        reader = csv.DictReader(io.StringIO(content), delimiter=sep)
        return list(reader)
    except Exception as e:
        print("[WARN] {}: {}".format(url[:80], e))
        return []


def fetch_convocatorias(anio):
    url = URL_CONVOCATORIAS.get(anio)
    if not url:
        print("[WARN] COMPRAR: sin URL de convocatorias para {}".format(anio))
        return []
    rows = fetch_csv(url)
    resultado = []
    for row in rows:
        if not _es_jgm(row):
            continue
        resultado.append({
            "fuente": "COMPRAR",
            "tipo": "convocatoria",
            "anio": anio,
            "numero_proceso": row.get("Número Procedimiento") or row.get("numero_proceso") or "",
            "modalidad": row.get("Tipo de Procedimiento") or row.get("modalidad") or "",
            "descripcion": (row.get("Descripcion") or row.get("descripcion") or "")[:300],
            "organismo": row.get("Descripcion SAF") or row.get("descripcion_saf") or "JGM",
            "unidad": row.get("Descripcion UOC") or row.get("descripcion_uoc") or "",
            "monto_estimado": _float(row.get("Monto") or row.get("monto") or "0"),
            "fecha_publicacion": row.get("Fecha de Adjudicación") or row.get("fecha") or "",
            "estado": "convocatoria",
            "proveedor_cuit": "",
            "proveedor_nombre": "",
        })
    print("[COMPRAR] Convocatorias {}: {} de JGM".format(anio, len(resultado)))
    return resultado


def fetch_adjudicaciones(anio):
    url = URL_ADJUDICACIONES.get(anio)
    if not url:
        print("[WARN] COMPRAR: sin URL de adjudicaciones para {}".format(anio))
        return []
    rows = fetch_csv(url)
    resultado = []
    for row in rows:
        if not _es_jgm(row):
            continue
        resultado.append({
            "fuente": "COMPRAR",
            "tipo": "adjudicacion",
            "anio": anio,
            "numero_proceso": row.get("Número Procedimiento") or row.get("numero_proceso") or "",
            "modalidad": row.get("Tipo de Procedimiento") or row.get("modalidad") or "",
            "descripcion": (row.get("Descripcion") or row.get("descripcion") or "")[:300],
            "organismo": row.get("Descripcion SAF") or row.get("descripcion_saf") or "JGM",
            "unidad": row.get("Descripcion UOC") or row.get("descripcion_uoc") or "",
            "monto_estimado": _float(row.get("Monto") or row.get("monto") or "0"),
            "fecha_publicacion": row.get("Fecha de Adjudicación") or row.get("fecha_adjudicacion") or "",
            "estado": "adjudicada",
            "proveedor_cuit": row.get("CUIT") or row.get("cuit") or "",
            "proveedor_nombre": row.get("Descripción Proveedor") or row.get("razon_social") or "",
        })
    print("[COMPRAR] Adjudicaciones {}: {} de JGM".format(anio, len(resultado)))
    return resultado


def fetch_proveedores():
    rows = fetch_csv(URL_PROVEEDORES)
    mapa = {}
    for row in rows:
        cuit = str(row.get("cuit___nit") or row.get("cuit") or "").strip()
        if cuit:
            mapa[cuit] = {
                "cuit": cuit,
                "razon_social": row.get("razon_social") or "",
                "tipo": row.get("tipo_de_personeria") or "",
                "localidad": row.get("localidad") or "",
                "rubros": row.get("rubros") or "",
            }
    print("[COMPRAR] Proveedores SIPRO: {} registros".format(len(mapa)))
    return mapa


def run(anio=None):
    if anio is None:
        anio = datetime.now().year

    contratos = []
    # Traer 3 años para tener contexto histórico
    for a in [anio, anio - 1, anio - 2]:
        contratos.extend(fetch_convocatorias(a))
        contratos.extend(fetch_adjudicaciones(a))

    proveedores = fetch_proveedores()

    # Enriquecer con datos del proveedor
    for c in contratos:
        cuit = c.get("proveedor_cuit", "")
        if cuit and cuit in proveedores:
            c["proveedor_datos"] = proveedores[cuit]

    print("[COMPRAR] Total: {} contratos/licitaciones JGM".format(len(contratos)))
    return {
        "contratos": contratos,
        "proveedores": proveedores,
        "meta": {
            "anio": anio,
            "total_contratos": len(contratos),
            "total_proveedores": len(proveedores),
            "ultima_actualizacion": datetime.now().isoformat()
        }
    }


def save(data, path=None):
    os.makedirs(DATA_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(DATA_DIR, "comprar_raw.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("[COMPRAR] Guardado en " + path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--anio", type=int)
    args = parser.parse_args()
    data = run(anio=args.anio)
    save(data)
