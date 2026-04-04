"""
extractor_comprar.py
====================
Extractor de contratos y licitaciones de la Jefatura de Gabinete de Ministros.

Fuentes:
  1. datos.gob.ar — dataset ONC / COMPRAR (CSV público, sin auth)
     URL: https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/
  2. API OCDS de datos.gob.ar — estándar Open Contracting

Uso:
    python extractor_comprar.py
    python extractor_comprar.py --anio 2025
    python extractor_comprar.py --organismo "jefatura"
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
TIMEOUT = 60
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

# Codigos de unidad operativa de Jefatura de Gabinete
# UO 25 = Jefatura de Gabinete de Ministros
JGM_CODIGOS_UO = {"25", "025"}
JGM_TERMINOS = ["jefatura de gabinete", "jgm", "secretaria de innovacion publica"]

# URLs de datasets de contrataciones en datos.gob.ar
DATASETS = {
    "convocatorias": "https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/distribution/2.1/download/convocatorias-{anio}.csv",
    "adjudicaciones": "https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/distribution/2.4/download/adjudicaciones-{anio}.csv",
    "contratos": "https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/distribution/2.3/download/contratos-{anio}.csv",
    "proveedores": "https://infra.datos.gob.ar/catalog/modernizacion/dataset/2/distribution/2.6/download/proveedores.csv",
}


def _es_jgm(row: dict) -> bool:
    """Detecta si una fila de contratación pertenece a JGM."""
    uo = str(row.get("unidad_operativa", "") or row.get("codigo_organismo", "")).strip()
    organismo = (row.get("organismo", "") or row.get("nombre_organismo", "") or "").lower()
    if uo in JGM_CODIGOS_UO:
        return True
    return any(term in organismo for term in JGM_TERMINOS)


def fetch_csv(url: str) -> list:
    """Descarga y parsea un CSV de datos.gob.ar."""
    try:
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if res.status_code == 404:
            return []
        res.raise_for_status()
        content = res.content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)
    except Exception as e:
        print(f"[WARN] {url[:80]}: {e}")
        return []


def fetch_convocatorias(anio: int) -> list:
    """Trae licitaciones convocadas del año dado."""
    url = DATASETS["convocatorias"].format(anio=anio)
    rows = fetch_csv(url)
    resultado = []
    for row in rows:
        if not _es_jgm(row):
            continue
        resultado.append({
            "fuente": "COMPRAR",
            "tipo": "convocatoria",
            "anio": anio,
            "numero_proceso": row.get("numero_proceso_de_compra") or row.get("nro_proceso") or "",
            "modalidad": row.get("modalidad_de_contratacion") or row.get("modalidad") or "",
            "descripcion": (row.get("descripcion") or row.get("objeto") or "")[:300],
            "organismo": row.get("nombre_organismo") or row.get("organismo") or "JGM",
            "monto_estimado": _parse_monto(row.get("monto_estimado") or row.get("monto") or ""),
            "fecha_publicacion": row.get("fecha_publicacion") or row.get("fecha") or "",
            "estado": row.get("estado") or "convocatoria",
            "proveedor_cuit": "",
            "proveedor_nombre": "",
        })
    print(f"[COMPRAR] Convocatorias {anio}: {len(resultado)} de JGM")
    return resultado


def fetch_adjudicaciones(anio: int) -> list:
    """Trae licitaciones adjudicadas — acá están el CUIT y proveedor ganador."""
    url = DATASETS["adjudicaciones"].format(anio=anio)
    rows = fetch_csv(url)
    resultado = []
    for row in rows:
        if not _es_jgm(row):
            continue
        resultado.append({
            "fuente": "COMPRAR",
            "tipo": "adjudicacion",
            "anio": anio,
            "numero_proceso": row.get("numero_proceso_de_compra") or row.get("nro_proceso") or "",
            "modalidad": row.get("modalidad_de_contratacion") or row.get("modalidad") or "",
            "descripcion": (row.get("descripcion") or row.get("objeto") or "")[:300],
            "organismo": row.get("nombre_organismo") or row.get("organismo") or "JGM",
            "monto_estimado": _parse_monto(row.get("monto_total") or row.get("monto_adjudicado") or ""),
            "fecha_publicacion": row.get("fecha_publicacion") or row.get("fecha_adjudicacion") or "",
            "estado": "adjudicada",
            "proveedor_cuit": row.get("cuit_proveedor") or row.get("cuit") or "",
            "proveedor_nombre": row.get("razon_social") or row.get("proveedor") or "",
        })
    print(f"[COMPRAR] Adjudicaciones {anio}: {len(resultado)} de JGM")
    return resultado


def fetch_proveedores() -> dict:
    """
    Descarga el padrón de proveedores (SIPRO) para enriquecer con datos del proveedor.
    Devuelve dict CUIT -> datos.
    """
    url = DATASETS["proveedores"]
    rows = fetch_csv(url)
    mapa = {}
    for row in rows:
        cuit = str(row.get("cuit") or row.get("nro_cuit") or "").strip()
        if cuit:
            mapa[cuit] = {
                "cuit": cuit,
                "razon_social": row.get("razon_social") or row.get("nombre") or "",
                "tipo_empresa": row.get("tipo_persona") or row.get("tipo") or "",
                "estado_sipro": row.get("estado") or "",
            }
    print(f"[COMPRAR] Proveedores SIPRO: {len(mapa)} registros")
    return mapa


def _parse_monto(valor: str) -> float:
    """Convierte string de monto a float."""
    if not valor:
        return 0.0
    try:
        return float(re.sub(r"[^\d.,]", "", str(valor)).replace(",", ".") or "0")
    except ValueError:
        return 0.0


def run(anio: int = None, organismo_filtro: str = None) -> dict:
    if anio is None:
        anio = datetime.now().year

    # Traer anio actual + anterior (contratos pueden cruzar años)
    contratos = []
    for a in [anio, anio - 1]:
        contratos.extend(fetch_convocatorias(a))
        contratos.extend(fetch_adjudicaciones(a))

    # Filtro adicional por organismo si se especifica
    if organismo_filtro:
        filtro = organismo_filtro.lower()
        contratos = [c for c in contratos if filtro in c.get("organismo", "").lower()]

    proveedores = fetch_proveedores()

    # Enriquecer contratos con datos del proveedor
    for c in contratos:
        cuit = c.get("proveedor_cuit", "")
        if cuit and cuit in proveedores:
            c["proveedor_datos"] = proveedores[cuit]

    print(f"[COMPRAR] Total: {len(contratos)} contratos/licitaciones JGM")
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


def save(data: dict, path: str = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(OUTPUT_DIR, "comprar_raw.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[COMPRAR] Guardado en {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--anio", type=int, help="Año (default: actual)")
    parser.add_argument("--organismo", help="Filtro adicional por organismo")
    args = parser.parse_args()

    data = run(anio=args.anio, organismo_filtro=args.organismo)
    save(data)
