"""
scraper_historico.py
====================
Scraper retroactivo del PEN desde 10/12/2023 (gestión Milei).

Fuentes:
  1. COMPR.AR directo — API pública con datos 2021-2026
  2. CONTRAT.AR OCDS  — obra pública (via apis_oficiales.py)
  3. CKAN datos.gob.ar — adjudicaciones hasta 2020 (referencia)
  4. BORA              — resoluciones y decretos

Uso:
    python scripts/scraper_historico.py
    python scripts/scraper_historico.py --solo-comprar
    python scripts/scraper_historico.py --solo-bora
    python scripts/scraper_historico.py --desde 2024-01-01
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
FECHA_INICIO_MILEI = datetime(2023, 12, 10)
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR  = os.path.join(SCRIPT_DIR, "..", "src", "frontend", "data")
DATA_DIR    = os.path.join(SCRIPT_DIR, "..", "data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/csv, */*",
    "Accept-Language": "es-AR,es;q=0.9",
}
TIMEOUT = 60

# ─────────────────────────────────────────────────────────────────────────────
# URLs REALES DE COMPR.AR (API directa, datos 2021-2026)
# ─────────────────────────────────────────────────────────────────────────────
# Endpoint público sin autenticación — paginado por offset
COMPRAR_API_ADJ   = "https://comprar.gob.ar/Compras.aspx/ObtenerAdjudicaciones"
COMPRAR_API_CONV  = "https://comprar.gob.ar/Compras.aspx/ObtenerProcesos"

# Alternativa: API REST de Argentina Compra (ONC)
ONC_API_BASE      = "https://api.argentinacompra.gov.ar/prod/onc/sitio/api/v1"
ONC_ADJ_ENDPOINT  = f"{ONC_API_BASE}/adjudicaciones"
ONC_CONV_ENDPOINT = f"{ONC_API_BASE}/procesos"

# CKAN datos.gob.ar (hasta 2020 solamente)
CKAN_BASE = "https://datos.gob.ar/api/3/action/datastore_search"
CKAN_RESOURCE_IDS = {
    "adjudicaciones": {
        2020: "jgm_4.12", 2019: "jgm_4.10", 2018: "jgm_4.8",
        2017: "jgm_4.5",  2016: "jgm_4.3",  2015: "jgm_4.1",
    },
    "convocatorias": {
        2020: "jgm_4.11", 2019: "jgm_4.9", 2018: "jgm_4.7",
        2017: "jgm_4.6",  2016: "jgm_4.4", 2015: "jgm_4.2",
    },
}

# CSV directos datos.gob.ar (hasta 2020)
CKAN_CSV_BASE = "https://infra.datos.gob.ar/catalog/jgm/dataset/4/distribution"
CKAN_CSV_URLS = {
    "adjudicaciones": {
        2020: f"{CKAN_CSV_BASE}/4.20/download/adjudicaciones-2020.csv",
    },
    "convocatorias": {
        2020: f"{CKAN_CSV_BASE}/4.19/download/convocatorias-2020.csv",
    },
}

# CONTRAT.AR OCDS — obra pública
CONTRAT_CSV = "https://infra.datos.gob.ar/catalog/jgm/dataset/5/distribution/5.1/download/contratos-ocds.csv"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _get(url, params=None, json_body=None, intentos=3):
    for i in range(intentos):
        try:
            if json_body:
                r = requests.post(url, json=json_body, headers=HEADERS, timeout=TIMEOUT, verify=False)
            else:
                r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT, verify=False)
            if r.status_code == 200:
                return r
            print(f"  [HTTP {r.status_code}] {url[:70]}")
            return None
        except Exception as e:
            print(f"  [WARN intento {i+1}] {url[:60]}: {e}")
            if i < intentos - 1:
                time.sleep(3)
    return None


def _parse_monto(v) -> float:
    if not v:
        return 0.0
    try:
        return float(re.sub(r"[^\d.,]", "", str(v)).replace(",", ".") or "0")
    except ValueError:
        return 0.0


def _parse_fecha(v) -> str:
    if not v:
        return ""
    s = str(v).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s[:10]


def _es_milei(fecha_str: str) -> bool:
    if not fecha_str:
        return False
    try:
        return datetime.strptime(fecha_str[:10], "%Y-%m-%d") >= FECHA_INICIO_MILEI
    except ValueError:
        return False


def _guardar_json(data, nombre: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, nombre)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    n = len(data) if isinstance(data, list) else "—"
    print(f"  [JSON] {path} ({n})")


def _guardar_csv(rows: list, nombre: str):
    if not rows:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, nombre)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  [CSV]  {path} ({len(rows)})")


def _fetch_csv_url(url: str) -> list:
    r = _get(url)
    if not r:
        return []
    try:
        content = r.content.decode("utf-8", errors="replace")
        return list(csv.DictReader(io.StringIO(content)))
    except Exception as e:
        print(f"  [WARN CSV] {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# FUENTE 1: API ONC Argentina Compra (datos recientes 2021-2026)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_onc_api(desde: datetime) -> list:
    """
    Intenta la API REST de Argentina Compra (ONC).
    URL: https://api.argentinacompra.gov.ar
    """
    print(f"\n{'━'*60}")
    print(f"🌐 API ONC Argentina Compra (datos recientes)")
    print(f"{'━'*60}")

    todos = []

    # Intentar endpoint de adjudicaciones paginado por fecha
    params = {
        "fechaDesde": desde.strftime("%Y-%m-%d"),
        "fechaHasta": datetime.now().strftime("%Y-%m-%d"),
        "pageSize":   1000,
        "page":       1,
    }

    pagina = 1
    while True:
        params["page"] = pagina
        print(f"  Página {pagina}...", end=" ", flush=True)
        r = _get(ONC_ADJ_ENDPOINT, params=params)

        if not r:
            print("sin respuesta")
            break

        try:
            data = r.json()
        except Exception:
            print("error JSON")
            break

        # Estructura variable según versión de la API
        items = (
            data.get("data", []) or
            data.get("adjudicaciones", []) or
            data.get("results", []) or
            (data if isinstance(data, list) else [])
        )

        if not items:
            print("0 items — fin")
            break

        print(f"{len(items)} items")
        for item in items:
            fecha = _parse_fecha(
                item.get("fechaAdjudicacion") or
                item.get("fecha_adjudicacion") or
                item.get("date") or ""
            )
            if not _es_milei(fecha):
                continue
            todos.append({
                "tipo":               "adjudicacion",
                "numero_proceso":     str(item.get("nroProceso") or item.get("numero_proceso") or ""),
                "modalidad":          item.get("modalidad") or item.get("tipo_procedimiento") or "",
                "organismo":          item.get("organismo") or item.get("nombre_organismo") or "",
                "unidad_operativa":   str(item.get("codigoOrganismo") or ""),
                "descripcion":        str(item.get("descripcion") or item.get("objeto") or "")[:300],
                "proveedor_nombre":   item.get("proveedor") or item.get("razon_social") or "",
                "proveedor_cuit":     str(item.get("cuit") or item.get("cuit_proveedor") or ""),
                "monto_adjudicado":   _parse_monto(item.get("monto") or item.get("monto_adjudicado") or ""),
                "fecha_adjudicacion": fecha,
                "estado":             "adjudicada",
                "fuente":             "ONC API",
            })

        # Si hay menos items que el page size, es la última página
        if len(items) < params["pageSize"]:
            break
        pagina += 1
        time.sleep(0.5)

    print(f"\n  → ONC API: {len(todos)} contratos")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# FUENTE 2: CKAN datos.gob.ar (hasta 2020 — para contexto)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_ckan(desde: datetime) -> list:
    """
    Descarga adjudicaciones de CKAN datos.gob.ar.
    Solo tiene datos hasta 2020, útil como fallback y contexto histórico.
    """
    print(f"\n{'━'*60}")
    print(f"📁 CKAN datos.gob.ar (hasta 2020)")
    print(f"{'━'*60}")

    todos = []

    # Intentar primero los CSV directos (más confiables que CKAN API)
    for tipo, urls_por_anio in CKAN_CSV_URLS.items():
        for anio, url in urls_por_anio.items():
            if anio < desde.year:
                continue
            print(f"  → {tipo} {anio} CSV...", end=" ", flush=True)
            rows = _fetch_csv_url(url)
            print(f"{len(rows)} filas")

            for row in rows:
                # Las columnas del CSV de 2020 son distintas
                fecha = _parse_fecha(
                    row.get("Fecha de Adjudicación") or
                    row.get("fecha_adjudicacion") or
                    row.get("fecha_publicacion") or ""
                )
                if not _es_milei(fecha):
                    continue
                todos.append({
                    "tipo":               tipo,
                    "numero_proceso":     row.get("Número Procedimiento") or row.get("numero_proceso", ""),
                    "modalidad":          row.get("Tipo de Procedimiento") or row.get("modalidad", ""),
                    "organismo":          row.get("Descripcion SAF") or row.get("Descripcion UOC") or "",
                    "unidad_operativa":   row.get("Nro SAF") or row.get("Nro UOC") or "",
                    "descripcion":        "",
                    "proveedor_nombre":   row.get("Descripción Proveedor") or "",
                    "proveedor_cuit":     row.get("CUIT") or "",
                    "monto_adjudicado":   _parse_monto(row.get("Monto") or ""),
                    "fecha_adjudicacion": fecha,
                    "estado":             "adjudicada",
                    "fuente":             f"CKAN/{anio}",
                })
            time.sleep(0.5)

    print(f"\n  → CKAN: {len(todos)} contratos período Milei")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# FUENTE 3: CONTRAT.AR OCDS — obra pública
# ─────────────────────────────────────────────────────────────────────────────
def scrape_contrat_ocds(desde: datetime) -> list:
    """Descarga contratos de obra pública en estándar OCDS."""
    print(f"\n{'━'*60}")
    print(f"🏗️  CONTRAT.AR OCDS — obra pública")
    print(f"{'━'*60}")

    rows = _fetch_csv_url(CONTRAT_CSV)
    print(f"  → {len(rows)} filas totales")

    todos = []
    for row in rows:
        fecha = _parse_fecha(
            row.get("tender/datePublished") or
            row.get("date") or
            row.get("fecha_publicacion") or ""
        )
        if not _es_milei(fecha):
            continue
        todos.append({
            "tipo":               "obra_publica",
            "numero_proceso":     row.get("ocid") or row.get("id") or "",
            "modalidad":          row.get("tender/procurementMethod") or "",
            "organismo":          row.get("buyer/name") or "",
            "unidad_operativa":   row.get("buyer/identifier/id") or "",
            "descripcion":        str(row.get("tender/title") or "")[:300],
            "proveedor_nombre":   row.get("awards/0/suppliers/0/name") or "",
            "proveedor_cuit":     row.get("awards/0/suppliers/0/identifier/id") or "",
            "monto_adjudicado":   _parse_monto(row.get("contracts/0/value/amount") or ""),
            "fecha_adjudicacion": fecha,
            "estado":             row.get("tender/status") or "adjudicada",
            "fuente":             "CONTRAT.AR OCDS",
        })

    print(f"  → {len(todos)} obras período Milei")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# FUENTE 4: BORA
# ─────────────────────────────────────────────────────────────────────────────
BORA_SEARCH = "https://www.boletinoficial.gob.ar/busquedaAvanzada/realizarBusqueda"
BORA_HEADERS = {
    **HEADERS,
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.boletinoficial.gob.ar/busquedaAvanzada/index",
}
TERMINOS_PEN = [
    "Jefatura de Gabinete",
    "Decisión Administrativa",
    "designación",
    "contratación directa",
    "Presidencia de la Nación",
    "Secretaría General",
]


def _bora_buscar(termino, fd, fh) -> list:
    payload = {"params": {
        "denominacion": termino, "seccion": "1",
        "fechaDesde": fd, "fechaHasta": fh,
        "cantidadPorPagina": 100, "paginaActual": 1,
    }}
    try:
        r = requests.post(BORA_SEARCH, json=payload, headers=BORA_HEADERS, timeout=30, verify=False)
        r.raise_for_status()
        return r.json().get("data", {}).get("items", [])
    except Exception as e:
        print(f"  [WARN] BORA '{termino}': {e}")
        return []


def scrape_bora(desde: datetime) -> list:
    print(f"\n{'━'*60}")
    print(f"📰 BORA — desde {desde.strftime('%d/%m/%Y')}")
    print(f"{'━'*60}")

    todos = []
    vistos = set()
    cursor = desde
    hoy = datetime.now()

    while cursor <= hoy:
        fin = min(cursor + timedelta(days=90), hoy)
        fd = cursor.strftime("%d/%m/%Y")
        fh = fin.strftime("%d/%m/%Y")
        print(f"\n  {fd} → {fh}")

        for termino in TERMINOS_PEN:
            items = _bora_buscar(termino, fd, fh)
            print(f"    '{termino}': {len(items)}")
            for item in items:
                key = str(item.get("nroNorma", "")) + str(item.get("fechaPublicacion", ""))
                if key in vistos:
                    continue
                vistos.add(key)
                fp = item.get("fechaPublicacion", "")
                # Convertir dd/mm/yyyy → yyyy-mm-dd
                if "/" in fp:
                    partes = fp.split("/")
                    if len(partes) == 3:
                        fp = f"{partes[2]}-{partes[1]}-{partes[0]}"
                todos.append({
                    "numero_norma":      item.get("nroNorma", ""),
                    "tipo_norma":        item.get("tipoNorma", ""),
                    "organismo":         item.get("dependencia", ""),
                    "fecha_publicacion": fp[:10],
                    "titulo":            item.get("titulo", "")[:500],
                    "url":               "https://www.boletinoficial.gob.ar" + item.get("urlDetalle", ""),
                    "termino_busqueda":  termino,
                    "fuente":            "BORA",
                })
            time.sleep(1)

        cursor = fin + timedelta(days=1)

    print(f"\n  ✅ BORA: {len(todos)} normas únicas")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# CLASIFICAR Y GENERAR JSONs para dashboards
# ─────────────────────────────────────────────────────────────────────────────
def _clasificar(organismo: str) -> str:
    org = organismo.lower()
    if any(t in org for t in ["jefatura de gabinete", "jgm", "innovacion", "innovación"]):
        return "jgm"
    if any(t in org for t in ["secretaria general", "sgp", "secretaría general"]):
        return "sgp"
    if any(t in org for t in ["presidencia", "casa militar", "secretaria legal"]):
        return "presidencia"
    return "otros_pen"


def generar_jsons(contratos: list):
    print(f"\n{'━'*60}")
    print(f"📊 Generando JSONs para dashboards")
    print(f"{'━'*60}")

    grupos = {"jgm": [], "sgp": [], "presidencia": [], "otros_pen": []}
    for c in contratos:
        grupos[_clasificar(c.get("organismo", ""))].append(c)

    for clave, registros in grupos.items():
        _guardar_json(registros, f"contratos_{clave}.json")

    _guardar_json(contratos, "contratos_pen_historico.json")

    total_monto = sum(c.get("monto_adjudicado", 0) for c in contratos)
    resumen = {
        "generado_en":        datetime.now().isoformat(),
        "fecha_desde":        FECHA_INICIO_MILEI.strftime("%Y-%m-%d"),
        "fecha_hasta":        datetime.now().strftime("%Y-%m-%d"),
        "total_contratos":    len(contratos),
        "total_monto_ars":    total_monto,
        "total_monto_b":      round(total_monto / 1e9, 2),
        "organismos_unicos":  len(set(c.get("organismo", "") for c in contratos)),
        "proveedores_unicos": len(set(c.get("proveedor_cuit", "") for c in contratos if c.get("proveedor_cuit"))),
        "por_organismo": {
            k: {"contratos": len(v), "monto_ars": sum(c.get("monto_adjudicado", 0) for c in v)}
            for k, v in grupos.items()
        },
    }
    _guardar_json(resumen, "resumen_pen.json")

    print(f"\n  💰 Total: ${total_monto/1e9:.2f}B ARS")
    print(f"  🏛️  Organismos: {resumen['organismos_unicos']}")
    print(f"  🏢 Proveedores: {resumen['proveedores_unicos']}")
    for k, v in grupos.items():
        if v:
            print(f"  {k}: {len(v)}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(description="Scraper histórico PEN desde 10/12/2023")
    parser.add_argument("--desde", default="2023-12-10")
    parser.add_argument("--solo-comprar", action="store_true")
    parser.add_argument("--solo-bora",    action="store_true")
    args = parser.parse_args()

    desde = datetime.strptime(args.desde, "%Y-%m-%d")

    print("\n" + "═"*60)
    print("🔍 SCRAPER HISTÓRICO PEN — GESTIÓN MILEI")
    print(f"   Desde: {desde.strftime('%d/%m/%Y')}")
    print(f"   Hasta: {datetime.now().strftime('%d/%m/%Y')}")
    print("═"*60)

    contratos = []
    normas_bora = []

    if not args.solo_bora:
        # 1. Intentar API ONC (datos recientes)
        contratos_onc = scrape_onc_api(desde)
        contratos.extend(contratos_onc)

        # 2. CKAN datos.gob.ar (hasta 2020)
        contratos_ckan = scrape_ckan(desde)
        contratos.extend(contratos_ckan)

        # 3. CONTRAT.AR obra pública
        contratos_ocds = scrape_contrat_ocds(desde)
        contratos.extend(contratos_ocds)

        # Deduplicar por numero_proceso
        vistos = set()
        contratos_unicos = []
        for c in contratos:
            key = c.get("numero_proceso", "") + c.get("proveedor_cuit", "")
            if key and key in vistos:
                continue
            vistos.add(key)
            contratos_unicos.append(c)
        contratos = contratos_unicos

        _guardar_csv(contratos, "contratos_pen_historico.csv")

    if not args.solo_comprar:
        normas_bora = scrape_bora(desde)
        _guardar_csv(normas_bora, "bora_historico.csv")
        _guardar_json(normas_bora, "bora_historico.json")

    if contratos:
        generar_jsons(contratos)

    print("\n" + "═"*60)
    print("✅ FINALIZADO")
    print(f"   Contratos: {len(contratos)}")
    print(f"   Normas BORA: {len(normas_bora)}")
    print("═"*60 + "\n")


if __name__ == "__main__":
    main()