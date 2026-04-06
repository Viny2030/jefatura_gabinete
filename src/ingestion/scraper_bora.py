"""
scraper_bora.py
Extrae resoluciones y decisiones administrativas del Boletín Oficial (BORA)
relacionadas con la Jefatura de Gabinete de Ministros.
Fuente: https://www.boletinoficial.gob.ar/
"""

import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BORA] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

BASE_URL = "https://www.boletinoficial.gob.ar"
SEARCH_URL = f"{BASE_URL}/busquedaAvanzada/realizarBusqueda"

HEADERS = {
    "User-Agent": "PortalAnticorrupcion/1.0 (transparencia publica)",
    "Accept": "application/json, text/javascript, */*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{BASE_URL}/busquedaAvanzada/index",
}

TERMINOS_JGM = [
    "Jefatura de Gabinete",
    "Decisión Administrativa",
    "designación",
    "contratación directa",
]

RUBRO_PRIMERA_SECCION = "1"  # Primera sección del BORA = actos normativos


def fecha_ayer() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")


def fecha_hoy() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def buscar_bora(termino: str, fecha_desde: str, fecha_hasta: str) -> list[dict]:
    """Realiza búsqueda en la API interna del BORA."""
    payload = {
        "params": {
            "denominacion": termino,
            "seccion": RUBRO_PRIMERA_SECCION,
            "fechaDesde": fecha_desde,
            "fechaHasta": fecha_hasta,
            "cantidadPorPagina": 100,
            "paginaActual": 1,
        }
    }
    try:
        resp = requests.post(SEARCH_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", {}).get("items", [])
        log.info("  '%s': %d resultados", termino, len(items))
        return items
    except requests.HTTPError as e:
        log.error("HTTP %s buscando '%s': %s", e.response.status_code, termino, e)
        return []
    except Exception as e:
        log.error("Error buscando '%s' en BORA: %s", termino, e)
        return []


def parsear_detalle(url_detalle: str) -> str:
    """Extrae texto de la página de detalle de una norma."""
    try:
        resp = requests.get(f"{BASE_URL}{url_detalle}", headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        contenedor = soup.find("div", class_="contenidoArt") or soup.find("article")
        if contenedor:
            return contenedor.get_text(separator=" ", strip=True)[:2000]
        return ""
    except Exception:
        return ""


def normalizar_items(items: list[dict], termino: str) -> list[dict]:
    registros = []
    for item in items:
        registro = {
            "numero_norma": item.get("nroNorma", ""),
            "tipo_norma": item.get("tipoNorma", ""),
            "organismo": item.get("dependencia", ""),
            "fecha_publicacion": item.get("fechaPublicacion", ""),
            "titulo": item.get("titulo", "")[:500],
            "url": item.get("urlDetalle", ""),
            "termino_busqueda": termino,
            "fuente": "BORA",
            "fecha_ingesta": datetime.now().isoformat(),
        }
        registros.append(registro)
    return registros


def cargar_en_db(registros: list[dict], engine) -> None:
    df = pd.DataFrame(registros)
    try:
        with engine.begin() as conn:
            # Evitar duplicados por número de norma
            for _, row in df.iterrows():
                if row.get("numero_norma"):
                    conn.execute(
                        text("DELETE FROM normas_bora WHERE numero_norma = :n"),
                        {"n": row["numero_norma"]},
                    )
        df.to_sql("normas_bora", engine, if_exists="append", index=False, chunksize=200)
        log.info("  → %d normas cargadas en DB", len(df))
    except Exception as e:
        log.error("Error cargando normas en DB: %s", e)


def guardar_csv_local(registros: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(registros)
    ruta = f"data/bora_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    log.info("  → guardado en %s (%d filas)", ruta, len(df))


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None
    if not engine:
        log.warning("DATABASE_URL no configurada — modo CSV local")

    fecha_desde = fecha_ayer()
    fecha_hasta = fecha_hoy()
    log.info("Período: %s → %s", fecha_desde, fecha_hasta)

    todos_los_registros = []

    for termino in TERMINOS_JGM:
        items = buscar_bora(termino, fecha_desde, fecha_hasta)
        registros = normalizar_items(items, termino)
        todos_los_registros.extend(registros)
        time.sleep(1.5)  # Respetar rate limit del BORA

    # Deduplicar por numero_norma
    df = pd.DataFrame(todos_los_registros)
    if not df.empty and "numero_norma" in df.columns:
        df = df.drop_duplicates(subset=["numero_norma"])
        log.info("Total normas únicas: %d", len(df))

    guardar_csv_local(df.to_dict("records") if not df.empty else [])
    if engine and not df.empty:
        cargar_en_db(df.to_dict("records"), engine)

    log.info("Scraper BORA finalizado.")


if __name__ == "__main__":
    main()