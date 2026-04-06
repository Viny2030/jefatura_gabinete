"""
scraper_comprar.py
Descarga adjudicaciones y convocatorias de COMPR.AR desde datos.gob.ar
Fuente oficial: https://datos.gob.ar/dataset/jgm-sistema-contrataciones-electronicas
"""

import os
import io
import logging
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [COMPRAR] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# URLs oficiales de datos.gob.ar - CSVs descargables sin autenticación
FUENTES = {
    "adjudicaciones_2024": (
        "https://infra.datos.gob.ar/catalog/modernizacion/dataset/4/distribution/"
        "4.16/download/adjudicaciones-2024.csv"
    ),
    "adjudicaciones_2023": (
        "https://infra.datos.gob.ar/catalog/modernizacion/dataset/4/distribution/"
        "4.14/download/adjudicaciones-2023.csv"
    ),
    "convocatorias_2024": (
        "https://infra.datos.gob.ar/catalog/modernizacion/dataset/4/distribution/"
        "4.15/download/convocatorias-2024.csv"
    ),
    "sipro_proveedores": (
        "https://infra.datos.gob.ar/catalog/modernizacion/dataset/4/distribution/"
        "4.5/download/sipro.csv"
    ),
}

HEADERS = {
    "User-Agent": "PortalAnticorrupcion/1.0 (transparencia publica; contacto: github.com/Viny2030)"
}

COLUMNAS_ADJUDICACIONES = {
    "ejercicio": "ejercicio",
    "nro_proceso": "nro_proceso",
    "organismo_contratante": "organismo",
    "tipo_proceso": "tipo_proceso",
    "objeto_contratacion": "objeto",
    "razon_social_adjudicado": "proveedor",
    "cuit_adjudicado": "cuit_proveedor",
    "monto_adjudicado": "monto_adjudicado",
    "moneda": "moneda",
    "fecha_adjudicacion": "fecha_adjudicacion",
}


def descargar_csv(nombre: str, url: str) -> pd.DataFrame | None:
    log.info("Descargando %s ...", nombre)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(
            io.StringIO(resp.text),
            encoding="utf-8",
            low_memory=False,
            dtype=str,
        )
        log.info("  → %d filas, %d columnas", len(df), len(df.columns))
        return df
    except requests.HTTPError as e:
        log.error("HTTP %s para %s: %s", e.response.status_code, nombre, url)
        return None
    except Exception as e:
        log.error("Error descargando %s: %s", nombre, e)
        return None


def normalizar_adjudicaciones(df: pd.DataFrame) -> pd.DataFrame:
    # Mapear solo columnas que existan en el CSV recibido
    columnas_presentes = {k: v for k, v in COLUMNAS_ADJUDICACIONES.items() if k in df.columns}
    df = df.rename(columns=columnas_presentes)

    columnas_finales = list(columnas_presentes.values())
    df = df[[c for c in columnas_finales if c in df.columns]].copy()

    if "monto_adjudicado" in df.columns:
        df["monto_adjudicado"] = (
            df["monto_adjudicado"]
            .str.replace(",", ".", regex=False)
            .str.replace("[^0-9.]", "", regex=True)
        )
        df["monto_adjudicado"] = pd.to_numeric(df["monto_adjudicado"], errors="coerce")

    if "fecha_adjudicacion" in df.columns:
        df["fecha_adjudicacion"] = pd.to_datetime(
            df["fecha_adjudicacion"], dayfirst=True, errors="coerce"
        )

    df["fuente"] = "COMPR.AR"
    df["fecha_ingesta"] = datetime.now()
    return df.drop_duplicates()


def cargar_en_db(df: pd.DataFrame, tabla: str, engine) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {tabla} WHERE fuente = 'COMPR.AR'"))
        df.to_sql(tabla, engine, if_exists="append", index=False, chunksize=500)
        log.info("  → %d filas cargadas en '%s'", len(df), tabla)
    except Exception as e:
        log.error("Error cargando en DB tabla %s: %s", tabla, e)


def guardar_csv_local(df: pd.DataFrame, nombre: str) -> None:
    """Fallback: guarda CSV localmente si no hay DB configurada."""
    os.makedirs("data", exist_ok=True)
    ruta = f"data/{nombre}_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    log.info("  → guardado en %s", ruta)


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None
    if not engine:
        log.warning("DATABASE_URL no configurada — modo CSV local")

    frames_adj = []

    for nombre, url in FUENTES.items():
        df_raw = descargar_csv(nombre, url)
        if df_raw is None:
            continue

        if "adjudicacion" in nombre:
            df = normalizar_adjudicaciones(df_raw)
            frames_adj.append(df)
        elif nombre == "sipro_proveedores":
            guardar_csv_local(df_raw, "sipro_proveedores")
            if engine:
                df_raw["fecha_ingesta"] = datetime.now()
                cargar_en_db(df_raw, "proveedores_sipro", engine)

    if frames_adj:
        df_total = pd.concat(frames_adj, ignore_index=True)
        log.info("Total adjudicaciones consolidadas: %d", len(df_total))
        guardar_csv_local(df_total, "adjudicaciones")
        if engine:
            cargar_en_db(df_total, "contratos", engine)

    log.info("Scraper COMPR.AR finalizado.")


if __name__ == "__main__":
    main()