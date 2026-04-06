"""
scraper_nomina.py
Descarga la nómina de personal de la Jefatura de Gabinete de Ministros
desde el Mapa del Estado (mapadelestado.dyte.gob.ar).
Fuente: https://mapadelestado.dyte.gob.ar/back/api/datos.php
"""

import os
import io
import logging
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [NOMINA] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# API del Mapa del Estado - endpoint público CSV
# db=m → ministerios, id=9 → JGM, fi=csv
NOMINA_URL = "https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv&n1=001"

# Fallback: dataset nómina desde datos.gob.ar
NOMINA_FALLBACK_URL = (
    "https://infra.datos.gob.ar/catalog/modernizacion/dataset/6/distribution/"
    "6.1/download/nomina-personal-jgm.csv"
)

HEADERS = {
    "User-Agent": "PortalAnticorrupcion/1.0 (transparencia publica)",
    "Referer": "https://mapadelestado.dyte.gob.ar/",
}

COLUMNAS_NOMINA = {
    "organismo": "organismo",
    "unidad_organizativa": "unidad",
    "nombre": "nombre",
    "apellido": "apellido",
    "dni": "dni",
    "cuil": "cuil",
    "sexo": "sexo",
    "cargo": "cargo",
    "tipo_contratacion": "tipo_contratacion",
    "agrupamiento": "agrupamiento",
    "nivel": "nivel",
    "escalafon": "escalafon",
    "norma": "norma_designacion",
}


def descargar_nomina(url: str, nombre: str) -> pd.DataFrame | None:
    log.info("Descargando %s desde %s ...", nombre, url[:60])
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()

        # Detectar encoding (el endpoint puede devolver latin-1)
        encoding = resp.encoding or "utf-8"
        try:
            df = pd.read_csv(
                io.StringIO(resp.content.decode("utf-8")),
                dtype=str,
                low_memory=False,
            )
        except UnicodeDecodeError:
            df = pd.read_csv(
                io.StringIO(resp.content.decode("latin-1")),
                dtype=str,
                low_memory=False,
            )

        log.info("  → %d filas, %d columnas", len(df), len(df.columns))
        return df
    except requests.HTTPError as e:
        log.warning("HTTP %s para %s", e.response.status_code, nombre)
        return None
    except Exception as e:
        log.error("Error descargando %s: %s", nombre, e)
        return None


def normalizar_nomina(df: pd.DataFrame) -> pd.DataFrame:
    # Normalizar nombres de columnas a minúsculas sin espacios
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace("/", "_", regex=False)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )

    columnas_presentes = {k: v for k, v in COLUMNAS_NOMINA.items() if k in df.columns}
    df = df.rename(columns=columnas_presentes)

    columnas_finales = [v for v in COLUMNAS_NOMINA.values() if v in df.columns]
    df = df[columnas_finales].copy()

    # Limpiar CUIL/DNI: solo dígitos
    for col in ["cuil", "dni"]:
        if col in df.columns:
            df[col] = df[col].str.replace(r"[^0-9]", "", regex=True)

    # Capitalizar nombres
    for col in ["nombre", "apellido"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    df["fuente"] = "MapaDelEstado"
    df["fecha_ingesta"] = datetime.now()
    return df.drop_duplicates(subset=["cuil"] if "cuil" in df.columns else None)


def cargar_en_db(df: pd.DataFrame, engine) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM nomina WHERE fuente = 'MapaDelEstado'"))
        df.to_sql("nomina", engine, if_exists="append", index=False, chunksize=500)
        log.info("  → %d funcionarios cargados en DB", len(df))
    except Exception as e:
        log.error("Error cargando nómina en DB: %s", e)


def guardar_csv_local(df: pd.DataFrame) -> None:
    os.makedirs("data", exist_ok=True)
    ruta = f"data/nomina_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    log.info("  → guardado en %s", ruta)


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None
    if not engine:
        log.warning("DATABASE_URL no configurada — modo CSV local")

    # Intentar fuente primaria, luego fallback
    df = descargar_nomina(NOMINA_URL, "MapaDelEstado-JGM")
    if df is None or df.empty:
        log.warning("Fuente primaria sin datos, probando fallback...")
        df = descargar_nomina(NOMINA_FALLBACK_URL, "datos.gob.ar-JGM")

    if df is None or df.empty:
        log.error("No se pudo obtener la nómina de ninguna fuente.")
        return

    df_norm = normalizar_nomina(df)
    log.info("Nómina normalizada: %d funcionarios", len(df_norm))

    guardar_csv_local(df_norm)
    if engine:
        cargar_en_db(df_norm, engine)

    log.info("Scraper nómina JGM finalizado.")


if __name__ == "__main__":
    main()