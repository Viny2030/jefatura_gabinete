"""
scraper_nomina.py
=================
Descarga y procesa la nómina del personal civil de la APN.

Fuentes (en orden de prioridad):
  1. MapaDelEstado (mapadelestado.dyte.gob.ar) — API JSON, activa
  2. BIEP ZIP (biep.modernizacion.gob.ar)       — fallback, actualmente con 500
  3. datos.gob.ar CSV                           — fallback histórico

Escala salarial SINEP embebida para imputar sueldos estimados.

Salidas:
  - data/nomina_apn_raw.csv       → nómina completa sin modificar
  - data/nomina_apn_procesada.csv → nómina con sueldo estimado, tipo planta, etc.

Uso:
  python scripts/scraper_nomina.py
"""

import io
import os
import warnings
import zipfile
import requests
import urllib3
import pandas as pd
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuración ──────────────────────────────────────────────────────────────
BIEP_URL     = "https://biep.modernizacion.gob.ar/apps/directorio/archivos/datos.zip"
MAPA_URL     = "https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv"
DATOSGOB_URL = (
    "https://infra.datos.gob.ar/catalog/modernizacion/dataset/6/distribution/"
    "6.1/download/nomina-personal-jgm.csv"
)

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RAW_CSV  = os.path.join(DATA_DIR, "nomina_apn_raw.csv")
PROC_CSV = os.path.join(DATA_DIR, "nomina_apn_procesada.csv")
TIMEOUT  = 60

os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "PortalTransparencia/1.0 (datos publicos APN)",
    "Referer":    "https://mapadelestado.dyte.gob.ar/",
}

# ── Escala salarial SINEP embebida (feb 2025) ─────────────────────────────────
ESCALA_SINEP = {
    ("General Profesional",    "Superior",   "A"): 3_850_000,
    ("General Profesional",    "Superior",   "B"): 3_450_000,
    ("General Profesional",    "Superior",   "C"): 3_100_000,
    ("General Profesional",    "Intermedio", "A"): 2_750_000,
    ("General Profesional",    "Intermedio", "B"): 2_450_000,
    ("General Profesional",    "Intermedio", "C"): 2_200_000,
    ("General Profesional",    "Inicial",    "A"): 1_950_000,
    ("General Profesional",    "Inicial",    "B"): 1_750_000,
    ("General Profesional",    "Inicial",    "C"): 1_580_000,
    ("General Administrativo", "Superior",   "A"): 2_400_000,
    ("General Administrativo", "Superior",   "B"): 2_150_000,
    ("General Administrativo", "Superior",   "C"): 1_950_000,
    ("General Administrativo", "Intermedio", "A"): 1_750_000,
    ("General Administrativo", "Intermedio", "B"): 1_580_000,
    ("General Administrativo", "Intermedio", "C"): 1_430_000,
    ("General Administrativo", "Inicial",    "A"): 1_300_000,
    ("General Administrativo", "Inicial",    "B"): 1_180_000,
    ("General Administrativo", "Inicial",    "C"): 1_080_000,
    ("General Técnico",        "Superior",   "A"): 2_800_000,
    ("General Técnico",        "Superior",   "B"): 2_500_000,
    ("General Técnico",        "Superior",   "C"): 2_250_000,
    ("General Técnico",        "Intermedio", "A"): 2_000_000,
    ("General Técnico",        "Intermedio", "B"): 1_800_000,
    ("General Técnico",        "Intermedio", "C"): 1_630_000,
    ("General Técnico",        "Inicial",    "A"): 1_480_000,
    ("General Técnico",        "Inicial",    "B"): 1_350_000,
    ("General Técnico",        "Inicial",    "C"): 1_230_000,
    ("Autoridad Superior",     "—",          "—"): 8_500_000,
    ("Contratado Art. 9",      "—",          "—"): 1_500_000,
}
SUELDO_DEFAULT = 1_200_000


# ── Fuente 1: MapaDelEstado (PRIMARIA) ────────────────────────────────────────
def descargar_mapadelestado() -> pd.DataFrame | None:
    """
    API JSON del Mapa del Estado. Devuelve todos los organismos
    cuando no se especifica id= (o se itera por páginas).
    """
    print(f"[MAPA] Descargando desde {MAPA_URL} ...")
    try:
        r = requests.get(MAPA_URL, headers=HEADERS, timeout=TIMEOUT, verify=False)
        r.raise_for_status()

        # La API puede devolver JSON o CSV según el parámetro fi=
        ct = r.headers.get("Content-Type", "")
        if "json" in ct:
            data = r.json()
            # El JSON puede ser lista directa o wrapper {"data": [...]}
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and "data" in data:
                df = pd.DataFrame(data["data"])
            else:
                df = pd.json_normalize(data)
        else:
            # CSV directo
            df = pd.read_csv(io.StringIO(r.text), low_memory=False)

        print(f"[MAPA] {len(df):,} filas, {len(df.columns)} columnas")
        print(f"[MAPA] Columnas: {list(df.columns)}")
        return df

    except Exception as e:
        print(f"[MAPA] ⚠ Falló fuente primaria: {e}")
        return None


# ── Fuente 2: BIEP ZIP (FALLBACK) ─────────────────────────────────────────────
def descargar_biep() -> pd.DataFrame | None:
    """Descarga el ZIP del BIEP y retorna el DataFrame principal."""
    print(f"[BIEP] Intentando fallback desde {BIEP_URL} ...")
    try:
        r = requests.get(BIEP_URL, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        print(f"[BIEP] Descargado: {len(r.content)/1024:.0f} KB")

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            archivos = z.namelist()
            print(f"[BIEP] Archivos en ZIP: {archivos}")
            csv_files = [f for f in archivos if f.lower().endswith(".csv")]
            if not csv_files:
                raise FileNotFoundError(f"No hay CSV en el ZIP: {archivos}")

            csv_target = max(csv_files, key=lambda f: z.getinfo(f).file_size)
            print(f"[BIEP] Leyendo: {csv_target}")

            with z.open(csv_target) as f:
                for enc in ["utf-8", "latin-1", "cp1252"]:
                    try:
                        df = pd.read_csv(f, encoding=enc, low_memory=False)
                        print(f"[BIEP] Encoding: {enc} — {len(df):,} filas, {len(df.columns)} columnas")
                        print(f"[BIEP] Columnas: {list(df.columns)}")
                        return df
                    except UnicodeDecodeError:
                        f.seek(0)
                        continue

    except Exception as e:
        print(f"[BIEP] ⚠ Falló fallback BIEP: {e}")
        return None


# ── Fuente 3: datos.gob.ar CSV (FALLBACK 2) ───────────────────────────────────
def descargar_datosgob() -> pd.DataFrame | None:
    print(f"[DATOSGOB] Intentando fallback histórico desde datos.gob.ar ...")
    try:
        r = requests.get(DATOSGOB_URL, headers=HEADERS, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        print(f"[DATOSGOB] {len(df):,} filas, {len(df.columns)} columnas")
        return df
    except Exception as e:
        print(f"[DATOSGOB] ⚠ Falló fallback datos.gob.ar: {e}")
        return None


# ── Orquestador de fuentes ────────────────────────────────────────────────────
def descargar_nomina() -> pd.DataFrame:
    """Intenta las fuentes en orden hasta obtener datos."""
    for fn in [descargar_mapadelestado, descargar_biep, descargar_datosgob]:
        df = fn()
        if df is not None and not df.empty:
            return df
    raise RuntimeError("❌ Todas las fuentes fallaron. Verificar conectividad.")


# ── Normalización de columnas ─────────────────────────────────────────────────
def normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mapea columnas del MapaDelEstado / BIEP a nombres estándar del proyecto.
    Columnas confirmadas del endpoint MapaDelEstado (abril 2026):
      jurisdiccion, subjurisdiccion, unidad_de_nivel_politico, unidad,
      reporta_a, nombre_corto, tipo_administracion, unidad_rango, unidad_clase,
      norma_competencias_objetivos, car_orden, cargo, car_nivel,
      car_rango_jerarquia, car_categoria, car_extraescalafonario, car_escalafon,
      car_suplemento, autoridad_tratamiento, autoridad_nombre, autoridad_apellido,
      autoridad_dni, autoridad_cuil, autoridad_sexo, autoridad_norma_designacion, web
    """
    print(f"\n[NORM] Columnas originales: {list(df.columns)}\n")

    # Limpiar nombres: strip de tabs/espacios, lowercase
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    MAPPER = {
        # ── Organismo ─────────────────────────────────────────────────────────
        "jurisdiccion":                "organismo",
        "subjurisdiccion":             "sub_organismo",
        "unidad_de_nivel_politico":    "unidad_politica",
        "unidad":                      "unidad",
        "reporta_a":                   "reporta_a",
        "tipo_administracion":         "tipo_administracion",
        # variantes BIEP
        "jurisdiccion_desc":           "organismo",
        "organismo_desc":              "organismo",
        "reparticion":                 "organismo",

        # ── Autoridad (MapaDelEstado) ─────────────────────────────────────────
        "autoridad_nombre":            "nombre",
        "autoridad_apellido":          "apellido",
        "autoridad_cuil":              "cuil",
        "autoridad_dni":               "dni",
        "autoridad_sexo":              "genero",
        "autoridad_norma_designacion": "norma_designacion",
        "autoridad_tratamiento":       "tratamiento",

        # ── Cargo / escalafón ─────────────────────────────────────────────────
        "cargo":                       "cargo",
        "car_rango_jerarquia":         "jerarquia",
        "car_nivel":                   "nivel",
        "car_categoria":               "car_categoria",
        "car_escalafon":               "escalafon",
        "car_extraescalafonario":      "extraescalafonario",
        "car_suplemento":              "suplemento",
        "car_orden":                   "car_orden",
        # variantes BIEP
        "funcion":                     "cargo",
        "denominacion_cargo":          "cargo",
        "agrupamiento":                "agrupamiento",
        "tramo":                       "tramo",
        "grado":                       "nivel",

        # ── Nombre completo (BIEP) ────────────────────────────────────────────
        "apellido_nombre":             "nombre_completo",
        "nombre_apellido":             "nombre_completo",
        "nombre":                      "nombre_completo",

        # ── Tipo de planta (BIEP) ─────────────────────────────────────────────
        "situacion_revista":           "tipo_planta",
        "tipo_contratacion":           "tipo_planta",
        "modalidad":                   "tipo_planta",
        "situacion":                   "tipo_planta",

        # ── Fecha ingreso (BIEP) ──────────────────────────────────────────────
        "fecha_ingreso":               "fecha_ingreso",
        "fecha_inicio":                "fecha_ingreso",
        "ingreso":                     "fecha_ingreso",
        "anio_ingreso":                "anio_ingreso",
        "fecha_alta":                  "fecha_ingreso",

        # ── Localización ──────────────────────────────────────────────────────
        "provincia":                   "provincia",
        "provincia_desc":              "provincia",
        "lugar_trabajo":               "provincia",

        # ── Web ───────────────────────────────────────────────────────────────
        "web":                         "web",
    }

    rename = {col: MAPPER[col] for col in df.columns if col in MAPPER}
    df = df.rename(columns=rename)
    print(f"[NORM] Columnas mapeadas: {list(rename.keys())} → {list(rename.values())}")
    return df


# ── Clasificación tipo de planta ──────────────────────────────────────────────
def clasificar_tipo_planta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clasifica usando 'escalafon' (MapaDelEstado) o 'tipo_planta' (BIEP).
    """
    # Fuente MapaDelEstado: usar escalafon
    if "escalafon" in df.columns:
        def clasificar_escalafon(v):
            if pd.isna(v):
                return "Desconocido"
            v = str(v).upper()
            if "AUTORIDAD SUPERIOR" in v:
                return "Autoridad Superior"
            if "SINEP" in v:
                return "Planta Permanente (SINEP)"
            if "FUERA DE NIVEL" in v:
                return "Funcionario Fuera de Nivel"
            if "SIN DATO" in v:
                return "Sin Dato"
            return v.title()
        df["tipo_planta_std"] = df["escalafon"].apply(clasificar_escalafon)
        return df

    # Fuente BIEP: usar tipo_planta
    if "tipo_planta" not in df.columns:
        df["tipo_planta"] = "Desconocido"
    def clasificar(v):
        if pd.isna(v):
            return "Desconocido"
        v = str(v).upper()
        if any(x in v for x in ["PERMANENTE", "PLANTA PERM"]):
            return "Planta Permanente"
        if any(x in v for x in ["CONTRAT", "ART. 9", "ART 9", "LOCACION", "LOCACIÓN"]):
            return "Contratado"
        if any(x in v for x in ["AUTORIDAD", "MINISTRO", "SECRETAR", "SUBSECRET"]):
            return "Autoridad Superior"
        if any(x in v for x in ["TRANSITORIA", "NO PERM", "TERMINO"]):
            return "Planta No Permanente"
        return "Otro"
    df["tipo_planta_std"] = df["tipo_planta"].apply(clasificar)
    return df


def imputar_sueldo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Imputa sueldo estimado.
    MapaDelEstado: usa escalafon + jerarquia.
    BIEP: usa agrupamiento + tramo + nivel.
    """
    SUELDO_JERARQUIA = {
        "jefe de gabinete":                    15_000_000,
        "ministro":                            12_000_000,
        "secretario":                           8_000_000,
        "subsecretario":                        6_500_000,
        "dn-dg":                                5_000_000,  # director nacional/general
        "director - primer nivel operativo":    4_500_000,
        "director - segundo nivel operativo":   4_000_000,
        "director":                             4_000_000,
        "coordinador":                          3_200_000,
        "coordinador de área":                  3_000_000,
        "jefe de departamento":                 2_500_000,
        "jefe de división":                     2_200_000,
        "jefe de grupo de trabajo":             2_000_000,
        "jefe de agencia":                      2_000_000,
        "jefe de servicio":                     1_900_000,
        "s-d":                                  1_800_000,
        "ad honorem":                               0,
    }

    def buscar_sueldo(row):
        # MapaDelEstado: jerarquia + escalafon
        jerarquia = str(row.get("jerarquia", "")).strip().lower()
        escalafon = str(row.get("escalafon", "")).strip().upper()

        if jerarquia and jerarquia != "nan":
            for key, val in SUELDO_JERARQUIA.items():
                if key in jerarquia:
                    return val
            if "AUTORIDAD SUPERIOR" in escalafon:
                return 8_500_000

        # BIEP: agrupamiento + tramo + nivel
        agrup = str(row.get("agrupamiento", "")).strip()
        tramo = str(row.get("tramo", "")).strip()
        nivel = str(row.get("nivel", "")).strip().upper()
        key = (agrup, tramo, nivel)
        if key in ESCALA_SINEP:
            return ESCALA_SINEP[key]

        tipo = str(row.get("tipo_planta_std", ""))
        if "Contratado" in tipo:
            return ESCALA_SINEP.get(("Contratado Art. 9", "—", "—"), SUELDO_DEFAULT)
        if "Autoridad" in tipo:
            return ESCALA_SINEP.get(("Autoridad Superior", "—", "—"), SUELDO_DEFAULT)

        return SUELDO_DEFAULT

    df["sueldo_bruto_estimado_ars"] = df.apply(buscar_sueldo, axis=1)
    return df


def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas derivadas. Para MapaDelEstado parsea fecha del decreto."""

    import re

    # ── Parsear fecha desde norma_designacion (MapaDelEstado) ────────────────
    # Formato en URL: /20251104? → 2025-11-04
    if "norma_designacion" in df.columns and "fecha_ingreso" not in df.columns:
        def extraer_fecha_decreto(v):
            if pd.isna(v):
                return pd.NaT
            # Buscar patrón /YYYYMMDD? en la URL del BORA
            m = re.search(r'/(\d{8})[?\s]', str(v))
            if m:
                try:
                    return pd.to_datetime(m.group(1), format="%Y%m%d")
                except Exception:
                    pass
            # Buscar año-mes en número de decreto: "Decreto 784/2025"
            m2 = re.search(r'(\d{3,4})/(\d{4})', str(v))
            if m2:
                anio = int(m2.group(2))
                if 2000 <= anio <= 2030:
                    return pd.Timestamp(year=anio, month=1, day=1)
            return pd.NaT

        df["fecha_ingreso"] = df["norma_designacion"].apply(extraer_fecha_decreto)
        ok = df["fecha_ingreso"].notna().sum()
        print(f"[ENRICH] Fechas extraídas de decreto: {ok}/{len(df)}")

    # ── Antigüedad ────────────────────────────────────────────────────────────
    if "fecha_ingreso" in df.columns:
        df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], errors="coerce", dayfirst=True)
        hoy = pd.Timestamp.now()
        df["antiguedad_anios"] = ((hoy - df["fecha_ingreso"]).dt.days / 365.25).round(1)
        df["anio_ingreso"] = df["fecha_ingreso"].dt.year
    elif "anio_ingreso" in df.columns:
        df["anio_ingreso"] = pd.to_numeric(df["anio_ingreso"], errors="coerce")
        df["antiguedad_anios"] = datetime.now().year - df["anio_ingreso"]

    # ── Gestión Milei ─────────────────────────────────────────────────────────
    if "anio_ingreso" in df.columns:
        df["ingreso_gestion_milei"] = df["anio_ingreso"] >= 2024

    # ── Gestión JGM al momento de ingreso ────────────────────────────────────
    def asignar_jgm(row):
        fi = row.get("fecha_ingreso")
        if pd.isna(fi):
            return "Desconocido"
        if fi >= pd.Timestamp("2025-11-04"):
            return "Adorni"
        if fi >= pd.Timestamp("2024-05-27"):
            return "Francos"
        if fi >= pd.Timestamp("2023-12-10"):
            return "Posse"
        return "Pre-Milei"

    if "fecha_ingreso" in df.columns:
        df["jgm_al_ingreso"] = df.apply(asignar_jgm, axis=1)

    # ── Género ────────────────────────────────────────────────────────────────
    if "genero" in df.columns:
        df["genero"] = df["genero"].str.upper().str.strip()
        df["genero"] = df["genero"].map({
            "M": "Masculino", "F": "Femenino", "X": "No binario",
            "MASCULINO": "Masculino", "FEMENINO": "Femenino"
        }).fillna("No informado")

    # ── Nombre completo unificado ─────────────────────────────────────────────
    if "apellido" in df.columns and "nombre" in df.columns:
        df["nombre_completo"] = (
            df["apellido"].fillna("") + ", " + df["nombre"].fillna("")
        ).str.strip(", ")

    # ── Monitor: flag de organismos de interés ────────────────────────────────
    ORGANISMOS_MONITOR = {
        "Jefatura de Gabinete de Ministros": "JGM",
        "Presidencia de la Nación":          "Presidencia",
    }
    if "organismo" in df.columns:
        df["en_monitor"] = df["organismo"].map(ORGANISMOS_MONITOR).notna()
        df["organismo_monitor"] = df["organismo"].map(ORGANISMOS_MONITOR).fillna("")

    return df


# ── Guardar ───────────────────────────────────────────────────────────────────
def guardar(df_raw: pd.DataFrame, df_proc: pd.DataFrame):
    df_raw.to_csv(RAW_CSV, index=False, encoding="utf-8-sig")
    df_proc.to_csv(PROC_CSV, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Raw guardado:       {RAW_CSV}  ({len(df_raw):,} filas)")
    print(f"[OK] Procesado guardado: {PROC_CSV}  ({len(df_proc):,} filas)")


# ── Resumen ───────────────────────────────────────────────────────────────────
def resumen(df: pd.DataFrame):
    print("\n" + "=" * 60)
    print("RESUMEN NÓMINA APN")
    print("=" * 60)
    print(f"Total registros:  {len(df):,}")

    if "tipo_planta_std" in df.columns:
        print("\nPor escalafón:")
        for tipo, cnt in df["tipo_planta_std"].value_counts().items():
            print(f"  {tipo:<45} {cnt:>6,}  ({cnt/len(df)*100:.1f}%)")

    if "jerarquia" in df.columns:
        print("\nTop 10 jerarquías:")
        for j, cnt in df["jerarquia"].value_counts().head(10).items():
            print(f"  {str(j):<45} {cnt:>6,}")

    if "organismo" in df.columns:
        print("\nPor organismo:")
        for org, cnt in df["organismo"].value_counts().items():
            print(f"  {str(org):<50} {cnt:>6,}")

    if "genero" in df.columns:
        print("\nPor género:")
        for g, cnt in df["genero"].value_counts().items():
            print(f"  {g:<20} {cnt:>8,}  ({cnt/len(df)*100:.1f}%)")

    if "jgm_al_ingreso" in df.columns:
        print("\nIngresos por gestión JGM (según decreto):")
        for jgm, cnt in df["jgm_al_ingreso"].value_counts().items():
            print(f"  {jgm:<20} {cnt:>8,}")

    if "organismo_monitor" in df.columns:
        monitor = df[df["en_monitor"]]
        print(f"\nRegistros en Monitor (JGM + Presidencia): {len(monitor):,}")
        if "jerarquia" in monitor.columns:
            print("  Top jerarquías en Monitor:")
            for j, cnt in monitor["jerarquia"].value_counts().head(8).items():
                print(f"    {str(j):<40} {cnt:>4,}")

    if "sueldo_bruto_estimado_ars" in df.columns:
        no_zero = df[df["sueldo_bruto_estimado_ars"] > 0]["sueldo_bruto_estimado_ars"]
        if not no_zero.empty:
            print(f"\nSueldo bruto estimado promedio (excl. Ad Honorem): "
                  f"${no_zero.mean():,.0f} ARS")

    print("=" * 60)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("SCRAPER NÓMINA APN — MapaDelEstado / BIEP / datos.gob.ar")
    print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. Descargar (cascada de fuentes)
    df_raw = descargar_nomina()

    # Guardar raw antes de procesar
    df_raw.to_csv(RAW_CSV, index=False, encoding="utf-8-sig")
    print(f"[OK] Raw guardado: {RAW_CSV}")

    # 2. Normalizar columnas
    df = normalizar_columnas(df_raw.copy())

    # 3. Clasificar tipo de planta
    df = clasificar_tipo_planta(df)

    # 4. Imputar sueldo estimado
    df = imputar_sueldo(df)

    # 5. Enriquecer con columnas derivadas
    df = enriquecer(df)

    # 6. Guardar
    guardar(df_raw, df)

    # 7. Resumen
    resumen(df)

    return df


if __name__ == "__main__":
    main()