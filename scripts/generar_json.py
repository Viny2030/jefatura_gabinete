"""
generar_json.py
===============
Lee los CSVs del scraper y genera JSONs listos para consumir
desde los dashboards HTML sin necesitar API ni servidor.

Fuentes (en orden de prioridad):
  1. contratos_jgm.csv        ← scraper comprar.gob.ar (fuente limpia, con numero_proceso)
  2. adjudicaciones_*.csv     ← fallback BORA (si no existe el anterior)

Salidas:
  src/frontend/data/contratos_jgm.json
  src/frontend/data/contratos_sgp.json
  src/frontend/data/contratos_presidencia.json
  src/frontend/data/personal_jgm.json
  src/frontend/data/personal_sgp.json
  src/frontend/data/personal_presidencia.json
  src/frontend/data/meta.json

Uso:
  python scripts/generar_json.py
"""

import os
import json
import codecs
import pandas as pd
from datetime import datetime

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "..", "data")
OUT_DIR  = os.path.join(BASE, "..", "src", "frontend", "data")
os.makedirs(OUT_DIR, exist_ok=True)

NOMINA_CSV            = os.path.join(DATA_DIR, "nomina_apn_procesada.csv")
CONTRATOS_COMPRAR_CSV = os.path.join(BASE, "..", "contratos_jgm.csv")

# ── Selección de fuente de contratos ─────────────────────────────────────────
if os.path.exists(CONTRATOS_COMPRAR_CSV):
    CONTRATOS_CSV = CONTRATOS_COMPRAR_CSV
    FUENTE        = "comprar"
else:
    _csvs = sorted(
        [f for f in os.listdir(DATA_DIR)
         if f.startswith("adjudicaciones_") and f.endswith(".csv")],
        reverse=True,
    )
    if not _csvs:
        raise FileNotFoundError(
            f"No se encontró contratos_jgm.csv ni adjudicaciones_*.csv en {DATA_DIR}"
        )
    CONTRATOS_CSV = os.path.join(DATA_DIR, _csvs[0])
    FUENTE        = "bora"

# ── Filtros de organismo (solo se usan cuando FUENTE == "bora") ───────────────
ORG_JGM  = ["305"]
ORG_SGP  = ["301"]
ORG_PRES = ["338", "337", "322", "303", "300", "302", "304",
            "306", "307", "308", "309"]

def es_jgm(row):
    return "Jefatura" in str(row.get("organismo", ""))

def es_sgp(row):
    return "General" in str(row.get("sub_organismo", ""))

def es_pres(row):
    return ("Presidencia" in str(row.get("organismo", "")) and
            "General"    not in str(row.get("sub_organismo", "")))

# ── Helpers ───────────────────────────────────────────────────────────────────
def limpiar_nan(obj):
    if isinstance(obj, dict):
        return {k: limpiar_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [limpiar_nan(i) for i in obj]
    if obj != obj:                              # NaN
        return None
    if isinstance(obj, float) and obj == float("inf"):
        return None
    return obj

def guardar_json(data, ruta):
    with codecs.open(ruta, "w", encoding="utf-8") as f:
        json.dump(limpiar_nan(data), f, ensure_ascii=False, indent=2, default=str)
    print(f"  → {os.path.basename(ruta)} ({len(data):,} registros)")

def filtrar_org_contratos(df, codigos, prefijos_rama=None):
    mask = df["organismo"].str.contains("|".join(codigos), na=False, regex=True)
    if prefijos_rama:
        mask_rama = df["organismo"].str.startswith(tuple(prefijos_rama), na=False)
        mask = mask | mask_rama
    return df[mask]

def asignar_gestion(row):
    if pd.notna(row.get("fecha_adjudicacion")):
        anio = row["fecha_adjudicacion"].year
    elif pd.notna(row.get("ejercicio")):
        anio = int(row["ejercicio"])
    else:
        return "Sin datos"
    if anio <= 2015: return "Kirchner/CFK"
    if anio <= 2019: return "Macri"
    if anio <= 2023: return "Alberto"
    return "Milei"

# ── Banner ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("GENERADOR DE JSONs — Monitor de Transparencia PEN")
print(f"Ejecutado : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print(f"Fuente    : {FUENTE.upper()} → {os.path.basename(CONTRATOS_CSV)}")
print("=" * 60)

# ── [1] Nómina ────────────────────────────────────────────────────────────────
print("\n[1] Cargando nómina...")
df_nom = pd.read_csv(NOMINA_CSV, encoding="utf-8-sig", low_memory=False)
df_nom.columns = df_nom.columns.str.strip()
print(f"    {len(df_nom):,} filas | columnas: {df_nom.columns.tolist()}")

# ── [2] Contratos ─────────────────────────────────────────────────────────────
print("\n[2] Cargando contratos...")
df_con = pd.read_csv(CONTRATOS_CSV, encoding="utf-8-sig", low_memory=False)
df_con.columns = df_con.columns.str.strip()
print(f"    {len(df_con):,} filas | columnas: {df_con.columns.tolist()}")

# ── [3] Normalización según fuente ───────────────────────────────────────────
print("\n[3] Normalizando contratos...")

if FUENTE == "comprar":
    # ── Cargar detalle una sola vez (montos/CUIT/razón social) ───────────────
    _DETALLE_CSV = os.path.join(BASE, "..", "contratos_detalle.csv")
    _mapa_detalle = None
    if os.path.exists(_DETALLE_CSV):
        try:
            _det = pd.read_csv(_DETALLE_CSV, encoding="utf-8-sig", low_memory=False)
            _det.columns = _det.columns.str.strip()
            _det["numero_proceso"] = _det["numero_proceso"].astype(str).str.strip()
            _det["monto_adjudicado"] = pd.to_numeric(_det.get("monto_adjudicado"), errors="coerce")
            _mapa_detalle = _det.set_index("numero_proceso")
        except Exception as e:
            print(f"    [WARN] No se pudo leer contratos_detalle.csv: {e}")

    def normalizar_comprar(dfc, etiqueta="JGM"):
        """Normaliza un CSV de comprar.gob.ar al esquema del frontend.
        Cruza montos/CUIT/razón social desde contratos_detalle.csv si está."""
        dfc = dfc.copy()
        dfc.columns = dfc.columns.str.strip()
        dfc["organismo"]         = dfc.get("unidad_ejecutora", pd.Series(dtype=str)).fillna(etiqueta)
        dfc["sub_organismo"]     = ""
        dfc["proveedor"]         = dfc.get("nombre_proceso", pd.Series(dtype=str)).fillna("").str[:80]
        dfc["cuit"]              = ""
        dfc["cuit_proveedor"]    = ""
        dfc["tipo_contratacion"] = dfc.get("tipo_proceso", pd.Series(dtype=str)).fillna("")
        dfc["monto_adjudicado"]  = None

        if _mapa_detalle is not None:
            _np = dfc["numero_proceso"].astype(str).str.strip()
            dfc["monto_adjudicado"] = _np.map(_mapa_detalle["monto_adjudicado"])
            if "proveedor_cuit" in _mapa_detalle.columns:
                dfc["cuit"] = _np.map(_mapa_detalle["proveedor_cuit"]).fillna("").astype(str).str.strip()
            if "proveedor_razon" in _mapa_detalle.columns:
                _razon = _np.map(_mapa_detalle["proveedor_razon"]).fillna("").astype(str).str.strip()
                dfc["proveedor"] = _razon.where(_razon != "", dfc["proveedor"])
            _n = int((dfc["monto_adjudicado"] > 0).sum())
            print(f"    [{etiqueta}] detalle cruzado: {_n} contratos con monto (de {len(dfc)})")

        dfc["objeto"]             = dfc.get("nombre_proceso", pd.Series(dtype=str)).fillna("")
        # fecha_apertura viene como "17/02/2023 12:00 Hrs." → extraer dd/mm/yyyy
        _fecha = dfc.get("fecha_apertura", pd.Series(dtype=str)).astype(str).str.extract(r"(\d{2}/\d{2}/\d{4})")[0]
        dfc["fecha_adjudicacion"] = pd.to_datetime(_fecha, format="%d/%m/%Y", errors="coerce")
        dfc["fecha_str"]          = dfc["fecha_adjudicacion"].dt.strftime("%Y-%m-%d")
        dfc["ejercicio"]          = dfc["fecha_adjudicacion"].dt.year
        dfc["gestion"]            = dfc.apply(asignar_gestion, axis=1)
        if "numero_proceso" not in dfc.columns:
            dfc["numero_proceso"] = ""
        if "estado" not in dfc.columns:
            dfc["estado"] = ""
        return dfc

    COLS_CON = [
        "numero_proceso", "organismo", "proveedor", "cuit",
        "tipo_contratacion", "fecha_str", "monto_adjudicado",
        "objeto", "ejercicio", "gestion", "estado", "fecha_apertura",
    ]

    # JGM: el CSV principal ya cargado en df_con
    df_jgm = normalizar_comprar(df_con, "JGM")
    print(f"    Gestiones JGM: {df_jgm['gestion'].value_counts().to_dict()}")

    # SGP y Presidencia: cargar sus CSV propios si el scraper los generó
    def cargar_area_csv(nombre_csv, etiqueta):
        ruta = os.path.join(BASE, "..", nombre_csv)
        if os.path.exists(ruta):
            try:
                _df = pd.read_csv(ruta, encoding="utf-8-sig", low_memory=False)
                if len(_df):
                    print(f"    [{etiqueta}] {len(_df)} filas desde {nombre_csv}")
                    return normalizar_comprar(_df, etiqueta)
            except Exception as e:
                print(f"    [WARN] No se pudo cargar {nombre_csv}: {e}")
        else:
            print(f"    [{etiqueta}] {nombre_csv} no encontrado → sin contratos (aún)")
        return pd.DataFrame(columns=COLS_CON)

    df_sgp  = cargar_area_csv("contratos_sgp.csv",         "SGP")
    df_pres = cargar_area_csv("contratos_presidencia.csv", "PRESIDENCIA")


else:
    # Fuente BORA: adjudicaciones_*.csv
    df_con["organismo"] = (
        df_con["organismo"]
        .str.encode("latin-1", errors="ignore")
        .str.decode("utf-8", errors="ignore")
    )
    df_con["proveedor"]         = df_con["proveedor"].fillna(df_con["objeto"].str[:60])
    df_con["cuit"]              = df_con["cuit_proveedor"].astype(str)
    df_con["tipo_contratacion"] = df_con["tipo_proceso"]
    df_con["monto_adjudicado"]  = pd.to_numeric(df_con["monto_adjudicado"], errors="coerce")
    df_con["fecha_adjudicacion"]= pd.to_datetime(df_con["fecha_adjudicacion"], errors="coerce")
    df_con["fecha_str"]         = df_con["fecha_adjudicacion"].dt.strftime("%Y-%m-%d")
    df_con["gestion"]           = df_con.apply(asignar_gestion, axis=1)
    df_con["numero_proceso"]    = ""
    df_con["estado"]            = ""
    df_con["fecha_apertura"]    = df_con["fecha_adjudicacion"].dt.strftime("%Y-%m-%d")
    print(f"    Gestiones: {df_con['gestion'].value_counts().to_dict()}")

    COLS_CON = [
        "numero_proceso", "organismo", "proveedor", "cuit",
        "tipo_contratacion", "fecha_str", "monto_adjudicado",
        "objeto", "ejercicio", "gestion", "estado", "fecha_apertura",
    ]

    df_jgm  = filtrar_org_contratos(df_con, ORG_JGM,  ["JGM - "])
    df_sgp  = filtrar_org_contratos(df_con, ORG_SGP,  ["SGP - "])
    df_pres = filtrar_org_contratos(df_con, ORG_PRES, ["PRESIDENCIA - "])

# ── [4] Nómina ────────────────────────────────────────────────────────────────
print("\n[4] Normalizando nómina...")
COLS_NOM = [
    "organismo", "sub_organismo", "apellido", "nombre",
    "cargo", "jerarquia", "escalafon", "car_categoria",
    "jgm_al_ingreso", "genero", "norma_designacion",
    "sueldo_bruto_estimado_ars", "cuil", "fecha_ingreso",
]
cols_nom_ok = [c for c in COLS_NOM if c in df_nom.columns]

# ── [5] Generar JSONs ─────────────────────────────────────────────────────────
print("\n[5] Generando JSONs...")

RAMAS = {
    "jgm": {
        "label"       : "Jefatura de Gabinete de Ministros",
        "df_con"      : df_jgm,
        "filtro_nom"  : es_jgm,
    },
    "sgp": {
        "label"       : "Secretaría General de la Presidencia",
        "df_con"      : df_sgp,
        "filtro_nom"  : es_sgp,
    },
    "presidencia": {
        "label"       : "Presidencia de la Nación (resto)",
        "df_con"      : df_pres,
        "filtro_nom"  : es_pres,
    },
}

resumen = {}

for rama, cfg in RAMAS.items():
    print(f"\n  [{rama.upper()}] {cfg['label']}")

    # Contratos
    df_c = cfg["df_con"].copy()
    # Mantener solo las columnas que existen
    cols_ok = [c for c in COLS_CON if c in df_c.columns]
    df_c = df_c[cols_ok].rename(columns={"fecha_str": "fecha_adjudicacion"})
    registros_c = df_c.to_dict(orient="records")
    guardar_json(registros_c, os.path.join(OUT_DIR, f"contratos_{rama}.json"))

    # Nómina
    df_n = df_nom[df_nom.apply(cfg["filtro_nom"], axis=1)][cols_nom_ok].copy()
    registros_n = df_n.to_dict(orient="records")
    guardar_json(registros_n, os.path.join(OUT_DIR, f"personal_{rama}.json"))

    monto_total = float(df_c["monto_adjudicado"].sum()) if "monto_adjudicado" in df_c.columns else 0.0
    resumen[rama] = {
        "contratos"  : len(registros_c),
        "personal"   : len(registros_n),
        "monto_total": monto_total,
    }

# ── [6] Meta ──────────────────────────────────────────────────────────────────
meta = {
    "generado"        : datetime.now().strftime("%Y-%m-%d %H:%M"),
    "fuente_contratos": FUENTE,
    "csv_utilizado"   : os.path.basename(CONTRATOS_CSV),
    "total_contratos" : len(df_con),
    "total_personal"  : len(df_nom),
    "ramas"           : resumen,
}
guardar_json(meta, os.path.join(OUT_DIR, "meta.json"))

# ── Resumen final ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("RESUMEN")
print("=" * 60)
for rama, r in resumen.items():
    monto = r["monto_total"]
    if monto and monto >= 1e9:
        monto_str = f"${monto/1e9:.1f}B"
    elif monto and monto >= 1e6:
        monto_str = f"${monto/1e6:.1f}M"
    elif monto:
        monto_str = f"${monto:,.0f}"
    else:
        monto_str = "sin monto (comprar.gob.ar no publica montos en listado)"
    print(f"  {rama.upper():<15} contratos: {r['contratos']:>5,}  "
          f"personal: {r['personal']:>4,}  monto: {monto_str}")

print(f"\nJSONs guardados en : {OUT_DIR}")
print(f"Fuente utilizada   : {FUENTE.upper()}")
print("=" * 60)
