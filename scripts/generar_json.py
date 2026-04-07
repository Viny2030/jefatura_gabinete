"""
generar_json.py
===============
Lee los CSVs del scraper y genera JSONs listos para consumir
desde los dashboards HTML sin necesitar API ni servidor.

Salidas:
  src/frontend/data/contratos_jgm.json
  src/frontend/data/contratos_sgp.json
  src/frontend/data/contratos_presidencia.json
  src/frontend/data/personal_jgm.json
  src/frontend/data/personal_sgp.json
  src/frontend/data/personal_presidencia.json

Uso:
  python scripts/generar_json.py
"""

import os
import json
import codecs
import pandas as pd
from datetime import datetime

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE        = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE, "..", "data")
OUT_DIR     = os.path.join(BASE, "..", "src", "frontend", "data")
os.makedirs(OUT_DIR, exist_ok=True)

NOMINA_CSV = os.path.join(DATA_DIR, "nomina_apn_procesada.csv")

# Toma siempre el CSV de adjudicaciones más reciente disponible
_csvs = sorted([
    f for f in os.listdir(DATA_DIR)
    if f.startswith("adjudicaciones_") and f.endswith(".csv")
], reverse=True)
if not _csvs:
    raise FileNotFoundError(f"No se encontró ningún adjudicaciones_*.csv en {DATA_DIR}")
CONTRATOS_CSV = os.path.join(DATA_DIR, _csvs[0])

# ── Filtros de organismo ──────────────────────────────────────────────────────
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
            "General" not in str(row.get("sub_organismo", "")))

# ── Helpers ───────────────────────────────────────────────────────────────────
def limpiar_nan(obj):
    if isinstance(obj, dict):
        return {k: limpiar_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [limpiar_nan(i) for i in obj]
    if obj != obj:
        return None
    if isinstance(obj, float) and obj == float('inf'):
        return None
    return obj

def guardar_json(data, ruta):
    with codecs.open(ruta, "w", encoding="utf-8") as f:
        json.dump(limpiar_nan(data), f, ensure_ascii=False, indent=2,
                  default=str)
    print(f"  → {os.path.basename(ruta)}  ({len(data):,} registros)")

def filtrar_org_contratos(df, codigos, prefijos_rama=None):
    """Filtra por códigos SAF O por prefijo de rama (JGM - / SGP - / PRESIDENCIA -)."""
    mask = df["organismo"].str.contains(
        "|".join(codigos), na=False, regex=True
    )
    if prefijos_rama:
        mask_rama = df["organismo"].str.startswith(
            tuple(prefijos_rama), na=False
        )
        mask = mask | mask_rama
    return df[mask]

def asignar_gestion(row):
    if pd.notna(row["fecha_adjudicacion"]):
        anio = row["fecha_adjudicacion"].year
    elif pd.notna(row["ejercicio"]):
        anio = int(row["ejercicio"])
    else:
        return "Sin datos"

    if anio <= 2015:
        return "Kirchner/CFK"
    elif anio <= 2019:
        return "Macri"
    elif anio <= 2023:
        return "Alberto"
    else:
        return "Milei"

# ── Cargar CSVs ───────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("GENERADOR DE JSONs — Monitor de Transparencia PEN")
print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("="*60)

print(f"\n  CSV de contratos: {_csvs[0]}")

print("\n[1] Cargando nómina...")
df_nom = pd.read_csv(NOMINA_CSV, encoding="utf-8-sig", low_memory=False)
df_nom.columns = df_nom.columns.str.strip()
print(f"    {len(df_nom):,} filas, columnas: {df_nom.columns.tolist()}")

print("\n[2] Cargando contratos...")
df_con = pd.read_csv(CONTRATOS_CSV, encoding="utf-8-sig", low_memory=False)
df_con.columns = df_con.columns.str.strip()
print(f"    {len(df_con):,} filas")

# ── Normalizar contratos ──────────────────────────────────────────────────────
print("\n[3] Normalizando contratos...")

df_con["organismo"] = df_con["organismo"].str.encode("latin-1", errors="ignore").str.decode("utf-8", errors="ignore")
df_con["proveedor"]          = df_con["proveedor"].fillna(df_con["objeto"].str[:60])
df_con["cuit"]               = df_con["cuit_proveedor"].astype(str)
df_con["tipo_contratacion"]  = df_con["tipo_proceso"]
df_con["monto_adjudicado"]   = pd.to_numeric(df_con["monto_adjudicado"], errors="coerce")
df_con["fecha_adjudicacion"] = pd.to_datetime(df_con["fecha_adjudicacion"], errors="coerce")
df_con["fecha_str"]          = df_con["fecha_adjudicacion"].dt.strftime("%Y-%m-%d")
df_con["gestion"]            = df_con.apply(asignar_gestion, axis=1)
print(f"    Gestiones asignadas: {df_con['gestion'].value_counts().to_dict()}")

COLS_CON = ["organismo", "proveedor", "cuit", "tipo_contratacion",
            "fecha_str", "monto_adjudicado", "objeto", "ejercicio", "gestion"]

# ── Normalizar nómina ─────────────────────────────────────────────────────────
print("\n[4] Normalizando nómina...")

COLS_NOM = ["organismo", "sub_organismo", "apellido", "nombre",
            "cargo", "jerarquia", "escalafon", "car_categoria",
            "jgm_al_ingreso", "genero", "norma_designacion",
            "sueldo_bruto_estimado_ars", "cuil", "fecha_ingreso"]

cols_nom_ok = [c for c in COLS_NOM if c in df_nom.columns]

# ── Generar JSONs por organismo ───────────────────────────────────────────────
print("\n[5] Generando JSONs...")

RAMAS = {
    "jgm": {
        "label":         "Jefatura de Gabinete de Ministros",
        "codigos_con":   ORG_JGM,
        "prefijos_rama": ["JGM - "],
        "filtro_nom":    es_jgm,
    },
    "sgp": {
        "label":         "Secretaría General de la Presidencia",
        "codigos_con":   ORG_SGP,
        "prefijos_rama": ["SGP - "],
        "filtro_nom":    es_sgp,
    },
    "presidencia": {
        "label":         "Presidencia de la Nación (resto)",
        "codigos_con":   ORG_PRES,
        "prefijos_rama": ["PRESIDENCIA - "],
        "filtro_nom":    es_pres,
    },
}

resumen = {}

for rama, cfg in RAMAS.items():
    print(f"\n  [{rama.upper()}] {cfg['label']}")

    # Contratos — filtra por código SAF Y por prefijo de rama
    df_c = filtrar_org_contratos(df_con, cfg["codigos_con"], cfg.get("prefijos_rama"))
    df_c = df_c[COLS_CON].copy()
    df_c = df_c.rename(columns={"fecha_str": "fecha_adjudicacion"})
    registros_c = df_c.to_dict(orient="records")
    guardar_json(registros_c, os.path.join(OUT_DIR, f"contratos_{rama}.json"))

    # Nómina
    df_n = df_nom[df_nom.apply(cfg["filtro_nom"], axis=1)][cols_nom_ok].copy()
    registros_n = df_n.to_dict(orient="records")
    guardar_json(registros_n, os.path.join(OUT_DIR, f"personal_{rama}.json"))

    resumen[rama] = {
        "contratos":   len(registros_c),
        "personal":    len(registros_n),
        "monto_total": float(df_c["monto_adjudicado"].sum()),
    }

# ── JSON de metadatos ─────────────────────────────────────────────────────────
meta = {
    "generado":        datetime.now().strftime("%Y-%m-%d %H:%M"),
    "total_contratos": len(df_con),
    "total_personal":  len(df_nom),
    "ramas":           resumen,
}
guardar_json(meta, os.path.join(OUT_DIR, "meta.json"))

# ── Resumen final ─────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("RESUMEN")
print("="*60)
for rama, r in resumen.items():
    monto = r['monto_total']
    if monto >= 1e9:
        monto_str = f"${monto/1e9:.1f}B"
    elif monto >= 1e6:
        monto_str = f"${monto/1e6:.1f}M"
    else:
        monto_str = f"${monto:,.0f}"
    print(f"  {rama.upper():<15} contratos: {r['contratos']:>5,}  "
          f"personal: {r['personal']:>4,}  monto: {monto_str}")

print(f"\nJSONs guardados en: {OUT_DIR}")
print("="*60)