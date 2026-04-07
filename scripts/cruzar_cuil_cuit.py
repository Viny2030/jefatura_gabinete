"""
cruzar_cuil_cuit.py
===================
Cruza CUILs de la nómina APN contra CUITs de contratos para detectar
funcionarios que también figuran como proveedores del Estado.

Salida:
  src/frontend/data/cruces.json

Uso:
  python scripts/cruzar_cuil_cuit.py
"""

import os
import json
import codecs
import pandas as pd
from datetime import datetime

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE          = os.path.dirname(os.path.abspath(__file__))
DATA_DIR      = os.path.join(BASE, "..", "data")
OUT_DIR       = os.path.join(BASE, "..", "src", "frontend", "data")
os.makedirs(OUT_DIR, exist_ok=True)

NOMINA_CSV    = os.path.join(DATA_DIR, "nomina_apn_procesada.csv")
CONTRATOS_CSV = os.path.join(DATA_DIR, "adjudicaciones_20260406.csv")

# ── Helpers ───────────────────────────────────────────────────────────────────
def normalizar_cuil(valor):
    """CUIL viene como float (20218346414.0) → convertir a int primero."""
    if pd.isna(valor):
        return None
    try:
        return str(int(float(valor)))
    except (ValueError, TypeError):
        return str(valor).replace("-", "").replace(" ", "").strip()

def normalizar_cuit(valor):
    """CUIT viene como string con guiones (20-18053177-8) → dígitos puros."""
    if pd.isna(valor):
        return None
    return str(valor).replace("-", "").replace(" ", "").strip()

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
        json.dump(limpiar_nan(data), f, ensure_ascii=False, indent=2, default=str)
    print(f"  → {os.path.basename(ruta)}  ({len(data):,} registros)")

# ── Cargar datos ──────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("MOTOR DE CRUCES — CUIL Nómina vs CUIT Contratos")
print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("="*60)

print("\n[1] Cargando nómina...")
df_nom = pd.read_csv(NOMINA_CSV, encoding="utf-8-sig", low_memory=False)
df_nom.columns = df_nom.columns.str.strip()
print(f"    {len(df_nom):,} funcionarios")

print("\n[2] Cargando contratos...")
df_con = pd.read_csv(CONTRATOS_CSV, encoding="utf-8-sig", low_memory=False)
df_con.columns = df_con.columns.str.strip()
print(f"    {len(df_con):,} contratos")

# ── Normalizar IDs ────────────────────────────────────────────────────────────
print("\n[3] Normalizando CUILs y CUITs...")

df_nom["cuil_norm"] = df_nom["cuil"].apply(normalizar_cuil)
df_con["cuit_norm"] = df_con["cuit_proveedor"].apply(normalizar_cuit)

cuils_validos = df_nom["cuil_norm"].dropna()
cuits_validos = df_con["cuit_norm"].dropna()
print(f"    CUILs nómina válidos:    {len(cuils_validos):,}  (largo: {cuils_validos.apply(len).value_counts().to_dict()})")
print(f"    CUITs contratos únicos:  {len(cuits_validos.unique()):,}  (largo: {cuits_validos.apply(len).value_counts().head(3).to_dict()})")

# ── Cruce ─────────────────────────────────────────────────────────────────────
print("\n[4] Cruzando...")

set_cuils = set(cuils_validos.unique())
set_cuits = set(cuits_validos.unique())
ids_cruzados = set_cuils & set_cuits

print(f"    Coincidencias encontradas: {len(ids_cruzados):,}")

# ── Construir registros de cruce ──────────────────────────────────────────────
print("\n[5] Armando registros...")

resultados = []

for id_norm in ids_cruzados:
    # Datos del funcionario en nómina
    filas_nom = df_nom[df_nom["cuil_norm"] == id_norm]

    for _, func in filas_nom.iterrows():
        # Contratos asociados a ese CUIT
        contratos_func = df_con[df_con["cuit_norm"] == id_norm][[
            "organismo", "tipo_proceso", "monto_adjudicado",
            "objeto", "ejercicio", "fecha_adjudicacion", "cuit_proveedor"
        ]].copy()

        contratos_func["monto_adjudicado"] = pd.to_numeric(
            contratos_func["monto_adjudicado"], errors="coerce"
        )
        contratos_func["fecha_adjudicacion"] = pd.to_datetime(
            contratos_func["fecha_adjudicacion"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

        monto_total = contratos_func["monto_adjudicado"].sum()
        n_contratos = len(contratos_func)

        resultados.append({
            "cuil":            id_norm,
            "nombre":          str(func.get("nombre", "")),
            "apellido":        str(func.get("apellido", "")),
            "organismo":       str(func.get("organismo", "")),
            "sub_organismo":   str(func.get("sub_organismo", "")),
            "cargo":           str(func.get("cargo", "")),
            "jerarquia":       str(func.get("jerarquia", "")),
            "fecha_ingreso":   str(func.get("fecha_ingreso", "")),
            "n_contratos":     n_contratos,
            "monto_total_ars": float(monto_total) if pd.notna(monto_total) else 0.0,
            "contratos":       contratos_func.to_dict(orient="records"),
        })

# Ordenar por monto descendente
resultados.sort(key=lambda x: x["monto_total_ars"], reverse=True)

# ── Guardar JSON ──────────────────────────────────────────────────────────────
print("\n[6] Guardando...")
guardar_json(resultados, os.path.join(OUT_DIR, "cruces.json"))

# ── Resumen ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("RESUMEN DE CRUCES")
print("="*60)
print(f"  Funcionarios cruzados: {len(resultados):,}")
if resultados:
    monto_total = sum(r["monto_total_ars"] for r in resultados)
    if monto_total >= 1e9:
        monto_str = f"${monto_total/1e9:.2f}B"
    elif monto_total >= 1e6:
        monto_str = f"${monto_total/1e6:.1f}M"
    else:
        monto_str = f"${monto_total:,.0f}"
    print(f"  Monto total involucrado: {monto_str}")
    print(f"\n  Top 5 por monto:")
    for r in resultados[:5]:
        m = r['monto_total_ars']
        m_str = f"${m/1e6:.1f}M" if m >= 1e6 else f"${m:,.0f}"
        print(f"    {r['apellido']}, {r['nombre']} — {r['n_contratos']} contratos — {m_str}")
        print(f"    Cargo: {r['cargo']} | {r['organismo'][:50]}")

print(f"\nJSON guardado en: {OUT_DIR}")
print("="*60)