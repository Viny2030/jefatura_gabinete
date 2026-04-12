#!/usr/bin/env python3
"""
generar_cruces_pen.py
=====================
Cruza funcionarios JGM con proveedores del PEN (monitor_contratos_v2 + gob_bo_comprar_tgn).

NIVELES:
  Nivel 1 — CUIL funcionario == CUIT proveedor              → ALTO
  Nivel 2 — Apellido funcionario en razón social proveedor  → MEDIO
  Nivel 3 — Proveedor cobra en JGM y en otro organismo PEN  → MEDIO

OUTPUT: src/frontend/data/cruces.json
"""

import os
import re
import json
import glob
import unicodedata
import logging
from datetime import datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PERSONAL_JGM = os.path.join(BASE_DIR, "src", "frontend", "data", "personal_jgm.json")
OUTPUT_JSON  = os.path.join(BASE_DIR, "src", "frontend", "data", "cruces.json")

DATA_PATHS = [
    os.path.join(BASE_DIR, "..", "monitor", "data"),
    os.path.join(BASE_DIR, "..", "tgn", "data"),
    r"C:\Users\ASUS\PycharmProjects\monitor_contratos_v2\data",
    r"C:\Users\ASUS\PycharmProjects\gob_bo_comprar_tgn\data",
]

JGM_SAF = "305"


# ── Helpers ───────────────────────────────────────────────────────────────────
def normalizar(texto):
    if not texto:
        return ""
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize("NFD", texto)
    texto = "".join(c for c in texto if unicodedata.category(c) != "Mn")
    texto = re.sub(r"[^A-Z\s]", "", texto)
    return texto.strip()


def cuil_a_str(cuil):
    if not cuil or (isinstance(cuil, float) and pd.isna(cuil)):
        return ""
    return re.sub(r"\D", "", str(int(round(float(cuil)))))


def cuit_a_str(cuit):
    if not cuit or (isinstance(cuit, float) and pd.isna(cuit)):
        return ""
    return re.sub(r"\D", "", str(cuit))


def parse_monto(val):
    """Convierte montos en cualquier formato a float. Ej: '$1.163.068.90' -> 1163068.90"""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        if pd.isna(val):
            return 0
        return float(val)
    try:
        limpio = str(val).replace("$", "").replace(" ", "").strip()
        if "," in limpio and "." in limpio:
            limpio = limpio.replace(".", "").replace(",", ".")
        elif "," in limpio:
            limpio = limpio.replace(",", ".")
        elif limpio.count(".") > 1:
            limpio = limpio.replace(".", "")
        return float(limpio) if limpio else 0
    except Exception:
        return 0


def fmt_monto(n):
    if not n:
        return "—"
    try:
        n = float(n)
    except Exception:
        return "—"
    if n >= 1e9:
        return f"${n/1e9:.1f}B"
    if n >= 1e6:
        return f"${n/1e6:.1f}M"
    return f"${n:,.0f}"


# ── Carga de personal JGM ─────────────────────────────────────────────────────
def cargar_personal():
    with open(PERSONAL_JGM, encoding="utf-8") as f:
        data = json.load(f)
    personal = []
    for p in data:
        cuil = cuil_a_str(p.get("cuil", ""))
        apellido = normalizar(p.get("apellido", ""))
        if not apellido or len(apellido) < 4:
            continue
        personal.append({
            "apellido":  apellido,
            "nombre":    p.get("nombre", ""),
            "cargo":     p.get("cargo", ""),
            "jerarquia": p.get("jerarquia", ""),
            "gestion":   p.get("jgm_al_ingreso", ""),
            "cuil":      cuil,
        })
    log.info(f"Personal JGM cargado: {len(personal)} registros")
    return personal


# ── Carga de contratos PEN ────────────────────────────────────────────────────
def cargar_contratos_pen():
    registros = []
    rutas_usadas = []

    for base in DATA_PATHS:
        if not os.path.exists(base):
            continue
        patron = os.path.join(base, "**", "reporte_*.xlsx")
        archivos = glob.glob(patron, recursive=True)
        if not archivos:
            continue
        rutas_usadas.append(base)
        log.info(f"Leyendo {len(archivos)} archivos desde {base}")

        for archivo in archivos:
            try:
                xl = pd.ExcelFile(archivo)

                hoja = 0
                for nombre in xl.sheet_names:
                    if "Detalle" in nombre or "detalle" in nombre:
                        hoja = nombre
                        break

                df = pd.read_excel(archivo, sheet_name=hoja)

                cols_req = ["cuit_proveedor", "proveedor_adjudicado", "organismo_contratante"]
                if not all(c in df.columns for c in cols_req):
                    continue

                df = df[df["cuit_proveedor"].notna()].copy()
                if df.empty:
                    continue

                for _, row in df.iterrows():
                    cuit = cuit_a_str(row.get("cuit_proveedor", ""))
                    if not cuit:
                        continue
                    registros.append({
                        "cuit":       cuit,
                        "proveedor":  str(row.get("proveedor_adjudicado", "")).strip(),
                        "organismo":  str(row.get("organismo_contratante", "")).strip(),
                        "monto_bora": row.get("monto_adjudicado_bora"),
                        "monto_tgn":  row.get("monto_cobrado_tgn"),
                        "fecha":      str(row.get("fecha", ""))[:10],
                        "link":       str(row.get("link_bora", "")),
                        "nivel_riesgo": str(row.get("nivel_riesgo_licit", "")),
                    })
            except Exception as e:
                log.debug(f"  Error leyendo {archivo}: {e}")

    log.info(f"Contratos PEN cargados: {len(registros)} registros con CUIT de {len(rutas_usadas)} fuentes")
    return registros


# ── Nivel 1: CUIL == CUIT ─────────────────────────────────────────────────────
def nivel1(personal, contratos):
    cruces = []
    cuil_map = {p["cuil"]: p for p in personal if p["cuil"]}

    for c in contratos:
        if c["cuit"] in cuil_map:
            p = cuil_map[c["cuit"]]
            monto = parse_monto(c["monto_bora"])
            cruces.append({
                "nivel":       "ALTO",
                "tipo":        "CUIL/CUIT exacto — Funcionario activo / Proveedor",
                "funcionario": f"{p['apellido']}, {p['nombre']}",
                "cargo":       p["cargo"],
                "gestion":     p["gestion"],
                "empresa":     c["proveedor"],
                "cuit":        c["cuit"],
                "organismo":   c["organismo"],
                "monto":       monto,
                "monto_fmt":   fmt_monto(monto),
                "fecha":       c["fecha"],
                "link":        c["link"],
                "contratos":   1,
            })

    log.info(f"Nivel 1 (CUIL==CUIT): {len(cruces)} alertas")
    return cruces


# ── Nivel 2: Apellido en razón social ─────────────────────────────────────────
def nivel2(personal, contratos):
    agrupados = {}

    for p in personal:
        apellido = p["apellido"]
        if len(apellido) < 4:
            continue
        regex = re.compile(r"\b" + re.escape(apellido) + r"\b")

        for c in contratos:
            razon = normalizar(c["proveedor"])
            if not razon:
                continue
            if regex.search(razon):
                key = f"{apellido}|{c['proveedor']}"
                if key not in agrupados:
                    agrupados[key] = {
                        "nivel":       "MEDIO",
                        "tipo":        "Apellido en razón social — posible vínculo",
                        "funcionario": f"{p['apellido']}, {p['nombre']}",
                        "cargo":       p["cargo"],
                        "gestion":     p["gestion"],
                        "empresa":     c["proveedor"],
                        "cuit":        c["cuit"],
                        "organismo":   c["organismo"],
                        "monto":       0,
                        "monto_fmt":   "—",
                        "fecha":       c["fecha"],
                        "link":        c["link"],
                        "contratos":   0,
                    }
                agrupados[key]["monto"]     += parse_monto(c["monto_bora"])
                agrupados[key]["contratos"] += 1

    resultado = list(agrupados.values())
    for r in resultado:
        r["monto_fmt"] = fmt_monto(r["monto"]) if r["monto"] else "—"

    log.info(f"Nivel 2 (apellido): {len(resultado)} alertas")
    return resultado


# ── Nivel 3: Proveedor cobra en JGM y en otro organismo ──────────────────────
def nivel3(contratos):
    cruces = []
    por_cuit = {}

    for c in contratos:
        cuit = c["cuit"]
        if not cuit:
            continue
        if cuit not in por_cuit:
            por_cuit[cuit] = {"proveedor": c["proveedor"], "organismos": set(), "contratos": []}
        por_cuit[cuit]["organismos"].add(c["organismo"])
        por_cuit[cuit]["contratos"].append(c)

    for cuit, data in por_cuit.items():
        organismos = data["organismos"]
        es_jgm = any(
            "JEFATURA" in o.upper() or "GABINETE" in o.upper()
            for o in organismos
        )
        if not es_jgm or len(organismos) < 2:
            continue

        otros = [o for o in organismos if "JEFATURA" not in o.upper() and "GABINETE" not in o.upper()]
        monto_total = sum(parse_monto(c["monto_bora"]) for c in data["contratos"])

        cruces.append({
            "nivel":       "MEDIO",
            "tipo":        f"Proveedor multi-organismo — JGM + {len(otros)} organismo(s) mas",
            "funcionario": "—",
            "cargo":       "—",
            "gestion":     "—",
            "empresa":     data["proveedor"],
            "cuit":        cuit,
            "organismo":   " | ".join(list(organismos)[:3]),
            "monto":       monto_total,
            "monto_fmt":   fmt_monto(monto_total),
            "fecha":       data["contratos"][0]["fecha"],
            "link":        data["contratos"][0]["link"],
            "contratos":   len(data["contratos"]),
        })

    log.info(f"Nivel 3 (multi-organismo): {len(cruces)} alertas")
    return cruces


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("GENERADOR DE CRUCES PEN")
    log.info(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    log.info("=" * 60)

    personal  = cargar_personal()
    contratos = cargar_contratos_pen()

    if not contratos:
        log.warning("No se encontraron contratos con CUIT. Verificar rutas de datos.")
        resultado = {
            "generado_en": datetime.now().isoformat(),
            "total":       0,
            "altos":       0,
            "medios":      0,
            "monto_total": 0,
            "cruces":      [],
        }
    else:
        cruces_n1 = nivel1(personal, contratos)
        cruces_n2 = nivel2(personal, contratos)
        cruces_n3 = nivel3(contratos)

        todos = cruces_n1 + cruces_n2 + cruces_n3
        todos.sort(key=lambda x: (0 if x["nivel"] == "ALTO" else 1, -x["monto"]))

        monto_total = sum(c["monto"] for c in todos)
        altos  = sum(1 for c in todos if c["nivel"] == "ALTO")
        medios = sum(1 for c in todos if c["nivel"] == "MEDIO")

        resultado = {
            "generado_en": datetime.now().isoformat(),
            "total":       len(todos),
            "altos":       altos,
            "medios":      medios,
            "monto_total": monto_total,
            "cruces":      todos[:100],
        }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"cruces.json exportado → {OUTPUT_JSON}")
    log.info(f"  Total: {resultado['total']} | ALTO: {resultado['altos']} | MEDIO: {resultado['medios']}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()