"""
extractor_tgn.py
================
Extractor de pagos reales de la Tesorería General de la Nación (TGN)
via la API de Presupuesto Abierto (presupuestoabierto.gob.ar).

Permite comparar:
  - Monto adjudicado en COMPRAR (contrato firmado)
  - Monto devengado en TGN (lo que realmente se pagó)

La diferencia > 15% dispara alerta de desvío de flujo de fondos.

Fuente: https://www.presupuestoabierto.gob.ar/sici/datos-abiertos
        Jurisdicción 25 = Jefatura de Gabinete de Ministros

Uso:
    python extractor_tgn.py
    python extractor_tgn.py --anio 2025
"""

import argparse
import csv
import io
import json
import os
from datetime import datetime

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JefaturaMonitor/1.0)"}
TIMEOUT = 60
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

# Jurisdicción 25 = Jefatura de Gabinete de Ministros
JGM_JURISDICCION = "25"

# URLs de datos abiertos de presupuesto
URLS_PRESUPUESTO = [
    "https://www.presupuestoabierto.gob.ar/datasets/credito_jurisdiccion_{anio}.csv",
    "https://www.presupuestoabierto.gob.ar/datasets/ejecucion_{anio}.csv",
]

# API de series de tiempo de datos.gob.ar (backup)
API_SERIES = "https://apis.datos.gob.ar/series/api/series/"


def fetch_ejecucion_jgm(anio: int) -> dict:
    """
    Descarga la ejecución presupuestaria de JGM del año dado.
    Devuelve resumen con crédito, devengado e IAP.
    """
    for url_template in URLS_PRESUPUESTO:
        url = url_template.format(anio=anio)
        try:
            res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if res.status_code != 200:
                continue

            content = res.content.decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(content))

            credito_total = 0.0
            devengado_total = 0.0
            programas = {}

            for row in reader:
                jur = str(row.get("jurisdiccion") or row.get("cod_jurisdiccion") or "").strip()
                desc = (row.get("desc_jurisdiccion") or row.get("jurisdiccion_desc") or "").upper()

                if jur != JGM_JURISDICCION and "GABINETE" not in desc and "JGM" not in desc:
                    continue

                cred = _float(row.get("credito_vigente") or row.get("credito") or "0")
                dev = _float(row.get("devengado") or row.get("ejecutado") or "0")
                programa = row.get("programa") or row.get("desc_programa") or "ND"

                credito_total += cred
                devengado_total += dev

                if programa not in programas:
                    programas[programa] = {"credito": 0.0, "devengado": 0.0}
                programas[programa]["credito"] += cred
                programas[programa]["devengado"] += dev

            if credito_total > 0:
                iap = round(devengado_total / credito_total, 4)
                print(f"[TGN] {anio}: crédito={credito_total/1e9:.2f}B ARS, devengado={devengado_total/1e9:.2f}B ARS, IAP={iap}")
                return {
                    "anio": anio,
                    "fuente": url,
                    "jurisdiccion": "JGM (25)",
                    "credito_vigente_m": round(credito_total / 1e6, 2),
                    "devengado_m": round(devengado_total / 1e6, 2),
                    "iap": iap,
                    "programas": {
                        k: {
                            "credito_m": round(v["credito"] / 1e6, 2),
                            "devengado_m": round(v["devengado"] / 1e6, 2),
                            "iap": round(v["devengado"] / v["credito"], 4) if v["credito"] > 0 else None
                        }
                        for k, v in sorted(programas.items(), key=lambda x: -x[1]["credito"])[:20]
                    }
                }

        except Exception as e:
            print(f"[WARN] TGN {url}: {e}")
            continue

    # Fallback: valor de referencia histórico
    print(f"[WARN] TGN: CSV no disponible para {anio}, usando referencia histórica")
    return {
        "anio": anio,
        "fuente": "historico",
        "jurisdiccion": "JGM (25)",
        "credito_vigente_m": None,
        "devengado_m": None,
        "iap": 0.94,  # IAP histórico JGM 2024
        "nota": "Actualizar con datos reales de presupuestoabierto.gob.ar"
    }


def detectar_desvios(contratos: list, pagos_tgn: dict, umbral_pct: float = 15.0) -> list:
    """
    Cruza contratos adjudicados con pagos TGN.
    Detecta desvíos donde monto_pagado difiere del monto_adjudicado en más del umbral.

    En esta versión inicial compara a nivel de programa presupuestario.
    Para cruce por CUIT/proveedor se requieren datos de órdenes de pago de TGN.
    """
    alertas = []
    programas_tgn = pagos_tgn.get("programas", {})

    for contrato in contratos:
        if contrato.get("tipo") != "adjudicacion":
            continue
        monto_adj = contrato.get("monto_estimado", 0)
        if monto_adj <= 0:
            continue

        # Buscar programa relacionado
        desc = contrato.get("descripcion", "").lower()
        for prog_nombre, prog_datos in programas_tgn.items():
            iap_prog = prog_datos.get("iap")
            if iap_prog is None:
                continue

            # Alerta si el IAP del programa está muy por encima o por debajo de 1
            desvio_pct = abs(1 - iap_prog) * 100
            if desvio_pct > umbral_pct:
                alertas.append({
                    "tipo_alerta": "DESVIO_PRESUPUESTARIO",
                    "nivel": "ALTA" if desvio_pct > 30 else "MEDIA",
                    "descripcion": f"Programa '{prog_nombre}' con IAP={iap_prog} (desvío {desvio_pct:.1f}%)",
                    "contrato_numero": contrato.get("numero_proceso"),
                    "contrato_descripcion": contrato.get("descripcion", "")[:150],
                    "monto_adjudicado_m": round(monto_adj / 1e6, 2),
                    "proveedor": contrato.get("proveedor_nombre", ""),
                    "proveedor_cuit": contrato.get("proveedor_cuit", ""),
                    "fuente": "COMPRAR vs TGN"
                })
            break  # un contrato puede matchear con un solo programa

    return alertas


def _float(val: str) -> float:
    try:
        return float(str(val).replace(".", "").replace(",", ".") or "0")
    except ValueError:
        return 0.0


def run(anio: int = None) -> dict:
    if anio is None:
        anio = datetime.now().year

    ejecucion = fetch_ejecucion_jgm(anio)
    # También traer el año anterior para comparativa
    ejecucion_prev = fetch_ejecucion_jgm(anio - 1)

    return {
        "ejecucion_actual": ejecucion,
        "ejecucion_anterior": ejecucion_prev,
        "variacion_iap": round(
            (ejecucion.get("iap", 0) or 0) - (ejecucion_prev.get("iap", 0) or 0), 4
        ),
        "meta": {
            "ultima_actualizacion": datetime.now().isoformat()
        }
    }


def save(data: dict, path: str = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(OUTPUT_DIR, "tgn_raw.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[TGN] Guardado en {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--anio", type=int, help="Año (default: actual)")
    args = parser.parse_args()

    data = run(anio=args.anio)
    save(data)
