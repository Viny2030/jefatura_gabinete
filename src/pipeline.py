"""
pipeline.py
===========
Orquestador principal del portal anticorrupción JGM.
Corre en orden: scrapers → motor de matrices → genera data/inteligencia.json

Uso:
    python pipeline.py                    # pipeline completo
    python pipeline.py --step ingesta     # solo scrapers
    python pipeline.py --step motor       # solo motor de matrices
    python pipeline.py --step bora        # solo BORA
    python pipeline.py --step comprar     # solo COMPRAR
    python pipeline.py --step tgn         # solo TGN

Scheduling:
    - Cron diario (BORA, TGN): 0 7 * * *
    - Cron semanal (COMPRAR completo): 0 6 * * 1
    - GitHub Action: .github/workflows/update_data.yml
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

# Ajustar path para imports relativos
sys.path.insert(0, os.path.dirname(__file__))

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def run_step(nombre: str, func, *args, **kwargs):
    """Ejecuta un step con logging de tiempo y manejo de errores."""
    print(f"\n{'='*60}")
    print(f"[PIPELINE] STEP: {nombre}")
    print(f"{'='*60}")
    t0 = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - t0
        print(f"[OK] {nombre} completado en {elapsed:.1f}s")
        return result
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] {nombre} falló en {elapsed:.1f}s: {e}")
        import traceback
        traceback.print_exc()
        return None


def step_bora(dias: int = 1):
    from ingestion.extractor_bora import run as bora_run, save as bora_save
    data = bora_run(dias=dias)
    bora_save(data)
    return data


def step_comprar(anio: int = None):
    from ingestion.extractor_comprar import run as comprar_run, save as comprar_save
    data = comprar_run(anio=anio)
    comprar_save(data)
    return data


def step_tgn(anio: int = None):
    from ingestion.extractor_tgn import run as tgn_run, save as tgn_save
    data = tgn_run(anio=anio)
    tgn_save(data)
    return data


def step_motor(solo: str = None):
    from engine.motor_matrices import run as motor_run, save as motor_save
    data = motor_run(solo=solo)
    motor_save(data)
    return data


def generar_resumen(inteligencia: dict) -> dict:
    """Genera un resumen ejecutivo para el dashboard."""
    if not inteligencia:
        return {}

    meta = inteligencia.get("meta", {})
    alertas = inteligencia.get("alertas", [])
    grafo = inteligencia.get("grafo", {})

    # Agrupar alertas por tipo
    tipos = {}
    for a in alertas:
        t = a.get("tipo_alerta", "OTRO")
        if t not in tipos:
            tipos[t] = {"alta": 0, "media": 0, "total": 0}
        tipos[t]["total"] += 1
        if a.get("nivel") == "ALTA":
            tipos[t]["alta"] += 1
        else:
            tipos[t]["media"] += 1

    # Top 5 alertas de mayor impacto financiero
    alertas_financieras = sorted(
        [a for a in alertas if a.get("monto_m") or a.get("monto_total_m")],
        key=lambda x: x.get("monto_m") or x.get("monto_total_m") or 0,
        reverse=True
    )[:5]

    resumen = {
        "kpis": {
            "total_alertas": meta.get("total_alertas", 0),
            "alertas_alta": meta.get("alertas_alta", 0),
            "alertas_media": meta.get("alertas_media", 0),
            "funcionarios_en_alerta": grafo.get("stats", {}).get("nodos_funcionarios", 0),
            "empresas_en_alerta": grafo.get("stats", {}).get("nodos_empresas", 0),
            "vinculos_familiares": grafo.get("stats", {}).get("aristas_rojas", 0),
            "vinculos_comerciales": grafo.get("stats", {}).get("aristas_amarillas", 0),
        },
        "por_tipo": tipos,
        "top_alertas_financieras": alertas_financieras,
        "ultima_actualizacion": meta.get("ultima_actualizacion", ""),
        "fuentes": ["BORA", "COMPRAR/SIPRO", "TGN/Presupuesto Abierto"]
    }

    return resumen


def run_pipeline(steps: list = None, dias_bora: int = 1, anio: int = None):
    """Corre el pipeline completo o los steps especificados."""
    t_total = time.time()
    all_steps = steps or ["bora", "comprar", "tgn", "motor"]

    print(f"\n{'#'*60}")
    print(f"# PIPELINE ANTICORRUPCIÓN JGM — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"# Steps: {', '.join(all_steps)}")
    print(f"{'#'*60}")

    os.makedirs(DATA_DIR, exist_ok=True)

    if "bora" in all_steps or "ingesta" in all_steps:
        run_step("BORA", step_bora, dias=dias_bora)

    if "comprar" in all_steps or "ingesta" in all_steps:
        run_step("COMPRAR", step_comprar, anio=anio)

    if "tgn" in all_steps or "ingesta" in all_steps:
        run_step("TGN", step_tgn, anio=anio)

    inteligencia = None
    if "motor" in all_steps:
        inteligencia = run_step("MOTOR DE MATRICES", step_motor)

    if inteligencia:
        resumen = generar_resumen(inteligencia)
        resumen_path = os.path.join(DATA_DIR, "resumen.json")
        with open(resumen_path, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2)
        print(f"[OK] Resumen guardado en {resumen_path}")

    elapsed_total = time.time() - t_total
    print(f"\n{'#'*60}")
    print(f"# PIPELINE COMPLETADO en {elapsed_total:.1f}s")
    if inteligencia:
        meta = inteligencia.get("meta", {})
        print(f"# Alertas: {meta.get('total_alertas',0)} ({meta.get('alertas_alta',0)} ALTA, {meta.get('alertas_media',0)} MEDIA)")
    print(f"{'#'*60}\n")

    return inteligencia


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline anticorrupción JGM")
    parser.add_argument("--step", choices=["bora", "comprar", "tgn", "motor", "ingesta"],
                        help="Step específico a correr")
    parser.add_argument("--dias", type=int, default=1, help="Días de BORA hacia atrás")
    parser.add_argument("--anio", type=int, help="Año para COMPRAR y TGN")
    args = parser.parse_args()

    steps = [args.step] if args.step else None
    run_pipeline(steps=steps, dias_bora=args.dias, anio=args.anio)
