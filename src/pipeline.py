"""
pipeline.py
===========
Orquestador principal del portal anticorrupción JGM.

Uso:
    python pipeline.py                    # pipeline completo
    python pipeline.py --step ingesta     # solo scrapers
    python pipeline.py --step motor       # solo motor de matrices
    python pipeline.py --step bora        # solo BORA
    python pipeline.py --step comprar     # solo COMPRAR
    python pipeline.py --step tgn         # solo TGN
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

# Ajustar path — funciona desde src/ o desde raiz del proyecto
_SRC = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SRC)
sys.path.insert(0, os.path.join(_SRC, "ingestion"))
sys.path.insert(0, os.path.join(_SRC, "engine"))

DATA_DIR = os.path.join(_SRC, "..", "data")


def run_step(nombre, func, *args, **kwargs):
    print("\n" + "="*60)
    print("[PIPELINE] STEP: " + nombre)
    print("="*60)
    t0 = time.time()
    try:
        result = func(*args, **kwargs)
        print("[OK] {} completado en {:.1f}s".format(nombre, time.time() - t0))
        return result
    except Exception as e:
        print("[ERROR] {} falló en {:.1f}s: {}".format(nombre, time.time() - t0, e))
        import traceback
        traceback.print_exc()
        return None


def step_bora(dias=1):
    import extractor_bora as m
    data = m.run(dias=dias)
    m.save(data)
    return data


def step_comprar(anio=None):
    import extractor_comprar as m
    data = m.run(anio=anio)
    m.save(data)
    return data


def step_tgn(anio=None):
    import extractor_tgn as m
    data = m.run(anio=anio)
    m.save(data)
    return data


def step_motor(solo=None):
    import motor_matrices as m
    data = m.run(solo=solo)
    m.save(data)
    return data


def generar_resumen(inteligencia):
    if not inteligencia:
        return {}
    meta = inteligencia.get("meta", {})
    alertas = inteligencia.get("alertas", [])
    grafo = inteligencia.get("grafo", {})

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

    return {
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
        "ultima_actualizacion": meta.get("ultima_actualizacion", ""),
        "fuentes": ["BORA", "COMPRAR/SIPRO", "TGN/Presupuesto Abierto"]
    }


def run_pipeline(steps=None, dias_bora=1, anio=None):
    t_total = time.time()
    all_steps = steps or ["bora", "comprar", "tgn", "motor"]

    print("\n" + "#"*60)
    print("# PIPELINE ANTICORRUPCIÓN JGM — " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("# Steps: " + ", ".join(all_steps))
    print("#"*60)

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
        print("[OK] Resumen guardado en " + resumen_path)

    print("\n" + "#"*60)
    print("# PIPELINE COMPLETADO en {:.1f}s".format(time.time() - t_total))
    if inteligencia:
        meta = inteligencia.get("meta", {})
        print("# Alertas: {} ({} ALTA, {} MEDIA)".format(
            meta.get("total_alertas", 0),
            meta.get("alertas_alta", 0),
            meta.get("alertas_media", 0)
        ))
    print("#"*60 + "\n")
    return inteligencia


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline anticorrupción JGM")
    parser.add_argument("--step", choices=["bora", "comprar", "tgn", "motor", "ingesta"])
    parser.add_argument("--dias", type=int, default=1)
    parser.add_argument("--anio", type=int)
    args = parser.parse_args()

    steps = [args.step] if args.step else None
    run_pipeline(steps=steps, dias_bora=args.dias, anio=args.anio)
