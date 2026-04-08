#!/usr/bin/env python3
"""
fix_state.py
Fusiona scraper_progreso.json + _scraper_state.json en _scraper_state.json
y elimina scraper_progreso.json para evitar conflictos futuros.

Correr desde la raíz del proyecto:
    python scripts/fix_state.py
"""
import json, os

BASE_DIR    = os.path.join(os.path.dirname(__file__), "..", "src", "frontend", "data")
PROGRESO    = os.path.join(BASE_DIR, "scraper_progreso.json")
STATE       = os.path.join(BASE_DIR, "_scraper_state.json")
OUTPUT_JSON = os.path.join(BASE_DIR, "contratos_comprar_raw.json")

# Leer ambos
with open(PROGRESO, encoding="utf-8") as f:
    progreso = json.load(f)

with open(STATE, encoding="utf-8") as f:
    state = json.load(f)

# Total real de contratos en el JSON
total = 0
if os.path.exists(OUTPUT_JSON):
    with open(OUTPUT_JSON, encoding="utf-8") as f:
        data = json.load(f)
        total = len(data)

# Fusionar áreas completadas (sin duplicados, preservando orden)
areas_prev  = progreso.get("areas_completadas", [])
areas_new   = state.get("areas_completadas", [])
merged = list(dict.fromkeys(areas_prev + areas_new))  # preserva orden, elimina dups

nuevo_state = {
    "areas_completadas": merged,
    "total": total
}

with open(STATE, "w", encoding="utf-8") as f:
    json.dump(nuevo_state, f, ensure_ascii=False, indent=2)

print(f"✅ _scraper_state.json actualizado:")
print(f"   Áreas completadas ({len(merged)}): {merged}")
print(f"   Total contratos en JSON: {total}")
print(f"\n   Podés borrar scraper_progreso.json manualmente si querés.")