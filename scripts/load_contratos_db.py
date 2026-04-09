#!/usr/bin/env python3
"""
load_contratos_db.py
Carga contratos_comprar_raw.json en la tabla contratos de PostgreSQL.
Soporta formato viejo (saf_id, saf_nombre) y nuevo (saf, saf_texto).
Usa INSERT ... ON CONFLICT DO NOTHING para ser idempotente.

Uso:
    python scripts/load_contratos_db.py
    python scripts/load_contratos_db.py --dry-run
"""

import json
import os
import sys
import argparse
from datetime import datetime
from collections import Counter

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

INPUT_JSON   = os.path.join(os.path.dirname(__file__), "..", "src", "frontend", "data", "contratos_comprar_raw.json")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@localhost:5433/jgm_anticorrupcion")

TIPOS_PROCESO = ["Licitación", "Contratación", "Concurso", "Compra", "Subasta",
                 "LicitaciÃ³n", "ContrataciÃ³n"]


def parse_fecha(fecha_str):
    if not fecha_str:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(fecha_str[:10].strip(), fmt).date()
        except ValueError:
            continue
    return None


def normalizar(c):
    """
    Normaliza un contrato al formato canónico independientemente
    de si viene del scraper viejo o nuevo.
    """
    numero = c.get("numero_proceso", "").strip()

    saf = (c.get("saf") or c.get("saf_id") or "").strip()

    nombre_raw = c.get("nombre_proceso", "")
    tipo_raw   = c.get("tipo_proceso", "")
    expediente = c.get("expediente", "")

    if any(nombre_raw.startswith(t) for t in TIPOS_PROCESO) and not tipo_raw:
        nombre = expediente
        tipo   = nombre_raw
    else:
        nombre = nombre_raw
        tipo   = tipo_raw

    area = c.get("area", "")
    if not area:
        ref = (c.get("organismo", "") + " " + c.get("saf_nombre", "")).lower()
        if "gabinete" in ref or "jgm" in ref:
            area = "jgm"
        elif "presidencia" in ref:
            area = "presidencia"
        elif "interior" in ref:
            area = "interior"
        elif "economía" in ref or "economia" in ref or "hacienda" in ref:
            area = "economia"
        elif "salud" in ref:
            area = "salud"
        elif "seguridad" in ref:
            area = "seguridad"
        elif "defensa" in ref:
            area = "defensa"
        elif "justicia" in ref:
            area = "justicia"
        elif "infraestructura" in ref or "transporte" in ref or "obras" in ref:
            area = "infraestructura"
        elif "capital humano" in ref or "educacion" in ref or "trabajo" in ref:
            area = "capital_humano"
        elif "exteriores" in ref or "canciller" in ref:
            area = "exteriores"
        elif "control" in ref or "auditor" in ref:
            area = "control"
        else:
            area = "?"

    # url_detalle — solo presente en scraper nuevo con el fix aplicado
    url_detalle = c.get("url_detalle") or None

    return {
        "numero":      numero,
        "saf":         saf,
        "nombre":      nombre,
        "tipo":        tipo,
        "fecha":       parse_fecha(c.get("fecha_apertura")),
        "estado":      c.get("estado", ""),
        "unidad":      c.get("unidad_ejecutora", ""),
        "saf_texto":   c.get("saf_nombre", c.get("saf_texto", "")),
        "area":        area,
        "organismo":   c.get("organismo", ""),
        "url_detalle": url_detalle,
    }


def load_contratos(dry_run=False):
    print(f"\n{'='*60}")
    print("CARGA CONTRATOS -> PostgreSQL")
    print(f"{'='*60}")
    print(f"  Fuente: {os.path.abspath(INPUT_JSON)}")
    print(f"  DB:     {DATABASE_URL}\n")

    if not os.path.exists(INPUT_JSON):
        print(f"  ERROR: no se encontro {INPUT_JSON}")
        sys.exit(1)

    with open(INPUT_JSON, encoding="utf-8") as f:
        raw = json.load(f)
    print(f"  JSON cargado: {len(raw)} registros")

    rows = []
    skipped = 0
    area_counter = Counter()

    for c in raw:
        n = normalizar(c)
        if not n["numero"] or not n["saf"]:
            skipped += 1
            continue
        area_counter[n["area"]] += 1
        rows.append((
            n["numero"],
            n["nombre"],
            n["tipo"],
            n["fecha"],
            n["estado"],
            n["unidad"],
            n["saf_texto"],
            n["area"],
            n["saf"],
            n["organismo"],
            n["url_detalle"],
        ))

    print(f"\n  Filas válidas:  {len(rows)}")
    print(f"  Saltadas:       {skipped}")

    print("\n  Distribución por área:")
    for area, cnt in sorted(area_counter.items(), key=lambda x: -x[1]):
        print(f"    {area:30s} {cnt}")

    if dry_run:
        print("\n  [DRY RUN] No se realizaron inserciones.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
    except Exception as e:
        print(f"\n  ERROR conectando a DB: {e}")
        sys.exit(1)

    SQL = """
        INSERT INTO contratos (
            numero_proceso, nombre_proceso, tipo_proceso,
            fecha_apertura, estado,
            unidad_ejecutora, saf_texto,
            area, saf, organismo, url_detalle
        ) VALUES %s
        ON CONFLICT (numero_proceso, saf) DO UPDATE
            SET url_detalle = EXCLUDED.url_detalle
            WHERE contratos.url_detalle IS NULL
    """

    BATCH = 500
    insertados = 0
    for i in range(0, len(rows), BATCH):
        lote = rows[i:i+BATCH]
        try:
            execute_values(cur, SQL, lote)
            insertados += len(lote)
            print(f"  Lote {i//BATCH + 1}: {len(lote)} filas procesadas...")
        except Exception as e:
            conn.rollback()
            print(f"  ERROR en lote {i//BATCH + 1}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"\n  Total procesados: {insertados}")
    print(f"  (duplicados descartados por ON CONFLICT)")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula la carga sin escribir en la DB")
    args = parser.parse_args()
    load_contratos(dry_run=args.dry_run)