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

TIPOS_PROCESO = ["Licitación", "Contratación", "Concurso", "Compra", "Subasta"]


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

    Scraper viejo:
        numero_proceso, expediente, nombre_proceso, tipo_proceso,
        fecha_apertura, estado, unidad_ejecutora,
        saf_nombre, organismo, saf_id, link, fuente

    Scraper nuevo (con columnas corridas):
        numero_proceso, expediente(=nombre real), nombre_proceso(=tipo),
        fecha_apertura, estado, area, saf, organismo
    """
    numero = c.get("numero_proceso", "").strip()

    # SAF: viejo usa saf_id, nuevo usa saf
    saf = (c.get("saf") or c.get("saf_id") or "").strip()

    # Nombre y tipo — detectar columnas corridas (scraper nuevo)
    nombre_raw = c.get("nombre_proceso", "")
    tipo_raw   = c.get("tipo_proceso", "")
    expediente = c.get("expediente", "")

    # En scraper nuevo: nombre_proceso tiene el tipo y expediente tiene el nombre real
    if any(nombre_raw.startswith(t) for t in TIPOS_PROCESO) and not tipo_raw:
        nombre = expediente
        tipo   = nombre_raw
    else:
        nombre = nombre_raw
        tipo   = tipo_raw

    # Area: nuevo la tiene explícita, viejo no
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

    return {
        "numero":    numero,
        "saf":       saf,
        "nombre":    nombre,
        "tipo":      tipo,
        "fecha":     parse_fecha(c.get("fecha_apertura")),
        "estado":    c.get("estado", ""),
        "unidad":    c.get("unidad_ejecutora", ""),
        "saf_texto": c.get("saf_nombre", c.get("saf_texto", "")),
        "area":      area,
        "organismo": c.get("organismo", ""),
    }


def load_contratos(dry_run=False):
    print(f"\n{'='*60}")
    print("📦 CARGA CONTRATOS → PostgreSQL")
    print(f"{'='*60}")
    print(f"  Fuente: {os.path.abspath(INPUT_JSON)}")
    print(f"  DB:     {DATABASE_URL}\n")

    if not os.path.exists(INPUT_JSON):
        print(f"  ❌ No se encontró {INPUT_JSON}")
        sys.exit(1)

    with open(INPUT_JSON, encoding="utf-8") as f:
        contratos = json.load(f)

    print(f"  Contratos en JSON: {len(contratos)}")

    normalizados = [normalizar(c) for c in contratos]

    por_area = Counter(n["area"] for n in normalizados)
    print(f"\n  Por área (normalizado):")
    for area, count in sorted(por_area.items(), key=lambda x: -x[1]):
        print(f"    {area:<22} {count:>5}")

    rows = []
    skipped = 0
    for n in normalizados:
        if not n["numero"] or not n["saf"]:
            skipped += 1
            continue
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
        ))

    print(f"\n  Filas válidas:  {len(rows)}")
    print(f"  Saltadas:       {skipped}")

    if dry_run:
        print("\n  [DRY RUN] No se realizaron inserciones.")
        return

    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False
        cur  = conn.cursor()
        print("\n  ✅ Conexión OK")
    except Exception as e:
        print(f"  ❌ Error de conexión: {e}")
        sys.exit(1)

    SQL = """
        INSERT INTO contratos (
            numero_proceso, nombre_proceso, tipo_proceso,
            fecha_apertura, estado,
            unidad_ejecutora, saf_texto,
            area, saf, organismo
        ) VALUES %s
        ON CONFLICT (numero_proceso, saf) DO NOTHING
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
            print(f"  ❌ Error en lote {i//BATCH + 1}: {e}")
            cur.close()
            conn.close()
            sys.exit(1)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM contratos")
    total_db = cur.fetchone()[0]

    cur.execute("""
        SELECT area, COUNT(*) as n
        FROM contratos
        GROUP BY area
        ORDER BY n DESC
    """)
    dist_db = cur.fetchall()

    cur.close()
    conn.close()

    print(f"\n  ✅ Carga completa")
    print(f"     Procesadas: {insertados}  |  En tabla: {total_db}")
    print(f"\n  Distribución en DB:")
    for area, n in dist_db:
        print(f"    {area:<22} {n:>5}")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_contratos(dry_run=args.dry_run)