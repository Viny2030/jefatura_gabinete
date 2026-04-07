"""
importar_monitor_contratos.py
==============================
Lee los reportes Excel producidos por monitor_contratos_v2
y los subclasifica por JGM / SGP / Presidencia resto.

Produce:
  data/adjudicaciones_YYYYMMDD.csv  — mismo formato que generar_json.py consume

Uso:
  python scripts/importar_monitor_contratos.py
  python scripts/importar_monitor_contratos.py --repo C:/ruta/monitor_contratos_v2
  python scripts/importar_monitor_contratos.py --dias 30   (últimos 30 días)
  python scripts/importar_monitor_contratos.py --todo      (todos los reportes)

Sin --todo procesa solo el reporte más reciente.
"""

import os
import glob
import codecs
import argparse
import pandas as pd
from datetime import datetime, timedelta

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE         = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(BASE, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta por defecto al repo hermano
REPO_DEFAULT = os.path.join(BASE, "..", "..", "monitor_contratos_v2")

# ── Columnas de salida ────────────────────────────────────────────────────────
COLS_SALIDA = [
    "ejercicio", "organismo", "tipo_proceso", "cuit_proveedor",
    "monto_adjudicado", "moneda", "objeto", "fuente",
    "fecha_ingesta", "nro_proceso", "proveedor", "fecha_adjudicacion"
]

# ── Keywords para mapear organismo a rama ────────────────────────────────────
KEYWORDS_JGM = [
    "JEFATURA DE GABINETE", "JEFATURA GABINETE", "JGM",
    "OFICINA NACIONAL DE CONTRATACIONES",
    "SECRETARIA DE INNOVACION", "SECRETARIA DE TRANSFORMACION",
    "SECRETARIA DE MODERNIZACION",
]
KEYWORDS_SGP = [
    "SECRETARIA GENERAL DE LA PRESIDENCIA",
    "SECRETARIA GENERAL PRESIDENCIA",
    "SECRETARIA GENERAL DE PRESIDENCIA",
    "SGP",
]
KEYWORDS_PRESIDENCIA = [
    "PRESIDENCIA DE LA NACION", "PRESIDENCIA DE LA REPÚBLICA",
    "PRESIDENCIA DE LA NACION ARGENTINA",
    "SECRETARIA LEGAL Y TECNICA", "SECRETARIA DE CULTURA",
    "SIGEN", "SINDICATURA GENERAL",
    "CASA MILITAR", "SECRETARIA DE COMUNICACION",
    "AGENCIA FEDERAL DE INTELIGENCIA", "AFI",
    "SECRETARIA DE ASUNTOS ESTRATEGICOS",
    "OFICINA DEL PRESIDENTE",
]

def mapear_rama(organismo):
    org = str(organismo).upper().strip()
    for kw in KEYWORDS_JGM:
        if kw in org:
            return "JGM"
    for kw in KEYWORDS_SGP:
        if kw in org:
            return "SGP"
    for kw in KEYWORDS_PRESIDENCIA:
        if kw in org:
            return "PRESIDENCIA"
    return None

# ── Leer un reporte Excel ─────────────────────────────────────────────────────
def leer_reporte(path):
    """
    Lee las hojas relevantes de un reporte_YYYY-MM-DD.xlsx.
    Retorna DataFrame con registros de adjudicaciones.
    """
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception as e:
        print(f"  ⚠️  No se pudo abrir {os.path.basename(path)}: {e}")
        return pd.DataFrame()

    # Prioridad de hojas: Flujo Completo > Adjudicaciones
    hojas_candidatas = ["🚨 Flujo Completo", "🏆 Adjudicaciones"]
    df = pd.DataFrame()
    for hoja in hojas_candidatas:
        if hoja in xl.sheet_names:
            try:
                df = pd.read_excel(xl, sheet_name=hoja, engine="openpyxl")
                break
            except Exception as e:
                print(f"  ⚠️  Error leyendo hoja {hoja}: {e}")

    return df

# ── Normalizar a esquema de salida ────────────────────────────────────────────
def normalizar(df, fecha_reporte):
    """
    Mapea columnas del reporte monitor_contratos_v2
    al esquema de adjudicaciones_YYYYMMDD.csv.
    """
    if df.empty:
        return pd.DataFrame(columns=COLS_SALIDA)

    registros = []
    for _, row in df.iterrows():
        organismo = str(row.get("organismo_contratante", "")).strip()
        rama = mapear_rama(organismo)

        if rama is None:
            continue  # No es JGM/SGP/Presidencia

        # Monto — viene como string "$1.234.567" o float
        monto_raw = row.get("monto_adjudicado_bora", row.get("monto_adjudicado", None))
        if pd.notna(monto_raw):
            try:
                monto = float(str(monto_raw).replace("$", "").replace(".", "").replace(",", ".").strip())
            except Exception:
                monto = None
        else:
            monto = None

        # Fecha adjudicación
        fecha_adj = row.get("fecha", fecha_reporte)
        if pd.notna(fecha_adj):
            try:
                fecha_adj = pd.to_datetime(fecha_adj).strftime("%Y-%m-%d")
            except Exception:
                fecha_adj = str(fecha_reporte)
        else:
            fecha_adj = str(fecha_reporte)

        # Objeto — usar link como referencia si no hay texto
        objeto = str(row.get("link_bora", row.get("link", ""))).strip()

        # Nivel de riesgo como campo adicional en objeto
        nivel_riesgo = str(row.get("nivel_riesgo_licit", "")).strip()
        if nivel_riesgo and nivel_riesgo != "nan":
            objeto = f"[Riesgo: {nivel_riesgo}] {objeto}"

        registros.append({
            "ejercicio":          int(str(fecha_reporte)[:4]),
            "organismo":          f"{rama} - {organismo}",
            "tipo_proceso":       str(row.get("tipo_proceso_bora", row.get("tipo_proceso", ""))).strip(),
            "cuit_proveedor":     str(row.get("cuit_proveedor", "")).strip().replace("nan", ""),
            "monto_adjudicado":   monto,
            "moneda":             "Peso Argentino",
            "objeto":             objeto[:300],
            "fuente":             "BORA via monitor_contratos_v2",
            "fecha_ingesta":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "nro_proceso":        str(row.get("nro_proceso_comprar", row.get("aviso_id", ""))).strip().replace("nan", ""),
            "proveedor":          str(row.get("proveedor_adjudicado", row.get("proveedor", ""))).strip().replace("nan", ""),
            "fecha_adjudicacion": fecha_adj,
        })

    return pd.DataFrame(registros, columns=COLS_SALIDA)

# ── Guardar CSV ───────────────────────────────────────────────────────────────
def guardar_csv(df_nuevo, modo="merge"):
    hoy        = datetime.now().strftime("%Y%m%d")
    csv_salida = os.path.join(DATA_DIR, f"adjudicaciones_{hoy}.csv")

    if modo == "merge" and os.path.exists(csv_salida):
        df_existente = pd.read_csv(csv_salida, encoding="utf-8-sig", low_memory=False)
        df_existente.columns = df_existente.columns.str.strip()
        df_combinado = pd.concat([df_existente, df_nuevo], ignore_index=True)
        # Deduplicar por organismo + fecha + proveedor
        df_combinado["_key"] = (
            df_combinado["organismo"].astype(str) + "|" +
            df_combinado["fecha_adjudicacion"].astype(str) + "|" +
            df_combinado["proveedor"].astype(str)
        )
        antes = len(df_combinado)
        df_combinado = df_combinado.drop_duplicates(subset="_key", keep="first")
        df_combinado = df_combinado.drop(columns=["_key"])
        nuevos = len(df_combinado) - len(df_existente)
        print(f"  📎 Registros nuevos: {nuevos}  (deduplicados: {antes - len(df_combinado)})")
    else:
        df_combinado = df_nuevo

    with codecs.open(csv_salida, "w", encoding="utf-8-sig") as f:
        df_combinado.to_csv(f, index=False)

    print(f"  💾 CSV: {csv_salida}  ({len(df_combinado):,} filas totales)")
    return csv_salida

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Importar salidas de monitor_contratos_v2 → jefatura_gabinete1"
    )
    parser.add_argument("--repo", default=REPO_DEFAULT,
                        help="Ruta al repo monitor_contratos_v2")
    parser.add_argument("--dias", type=int, default=None,
                        help="Procesar reportes de los últimos N días")
    parser.add_argument("--todo", action="store_true",
                        help="Procesar todos los reportes históricos")
    parser.add_argument("--reemplazar", action="store_true",
                        help="Reemplaza el CSV existente en lugar de hacer merge")
    args = parser.parse_args()

    repo = os.path.abspath(args.repo)
    if not os.path.isdir(repo):
        print(f"❌ No se encontró el repo en: {repo}")
        print("   Usá --repo para especificar la ruta correcta.")
        return

    print("\n" + "="*60)
    print("IMPORTADOR monitor_contratos_v2 → jefatura_gabinete1")
    print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Repo fuente: {repo}")
    print("="*60)

    # Buscar reportes
    patron   = os.path.join(repo, "data", "**", "reporte_*.xlsx")
    archivos = sorted(glob.glob(patron, recursive=True))
    print(f"\n[1] Reportes encontrados: {len(archivos)}")

    if not archivos:
        print("❌ No se encontraron reportes. Verificá la ruta del repo.")
        return

    # Filtrar por rango de fechas
    if not args.todo and args.dias is None:
        # Por defecto: solo el más reciente
        archivos = [archivos[-1]]
        print(f"    Modo: solo último reporte ({os.path.basename(archivos[0])})")
    elif args.dias:
        corte = datetime.now() - timedelta(days=args.dias)
        archivos_filtrados = []
        for a in archivos:
            nombre = os.path.basename(a)
            try:
                fecha_str = nombre.replace("reporte_", "").replace(".xlsx", "")
                fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
                if fecha >= corte:
                    archivos_filtrados.append(a)
            except Exception:
                pass
        archivos = archivos_filtrados
        print(f"    Modo: últimos {args.dias} días ({len(archivos)} reportes)")
    else:
        print(f"    Modo: todos los reportes ({len(archivos)} archivos)")

    # Procesar reportes
    print("\n[2] Procesando...")
    dfs = []
    stats = {"jgm": 0, "sgp": 0, "presidencia": 0, "descartados": 0, "archivos": 0}

    for path in archivos:
        nombre = os.path.basename(path)
        fecha_str = nombre.replace("reporte_", "").replace(".xlsx", "")
        try:
            fecha_reporte = datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%Y-%m-%d")
        except Exception:
            fecha_reporte = datetime.now().strftime("%Y-%m-%d")

        df_raw = leer_reporte(path)
        if df_raw.empty:
            continue

        df_norm = normalizar(df_raw, fecha_reporte)
        if df_norm.empty:
            stats["descartados"] += len(df_raw)
            continue

        stats["archivos"] += 1
        stats["jgm"]         += df_norm["organismo"].str.startswith("JGM").sum()
        stats["sgp"]         += df_norm["organismo"].str.startswith("SGP").sum()
        stats["presidencia"] += df_norm["organismo"].str.startswith("PRESIDENCIA").sum()
        stats["descartados"] += len(df_raw) - len(df_norm)

        dfs.append(df_norm)
        print(f"  ✓ {nombre}: {len(df_norm)} registros PEN "
              f"(JGM:{df_norm['organismo'].str.startswith('JGM').sum()} "
              f"SGP:{df_norm['organismo'].str.startswith('SGP').sum()} "
              f"PRES:{df_norm['organismo'].str.startswith('PRESIDENCIA').sum()})")

    if not dfs:
        print("\n⚠️  Ningún reporte contenía registros JGM/SGP/Presidencia.")
        print("   Revisá los keywords de mapeo o el contenido de los reportes.")
        return

    df_final = pd.concat(dfs, ignore_index=True)[COLS_SALIDA]

    # Guardar
    print("\n[3] Guardando...")
    modo = "reemplazar" if args.reemplazar else "merge"
    csv_path = guardar_csv(df_final, modo=modo)

    # Resumen
    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    print(f"  Archivos procesados:  {stats['archivos']}")
    print(f"  Registros PEN:        {len(df_final):,}")
    print(f"  → JGM:                {stats['jgm']:,}")
    print(f"  → SGP:                {stats['sgp']:,}")
    print(f"  → Presidencia:        {stats['presidencia']:,}")
    print(f"  Descartados (otros):  {stats['descartados']:,}")
    print(f"\nSiguiente: python scripts/generar_json.py")
    print("="*60)

if __name__ == "__main__":
    main()