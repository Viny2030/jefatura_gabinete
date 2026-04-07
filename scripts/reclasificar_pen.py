"""
reclasificar_pen.py
====================
Toma los reportes Excel producidos por diario.py y reclasifica
cada registro según la estructura orgánica del PEN:

  JGM         — Jefatura de Gabinete de Ministros y dependencias
  SGP         — Secretaría General de la Presidencia
  PRESIDENCIA — Presidencia de la Nación (resto)
  OTROS       — Todo lo demás (se descarta del CSV de salida)

Produce:
  data/adjudicaciones_YYYYMMDD.csv  (merge con el existente)

Uso:
  python scripts/reclasificar_pen.py                      # último reporte
  python scripts/reclasificar_pen.py --todo               # todos los históricos
  python scripts/reclasificar_pen.py --dias 60            # últimos 60 días
  python scripts/reclasificar_pen.py --listar             # muestra todos los
                                                          # organismos sin filtrar
  python scripts/reclasificar_pen.py --agregar ORGANISMO JGM
                                                          # agrega mapeo manual
"""

import os
import re
import glob
import codecs
import argparse
import unicodedata
import pandas as pd
from datetime import datetime, timedelta

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Ruta al repo hermano (ajustable con --repo)
REPO_DEFAULT = os.path.join(BASE, "..", "..", "monitor_contratos_v2")

# Archivo de mapeos manuales persistentes
MAPEOS_FILE = os.path.join(DATA_DIR, "mapeos_organismos.csv")

# ── Columnas de salida ────────────────────────────────────────────────────────
COLS_SALIDA = [
    "ejercicio", "organismo", "tipo_proceso", "cuit_proveedor",
    "monto_adjudicado", "moneda", "objeto", "fuente",
    "fecha_ingesta", "nro_proceso", "proveedor", "fecha_adjudicacion"
]

# ── Mapa de organismos conocidos de la APN ────────────────────────────────────
# Estructura real según decreto de estructura del PEN
# Formato: "TEXTO EN ORGANISMO" -> "RAMA"
# Se busca como substring en el nombre del organismo (uppercase)
MAPA_APN = {
    # ── PRESIDENCIA directa ───────────────────────────────────────────────────
    # Dependen directamente de Presidencia de la Nación
    "PRESIDENCIA DE LA NACION":                     "PRESIDENCIA",
    "SECRETARIA LEGAL Y TECNICA":                   "PRESIDENCIA",
    "SECRETARIA DE CULTURA":                        "PRESIDENCIA",
    "SINDICATURA GENERAL":                          "PRESIDENCIA",
    "SIGEN":                                        "PRESIDENCIA",
    "CASA MILITAR":                                 "PRESIDENCIA",
    "SECRETARIA DE INTELIGENCIA":                   "PRESIDENCIA",
    "AUTORIDAD REGULATORIA NUCLEAR":                "PRESIDENCIA",
    "TEATRO NACIONAL CERVANTES":                    "PRESIDENCIA",
    "INSTITUTO NACIONAL DEL TEATRO":                "PRESIDENCIA",
    "FONDO NACIONAL DE LAS ARTES":                  "PRESIDENCIA",
    "INSTITUTO NACIONAL DE CINE":                   "PRESIDENCIA",
    "INCAA":                                        "PRESIDENCIA",
    "BIBLIOTECA NACIONAL":                          "PRESIDENCIA",
    "BALLET NACIONAL":                              "PRESIDENCIA",
    "INSTITUTO NACIONAL DE LA MUSICA":              "PRESIDENCIA",

    # ── SGP ──────────────────────────────────────────────────────────────────
    # Secretaría General de la Presidencia y sus dependencias
    "SECRETARIA GENERAL DE LA PRESIDENCIA":         "SGP",
    "SECRETARIA GENERAL PRESIDENCIA":               "SGP",
    "SECRETARIA GENERAL":                           "SGP",

    # ── JGM — Jefatura de Gabinete coordina todos los ministerios ─────────────
    # Bajo la gestión Milei, JGM tiene coordinación sobre toda la APN
    # Incluye: ministerios, secretarías, organismos descentralizados,
    # empresas del Estado, entes reguladores y fuerzas de seguridad/defensa

    # Jefatura directa
    "JEFATURA DE GABINETE":                         "JGM",
    "OFICINA NACIONAL DE CONTRATACIONES":           "JGM",
    "AGENCIA DE ADMINISTRACION DE BIENES":          "JGM",
    "AGENCIA DE ACCESO A LA INFORMACION":           "JGM",
    "ENTE NACIONAL DE COMUNICACIONES":              "JGM",
    "ENACOM":                                       "JGM",
    "CORREO OFICIAL":                               "JGM",
    "ARSAT":                                        "JGM",
    "CONICET":                                      "JGM",
    "CONAE":                                        "JGM",
    "ADMINISTRACION DE PARQUES NACIONALES":         "JGM",
    "INPROTUR":                                     "JGM",
    "RADIO Y TELEVISION ARGENTINA":                 "JGM",

    # Ministerios (todos coordinados por JGM)
    "MINISTERIO DE ECONOMIA":                       "JGM",
    "MINISTERIO DEL INTERIOR":                      "JGM",
    "MINISTERIO DE DEFENSA":                        "JGM",
    "MINISTERIO DE SEGURIDAD":                      "JGM",
    "MINISTERIO DE JUSTICIA":                       "JGM",
    "MINISTERIO DE SALUD":                          "JGM",
    "MINISTERIO DE CAPITAL HUMANO":                 "JGM",
    "MINISTERIO DE RELACIONES EXTERIORES":          "JGM",
    "MINISTERIO DE DESREGULACION":                  "JGM",

    # Organismos bajo Ministerio de Economía
    "AGENCIA DE RECAUDACION Y CONTROL ADUANERO":    "JGM",
    "AFIP":                                         "JGM",
    "BANCO DE LA NACION ARGENTINA":                 "JGM",
    "BANCO CENTRAL":                                "JGM",
    "BCRA":                                         "JGM",
    "COMISION NACIONAL DE VALORES":                 "JGM",
    "INSTITUTO NACIONAL DE TECNOLOGIA AGROPECUARIA": "JGM",
    "INTA":                                         "JGM",
    "COMISION NACIONAL DE ENERGIA ATOMICA":         "JGM",
    "CNEA":                                         "JGM",
    "NUCLEOELECTRICA ARGENTINA":                    "JGM",
    "EMPRESA ARGENTINA DE NAVEGACION AEREA":        "JGM",
    "EANA":                                         "JGM",
    "COMISION NACIONAL DE REGULACION DEL TRANSPORTE": "JGM",
    "CNRT":                                         "JGM",
    "VIALIDAD NACIONAL":                            "JGM",
    "AGENCIA NACIONAL DE SEGURIDAD VIAL":           "JGM",
    "YPF":                                          "JGM",
    "AEROPUERTOS ARGENTINA":                        "JGM",
    "AEROLINEAS ARGENTINAS":                        "JGM",

    # Organismos bajo Ministerio de Seguridad
    "POLICIA FEDERAL ARGENTINA":                    "JGM",
    "GENDARMERIA NACIONAL":                         "JGM",
    "PREFECTURA NAVAL":                             "JGM",
    "POLICIA DE SEGURIDAD AEROPORTUARIA":           "JGM",
    "SERVICIO PENITENCIARIO FEDERAL":               "JGM",

    # Organismos bajo Ministerio de Defensa
    "ARMADA ARGENTINA":                             "JGM",
    "EJERCITO ARGENTINO":                           "JGM",
    "FUERZA AEREA ARGENTINA":                       "JGM",
    "ESTADO MAYOR":                                 "JGM",
    "INSTITUTO DE INVESTIGACIONES CIENTIFICAS Y TECNICAS PARA LA DEFENSA": "JGM",
    "CITEFA":                                       "JGM",

    # Organismos bajo Ministerio de Capital Humano
    "ADMINISTRACION NACIONAL DE LA SEGURIDAD SOCIAL": "JGM",
    "ANSES":                                        "JGM",
    "ADMINISTRACION NACIONAL DE ESTABLECIMIENTOS DE SALUD": "JGM",
    "ANES":                                         "JGM",
    "UNIVERSIDAD NACIONAL":                         "JGM",

    # Organismos bajo Ministerio de Salud
    "ADMINISTRACION NACIONAL DE MEDICAMENTOS":      "JGM",
    "ANMAT":                                        "JGM",
    "SUPERINTENDENCIA DE SERVICIOS DE SALUD":       "JGM",

    # Organismos bajo Ministerio del Interior
    "REGISTRO NACIONAL DE LAS PERSONAS":            "JGM",
    "RENAPER":                                      "JGM",
    "DIRECCION NACIONAL DE MIGRACIONES":            "JGM",

    # Auditoría — órgano de control del PEN (LAF)
    "AUDITORIA GENERAL DE LA NACION":               "JGM",
}


def normalizar(texto):
    """Uppercase sin tildes para comparación robusta."""
    if not isinstance(texto, str):
        return ""
    t = texto.upper().strip()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def cargar_mapeos_manuales():
    """Carga mapeos manuales del archivo CSV persistente."""
    if not os.path.exists(MAPEOS_FILE):
        return {}
    try:
        df = pd.read_csv(MAPEOS_FILE, encoding="utf-8-sig")
        return dict(zip(df["organismo"].apply(normalizar), df["rama"]))
    except Exception:
        return {}


def guardar_mapeo_manual(organismo, rama):
    """Agrega un mapeo manual al archivo persistente."""
    rama = rama.upper().strip()
    if rama not in ("JGM", "SGP", "PRESIDENCIA"):
        print(f"❌ Rama inválida: {rama}. Debe ser JGM, SGP o PRESIDENCIA.")
        return
    nuevo = pd.DataFrame([{"organismo": organismo, "rama": rama}])
    if os.path.exists(MAPEOS_FILE):
        df = pd.read_csv(MAPEOS_FILE, encoding="utf-8-sig")
        # Evitar duplicado
        df = df[df["organismo"].apply(normalizar) != normalizar(organismo)]
        df = pd.concat([df, nuevo], ignore_index=True)
    else:
        df = nuevo
    with codecs.open(MAPEOS_FILE, "w", encoding="utf-8-sig") as f:
        df.to_csv(f, index=False)
    print(f"✅ Mapeo guardado: '{organismo}' → {rama}")


def clasificar_organismo(organismo):
    """
    Retorna la rama (JGM/SGP/PRESIDENCIA) o None si no pertenece al PEN.
    Prioridad: mapeos manuales > MAPA_APN > None
    """
    org_norm = normalizar(organismo)

    # 1. Mapeos manuales (mayor prioridad)
    mapeos_manuales = cargar_mapeos_manuales()
    for key, rama in mapeos_manuales.items():
        if key in org_norm:
            return rama

    # 2. Mapa APN estático
    for key, rama in MAPA_APN.items():
        if normalizar(key) in org_norm:
            return rama

    return None


# ── Leer reportes ─────────────────────────────────────────────────────────────

def leer_reporte(path):
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception as e:
        print(f"  ⚠️  No se pudo abrir {os.path.basename(path)}: {e}")
        return pd.DataFrame()

    for hoja in ["🚨 Flujo Completo", "🏆 Adjudicaciones"]:
        if hoja in xl.sheet_names:
            try:
                return pd.read_excel(xl, sheet_name=hoja, engine="openpyxl")
            except Exception as e:
                print(f"  ⚠️  Error leyendo {hoja}: {e}")
    return pd.DataFrame()


def normalizar_monto(valor):
    if pd.isna(valor):
        return None
    try:
        return float(
            str(valor).replace("$", "").replace(".", "").replace(",", ".").strip()
        )
    except Exception:
        return None


def convertir_a_csv(df_raw, fecha_reporte):
    """Convierte un DataFrame de reporte al esquema de adjudicaciones CSV."""
    registros = []
    for _, row in df_raw.iterrows():
        organismo = str(row.get("organismo_contratante", "")).strip()
        rama = clasificar_organismo(organismo)
        if rama is None:
            continue

        # Fecha
        fecha_adj = row.get("fecha", fecha_reporte)
        try:
            fecha_adj = pd.to_datetime(fecha_adj).strftime("%Y-%m-%d")
        except Exception:
            fecha_adj = str(fecha_reporte)

        # Monto
        monto = normalizar_monto(
            row.get("monto_adjudicado_bora", row.get("monto_adjudicado"))
        )

        # Objeto — link BORA + nivel riesgo
        link  = str(row.get("link_bora", row.get("link", ""))).strip()
        riesgo = str(row.get("nivel_riesgo_licit", "")).strip()
        objeto = f"[{riesgo}] {link}" if riesgo and riesgo != "nan" else link

        # CUIT limpio
        cuit = str(row.get("cuit_proveedor", "")).strip()
        cuit = "" if cuit == "nan" else cuit

        # Proveedor
        prov = str(row.get("proveedor_adjudicado", row.get("proveedor", ""))).strip()
        prov = "" if prov == "nan" else prov

        # Nro proceso
        nro = str(row.get("nro_proceso_comprar", row.get("aviso_id", ""))).strip()
        nro = "" if nro == "nan" else nro

        registros.append({
            "ejercicio":          int(str(fecha_reporte)[:4]),
            "organismo":          f"{rama} - {organismo}",
            "tipo_proceso":       str(row.get("tipo_proceso_bora", row.get("tipo_proceso", ""))).strip(),
            "cuit_proveedor":     cuit,
            "monto_adjudicado":   monto,
            "moneda":             "Peso Argentino",
            "objeto":             objeto[:300],
            "fuente":             "BORA via monitor_contratos_v2",
            "fecha_ingesta":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "nro_proceso":        nro,
            "proveedor":          prov,
            "fecha_adjudicacion": fecha_adj,
        })

    return pd.DataFrame(registros, columns=COLS_SALIDA)


# ── Guardar CSV ───────────────────────────────────────────────────────────────

def guardar_csv(df_nuevo):
    hoy        = datetime.now().strftime("%Y%m%d")
    csv_salida = os.path.join(DATA_DIR, f"adjudicaciones_{hoy}.csv")

    if os.path.exists(csv_salida):
        df_existente = pd.read_csv(csv_salida, encoding="utf-8-sig", low_memory=False)
        df_existente.columns = df_existente.columns.str.strip()
        df_combinado = pd.concat([df_existente, df_nuevo], ignore_index=True)
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

    print(f"  💾 {csv_salida}  ({len(df_combinado):,} filas totales)")
    return csv_salida


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Reclasifica salidas de monitor_contratos_v2 por JGM/SGP/Presidencia"
    )
    parser.add_argument("--repo",  default=REPO_DEFAULT,
                        help="Ruta al repo monitor_contratos_v2")
    parser.add_argument("--todo",  action="store_true",
                        help="Procesar todos los reportes históricos")
    parser.add_argument("--dias",  type=int, default=None,
                        help="Procesar reportes de los últimos N días")
    parser.add_argument("--listar", action="store_true",
                        help="Lista todos los organismos sin filtrar (para mapeo manual)")
    parser.add_argument("--agregar", nargs=2, metavar=("ORGANISMO", "RAMA"),
                        help="Agrega mapeo manual: --agregar 'NOMBRE ORGANISMO' JGM")
    args = parser.parse_args()

    # ── Agregar mapeo manual ──────────────────────────────────────────────────
    if args.agregar:
        guardar_mapeo_manual(args.agregar[0], args.agregar[1])
        return

    repo = os.path.abspath(args.repo)
    if not os.path.isdir(repo):
        print(f"❌ Repo no encontrado: {repo}")
        return

    patron   = os.path.join(repo, "data", "**", "reporte_*.xlsx")
    archivos = sorted(glob.glob(patron, recursive=True))

    # ── Listar todos los organismos ───────────────────────────────────────────
    if args.listar:
        print(f"\n📋 Organismos en {len(archivos)} reportes:\n")
        todos = set()
        for a in archivos:
            df = leer_reporte(a)
            if not df.empty and "organismo_contratante" in df.columns:
                todos.update(df["organismo_contratante"].dropna().unique().tolist())
        for org in sorted(todos):
            rama = clasificar_organismo(org)
            marca = f"→ {rama}" if rama else "  (sin clasificar)"
            print(f"  {marca:<15}  {org}")
        print(f"\nTotal: {len(todos)} organismos únicos")
        print(f"Sin clasificar: {sum(1 for o in todos if clasificar_organismo(o) is None)}")
        print(f"\nPara agregar un mapeo:")
        print(f"  python scripts/reclasificar_pen.py --agregar 'NOMBRE ORGANISMO' JGM")
        return

    # ── Seleccionar archivos ──────────────────────────────────────────────────
    print("\n" + "="*60)
    print("RECLASIFICADOR PEN — monitor_contratos_v2 → jefatura_gabinete1")
    print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)
    print(f"\n[1] Reportes disponibles: {len(archivos)}")

    if not args.todo and args.dias is None:
        archivos = [archivos[-1]]
        print(f"    Procesando: {os.path.basename(archivos[0])}")
    elif args.dias:
        corte = datetime.now() - timedelta(days=args.dias)
        archivos = [
            a for a in archivos
            if _fecha_archivo(a) and _fecha_archivo(a) >= corte
        ]
        print(f"    Últimos {args.dias} días: {len(archivos)} reportes")
    else:
        print(f"    Procesando todos: {len(archivos)} reportes")

    # ── Procesar ──────────────────────────────────────────────────────────────
    print("\n[2] Procesando...")
    dfs  = []
    stats = {"jgm": 0, "sgp": 0, "presidencia": 0, "otros": 0, "archivos": 0}

    for path in archivos:
        nombre = os.path.basename(path)
        fecha_reporte = _fecha_str(path)
        df_raw  = leer_reporte(path)
        if df_raw.empty:
            continue

        df_norm = convertir_a_csv(df_raw, fecha_reporte)
        total_raw = len(df_raw)
        otros     = total_raw - len(df_norm)

        stats["archivos"]    += 1
        stats["jgm"]         += df_norm["organismo"].str.startswith("JGM").sum()
        stats["sgp"]         += df_norm["organismo"].str.startswith("SGP").sum()
        stats["presidencia"] += df_norm["organismo"].str.startswith("PRESIDENCIA").sum()
        stats["otros"]       += otros

        if not df_norm.empty:
            dfs.append(df_norm)
            print(f"  ✓ {nombre}: {len(df_norm)}/{total_raw} PEN "
                  f"(JGM:{df_norm['organismo'].str.startswith('JGM').sum()} "
                  f"SGP:{df_norm['organismo'].str.startswith('SGP').sum()} "
                  f"PRES:{df_norm['organismo'].str.startswith('PRESIDENCIA').sum()})")
        else:
            print(f"  · {nombre}: 0/{total_raw} PEN — ninguno clasificado")

    if not dfs:
        print("\n⚠️  Ningún registro clasificado como JGM/SGP/Presidencia.")
        print("   Corré con --listar para ver todos los organismos disponibles.")
        print("   Usá --agregar para mapear organismos manualmente.")
        return

    df_final = pd.concat(dfs, ignore_index=True)[COLS_SALIDA]

    print("\n[3] Guardando...")
    csv_path = guardar_csv(df_final)

    # ── Resumen ───────────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    print(f"  Archivos procesados:   {stats['archivos']}")
    print(f"  Registros PEN:         {len(df_final):,}")
    print(f"  → JGM:                 {stats['jgm']:,}")
    print(f"  → SGP:                 {stats['sgp']:,}")
    print(f"  → Presidencia:         {stats['presidencia']:,}")
    print(f"  Descartados (otros):   {stats['otros']:,}")

    if stats["jgm"] + stats["sgp"] + stats["presidencia"] == 0:
        print("\n  💡 Tip: corré --listar para ver organismos disponibles")
        print("         y --agregar para mapear los que correspondan al PEN")

    print(f"\nSiguiente: python scripts/generar_json.py")
    print("="*60)


def _fecha_str(path):
    nombre = os.path.basename(path)
    try:
        return nombre.replace("reporte_", "").replace(".xlsx", "")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def _fecha_archivo(path):
    try:
        fecha_str = _fecha_str(path)
        return datetime.strptime(fecha_str, "%Y-%m-%d")
    except Exception:
        return None


if __name__ == "__main__":
    main()