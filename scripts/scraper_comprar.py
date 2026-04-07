"""
scraper_comprar.py
==================
Scraper diario de contratos del PEN para el Monitor de Transparencia.
Fuentes: BORA Sección 3ra + COMPR.AR + TGN Presupuesto Abierto.

Filtra y mapea resultados a los tres organismos del proyecto:
  - JGM       (Jefatura de Gabinete de Ministros)
  - SGP       (Secretaría General de la Presidencia)
  - Presidencia resto

Produce:
  data/adjudicaciones_YYYYMMDD.csv  — mismo formato que generar_json.py consume

Uso:
  python scripts/scraper_comprar.py              # corre todo, solo días hábiles
  python scripts/scraper_comprar.py --force      # ignora guardia fin de semana
  python scripts/scraper_comprar.py --fuente bora
  python scripts/scraper_comprar.py --fuente comprar
  python scripts/scraper_comprar.py --fuente tgn

Lógica de actualización:
  - Si ya existe un CSV del día, agrega filas nuevas (no duplica por nro_proceso+cuit)
  - Si no existe, crea uno nuevo
"""

import os
import re
import io
import sys
import time
import codecs
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Columnas de salida (mismo esquema que adjudicaciones_20260406.csv) ─────────
COLS_SALIDA = [
    "ejercicio", "organismo", "tipo_proceso", "cuit_proveedor",
    "monto_adjudicado", "moneda", "objeto", "fuente",
    "fecha_ingesta", "nro_proceso", "proveedor", "fecha_adjudicacion"
]

# ── Mapeo de organismos a ramas del proyecto ──────────────────────────────────
KEYWORDS_JGM = [
    "JEFATURA DE GABINETE", "JEFATURA GABINETE", "JGM",
    "OFICINA NACIONAL DE CONTRATACIONES", "SECRETARIA DE INNOVACION",
    "SECRETARIA DE TRANSFORMACION",
]
KEYWORDS_SGP = [
    "SECRETARIA GENERAL DE LA PRESIDENCIA", "SECRETARIA GENERAL PRESIDENCIA",
    "SGP", "SECRETARIA GENERAL DE PRESIDENCIA",
]
KEYWORDS_PRESIDENCIA = [
    "PRESIDENCIA DE LA NACION", "PRESIDENCIA DE LA REPÚBLICA",
    "SECRETARIA LEGAL Y TECNICA", "SECRETARIA DE CULTURA",
    "SIGEN", "SINDICATURA GENERAL", "CASA MILITAR",
    "SECRETARIA DE COMUNICACION", "AGENCIA FEDERAL DE INTELIGENCIA",
]

# Códigos SAF para filtrar TGN
CODIGOS_JGM         = ["305"]
CODIGOS_SGP         = ["301"]
CODIGOS_PRESIDENCIA = ["338", "337", "322", "303", "300", "302", "304",
                       "306", "307", "308", "309"]
TODOS_CODIGOS = CODIGOS_JGM + CODIGOS_SGP + CODIGOS_PRESIDENCIA

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "es-AR,es;q=0.9",
    "Connection": "keep-alive",
}

TGN_TOKEN = os.environ.get("TGN_TOKEN", "707cb8c8-83e6-4c4d-a202-3e49c14eda89")


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_con_reintentos(url, intentos=3, timeout=60, espera=15, verify=False):
    for i in range(intentos):
        try:
            print(f"  🔄 Intento {i+1}: {url[:80]}...")
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"  ⚠️  Error intento {i+1}: {e}")
            if i < intentos - 1:
                time.sleep(espera)
    raise Exception(f"❌ Fallaron todos los intentos: {url}")


def mapear_rama(organismo):
    """Determina a qué rama pertenece un organismo por texto libre."""
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


def es_organismo_pen(organismo):
    return mapear_rama(organismo) is not None


def extraer_cuit(texto):
    if not texto:
        return ""
    m = re.search(r'\b(\d{2}-\d{7,8}-\d{1})\b', texto)
    if m:
        return m.group(1)
    m = re.search(r'\b(20|23|24|27|30|33|34)\d{9}\b', texto)
    if m:
        return m.group(0)
    return ""


def extraer_monto(texto):
    if not texto:
        return None
    m = re.search(
        r'(?:MONTO TOTAL ADJUDICADO|TOTAL ADJUDICADO|IMPORTE ADJUDICADO|MONTO ADJUDICADO)'
        r'[^\$\d]*\$?\s*([\d\.,]+)',
        texto, re.IGNORECASE
    )
    if m:
        raw = m.group(1).strip().replace(".", "").replace(",", ".")
        try:
            return float(raw)
        except Exception:
            return None
    return None


def extraer_proveedor(texto):
    if not texto:
        return ""
    patrones = [
        r'PROVEEDOR ADJUDICADO[:\s]+([A-ZÁÉÍÓÚÑ][^\n\r]{3,80}?)(?:\s*[,\.]?\s*CUIT|\s*$)',
        r'ADJUDICATARIO[:\s]+([A-ZÁÉÍÓÚÑ][^\n\r]{3,80}?)(?:\s*[,\.]?\s*CUIT|\s*$)',
        r'adjudicada?\s+(?:la\s+firma\s+|a\s+la\s+firma\s+|a\s+)([A-ZÁÉÍÓÚÑ][^\n\r]{3,80}?)(?:\s*[,\.]?\s*CUIT|\s*[,\.])',
        r'firma\s+([A-ZÁÉÍÓÚÑ][^\n\r]{3,80}?)\s*[,\.]?\s*(?:CUIT|C\.U\.I\.T)',
    ]
    for patron in patrones:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            resultado = m.group(1).strip().rstrip(".,- ")
            if len(resultado) > 3:
                return resultado
    return ""


# ── SCRAPER 1: BORA ───────────────────────────────────────────────────────────

def obtener_texto_aviso_bora(aviso_id, fecha_pub):
    fecha_raw = fecha_pub.replace("-", "")
    urls = [
        f"https://www.boletinoficial.gob.ar/detalleAviso/tercera/{aviso_id}/{fecha_raw}",
        f"https://www.boletinoficial.gob.ar/pdf/aviso/tercera/{aviso_id}/{fecha_raw}",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20, verify=False)
            if r.status_code != 200:
                continue
            if "pdf" in r.headers.get("Content-Type", "").lower():
                try:
                    from pdfminer.high_level import extract_text as pdf_extract
                    texto = pdf_extract(io.BytesIO(r.content))
                    if texto and len(texto) > 30:
                        return texto
                except Exception:
                    pass
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for selector in [{"id": "cuerpoAviso"}, {"id": "textoAviso"},
                              {"class": "aviso-cuerpo"}, {"class": "texto-aviso"}]:
                div = soup.find("div", selector)
                if div:
                    texto = div.get_text(separator=" ", strip=True)
                    if len(texto) > 50:
                        return texto
            texto_completo = soup.get_text(separator=" ", strip=True)
            if any(kw in texto_completo.upper() for kw in ["CUIT", "ADJUDIC", "PROVEEDOR"]):
                return texto_completo
        except Exception as e:
            print(f"  ⚠️  URL texto falló ({url[:60]}): {e}")
    return ""


def scraper_bora():
    url = "https://www.boletinoficial.gob.ar/seccion/tercera"
    print("\n📰 BORA — Sección Tercera...")

    try:
        response = get_con_reintentos(url)
        soup = BeautifulSoup(response.text, "html.parser")

        avisos_adj = []
        categoria_actual = ""
        for elem in soup.find_all(["h5", "a"]):
            if elem.name == "h5":
                categoria_actual = elem.text.strip()
            elif elem.name == "a" and "/detalleAviso/tercera/" in elem.get("href", ""):
                if "ADJUDICACION" not in categoria_actual.upper():
                    continue
                href   = elem["href"]
                partes = href.strip("/").split("/")
                aviso_id  = partes[-2] if len(partes) >= 2 else ""
                fecha_raw = partes[-1] if len(partes) >= 1 else ""
                fecha_pub = (f"{fecha_raw[:4]}-{fecha_raw[4:6]}-{fecha_raw[6:]}"
                             if len(fecha_raw) == 8 else fecha_raw)

                lineas = [l.strip() for l in elem.text.strip().split("\n") if l.strip()]
                palabras_tipo = ["Licitación", "Contratación", "Concurso",
                                 "Adjudicación", "Subasta", "Compulsa"]
                tipo_proceso = ""
                lineas_org   = []
                for linea in lineas:
                    if any(p.lower() in linea.lower() for p in palabras_tipo):
                        tipo_proceso = linea
                    else:
                        lineas_org.append(linea)
                organismo = re.sub(r'\s*-\s*$', '', " ".join(lineas_org)).strip()

                if not es_organismo_pen(organismo):
                    continue

                avisos_adj.append({
                    "aviso_id":    aviso_id,
                    "fecha_pub":   fecha_pub,
                    "organismo":   organismo,
                    "tipo_proceso": tipo_proceso,
                })

        print(f"  📋 {len(avisos_adj)} adjudicaciones PEN en BORA")

        registros = []
        for av in avisos_adj:
            time.sleep(1)
            texto = obtener_texto_aviso_bora(av["aviso_id"], av["fecha_pub"])
            cuit  = extraer_cuit(texto)
            prov  = extraer_proveedor(texto)
            monto = extraer_monto(texto)
            rama  = mapear_rama(av["organismo"])

            registros.append({
                "ejercicio":          datetime.now().year,
                "organismo":          f"{rama} - {av['organismo']}",
                "tipo_proceso":       av["tipo_proceso"],
                "cuit_proveedor":     cuit,
                "monto_adjudicado":   monto,
                "moneda":             "Peso Argentino",
                "objeto":             texto[:200] if texto else "",
                "fuente":             "BORA Sección 3ra",
                "fecha_ingesta":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "nro_proceso":        av["aviso_id"],
                "proveedor":          prov,
                "fecha_adjudicacion": av["fecha_pub"],
            })
            estado = f"✅ CUIT:{cuit}" if cuit else "⚠️  sin CUIT"
            print(f"  {estado} | {rama} | {prov[:35] if prov else 'sin proveedor'}")

        print(f"  ✅ BORA: {len(registros)} registros PEN")
        return (pd.DataFrame(registros, columns=COLS_SALIDA)
                if registros else pd.DataFrame(columns=COLS_SALIDA))

    except Exception as e:
        print(f"  ❌ Error BORA: {e}")
        return pd.DataFrame(columns=COLS_SALIDA)


# ── SCRAPER 2: COMPR.AR ───────────────────────────────────────────────────────

def scraper_comprar():
    url = "https://comprar.gob.ar/Compras.aspx?qs=W1HXHGHtH10="
    print("\n🛒 COMPR.AR — procesos abiertos...")

    try:
        response = get_con_reintentos(url, timeout=60)
        soup  = BeautifulSoup(response.text, "html.parser")
        tabla = soup.find("table", {"id": "ctl00_CPH1_GridListaPliegosAperturaProxima"})

        if not tabla:
            print("  ⚠️  Tabla no encontrada en COMPR.AR")
            return pd.DataFrame(columns=COLS_SALIDA)

        registros = []
        for row in tabla.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            unidad         = cols[5].text.strip() if len(cols) > 5 else ""
            nombre_proceso = cols[1].text.strip()
            texto_filtro   = unidad + " " + nombre_proceso

            if not es_organismo_pen(texto_filtro):
                continue

            rama = mapear_rama(texto_filtro)
            link_tag = cols[0].find("a") or cols[1].find("a")
            href = link_tag["href"] if link_tag else ""

            registros.append({
                "ejercicio":          datetime.now().year,
                "organismo":          f"{rama} - {unidad}" if rama else unidad,
                "tipo_proceso":       cols[2].text.strip(),
                "cuit_proveedor":     "",
                "monto_adjudicado":   None,
                "moneda":             "Peso Argentino",
                "objeto":             nombre_proceso,
                "fuente":             "COMPR.AR",
                "fecha_ingesta":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "nro_proceso":        cols[0].text.strip(),
                "proveedor":          "",
                "fecha_adjudicacion": cols[3].text.strip(),
            })

        print(f"  ✅ COMPR.AR: {len(registros)} procesos PEN")
        return (pd.DataFrame(registros, columns=COLS_SALIDA)
                if registros else pd.DataFrame(columns=COLS_SALIDA))

    except Exception as e:
        print(f"  ❌ Error COMPR.AR: {e}")
        return pd.DataFrame(columns=COLS_SALIDA)


# ── SCRAPER 3: TGN ───────────────────────────────────────────────────────────

def scraper_tgn():
    anio = datetime.now().year
    print(f"\n💰 TGN — Presupuesto Abierto {anio}...")

    url = "https://www.presupuestoabierto.gob.ar/api/v1/credito"
    headers_api = {
        "Authorization": f"Bearer {TGN_TOKEN}",
        "Content-Type":  "application/json",
        "Accept":        "text/csv",
    }
    body = {
        "columns": [
            "ejercicio_presupuestario",
            "jurisdiccion_id",
            "jurisdiccion_desc",
            "entidad_desc",
            "unidad_ejecutora_desc",
            "credito_pagado",
            "credito_devengado",
        ]
    }

    try:
        r = requests.post(url, headers=headers_api, json=body, timeout=60, verify=False)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), sep=",", on_bad_lines="skip")

        if "ejercicio_presupuestario" in df.columns:
            df = df[df["ejercicio_presupuestario"] == anio].copy()

        if df.empty:
            print(f"  ⚠️  Sin datos TGN para {anio}")
            return pd.DataFrame(columns=COLS_SALIDA)

        # Filtrar por código de jurisdicción
        if "jurisdiccion_id" in df.columns:
            df = df[df["jurisdiccion_id"].astype(str).isin(TODOS_CODIGOS)].copy()

        if df.empty:
            print("  ⚠️  Sin datos TGN para los organismos PEN")
            return pd.DataFrame(columns=COLS_SALIDA)

        def rama_por_codigo(cod):
            c = str(cod)
            if c in CODIGOS_JGM:        return "JGM"
            if c in CODIGOS_SGP:        return "SGP"
            return "PRESIDENCIA"

        registros = []
        for _, row in df.iterrows():
            monto  = pd.to_numeric(row.get("credito_pagado", 0), errors="coerce") or 0
            rama   = rama_por_codigo(row.get("jurisdiccion_id", ""))
            entidad = str(row.get("entidad_desc", ""))
            unidad  = str(row.get("unidad_ejecutora_desc", ""))

            registros.append({
                "ejercicio":          anio,
                "organismo":          f"{rama} - {entidad} / {unidad}".strip(" -/"),
                "tipo_proceso":       "Ejecución Presupuestaria",
                "cuit_proveedor":     "",
                "monto_adjudicado":   float(monto),
                "moneda":             "Peso Argentino",
                "objeto":             f"Crédito pagado {anio}",
                "fuente":             f"TGN Presupuesto Abierto {anio}",
                "fecha_ingesta":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "nro_proceso":        "",
                "proveedor":          entidad,
                "fecha_adjudicacion": f"{anio}-01-01",
            })

        print(f"  ✅ TGN: {len(registros)} líneas presupuestarias PEN")
        return pd.DataFrame(registros, columns=COLS_SALIDA)

    except Exception as e:
        print(f"  ❌ Error TGN: {e}")
        return pd.DataFrame(columns=COLS_SALIDA)


# ── Guardar CSV ───────────────────────────────────────────────────────────────

def guardar_csv(df_nuevo):
    hoy        = datetime.now().strftime("%Y%m%d")
    csv_salida = os.path.join(DATA_DIR, f"adjudicaciones_{hoy}.csv")

    if os.path.exists(csv_salida):
        df_existente = pd.read_csv(csv_salida, encoding="utf-8-sig", low_memory=False)
        df_existente.columns = df_existente.columns.str.strip()
        df_combinado = pd.concat([df_existente, df_nuevo], ignore_index=True)
        df_combinado["_key"] = (
            df_combinado["nro_proceso"].astype(str).str.strip() + "|" +
            df_combinado["cuit_proveedor"].astype(str).str.strip()
        )
        df_combinado = df_combinado.drop_duplicates(subset="_key", keep="first")
        df_combinado = df_combinado.drop(columns=["_key"])
        nuevos = len(df_combinado) - len(df_existente)
        print(f"\n  📎 Registros nuevos agregados: {nuevos}")
    else:
        df_combinado = df_nuevo

    with codecs.open(csv_salida, "w", encoding="utf-8-sig") as f:
        df_combinado.to_csv(f, index=False)

    print(f"  💾 CSV: {csv_salida}  ({len(df_combinado):,} filas)")
    return csv_salida


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper PEN — BORA + COMPR.AR + TGN")
    parser.add_argument("--force",  action="store_true",
                        help="Ignora guardia fin de semana/feriados")
    parser.add_argument("--fuente", choices=["bora", "comprar", "tgn", "todas"],
                        default="todas", help="Fuente a scrapear (default: todas)")
    args = parser.parse_args()

    # Guardia días hábiles
    if not args.force:
        hoy = datetime.now()
        if hoy.weekday() >= 5:
            dia = "sábado" if hoy.weekday() == 5 else "domingo"
            print(f"⏭️  Hoy es {dia} — usá --force para forzar la ejecución.")
            sys.exit(0)
        try:
            import holidays
            feriados_ar = holidays.Argentina(years=hoy.year)
            if hoy.date() in feriados_ar:
                print(f"⏭️  Feriado: {feriados_ar.get(hoy.date())} — usá --force.")
                sys.exit(0)
        except ImportError:
            pass

    print("\n" + "="*60)
    print("SCRAPER PEN — BORA + COMPR.AR + TGN")
    print(f"Ejecutado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Fuente: {args.fuente.upper()}")
    print("="*60)

    dfs = []

    if args.fuente in ("bora", "todas"):
        df = scraper_bora()
        if not df.empty:
            dfs.append(df)

    if args.fuente in ("comprar", "todas"):
        df = scraper_comprar()
        if not df.empty:
            dfs.append(df)

    if args.fuente in ("tgn", "todas"):
        df = scraper_tgn()
        if not df.empty:
            dfs.append(df)

    if not dfs:
        print("\n⚠️  Ninguna fuente devolvió datos PEN.")
        sys.exit(0)

    df_final = pd.concat(dfs, ignore_index=True)[COLS_SALIDA]
    csv_path = guardar_csv(df_final)

    print("\n" + "="*60)
    print("RESUMEN")
    print("="*60)
    for fuente, grp in df_final.groupby("fuente"):
        print(f"  {fuente:<35}  {len(grp):>5} registros")
    for rama in ["JGM", "SGP", "PRESIDENCIA"]:
        n = df_final["organismo"].str.startswith(rama).sum()
        if n:
            print(f"  → {rama:<12}  {n:>5} registros")
    print(f"\nSiguiente: python scripts/generar_json.py")
    print("="*60)


if __name__ == "__main__":
    main()