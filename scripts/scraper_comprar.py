#!/usr/bin/env python3
"""
scraper_comprar.py — scraper de producción para comprar.gob.ar
Extrae todos los procesos de compra de la JGM (SAF 591) con paginación completa.

Columnas: Número proceso | Expediente | Nombre proceso | Tipo de Proceso
          Fecha de apertura | Estado | Unidad Ejecutora | SAF

Uso:
    python scripts/scraper_comprar.py               # scraping completo
    python scripts/scraper_comprar.py --db          # guarda en PostgreSQL
    python scripts/scraper_comprar.py --csv         # guarda en CSV
    python scripts/scraper_comprar.py --db --csv    # ambos
"""
import re
import os
import csv
import sys
import math
import time
import argparse
import warnings
from datetime import datetime

import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

# ── Configuración ─────────────────────────────────────────────────────────────
BASE_URL         = "https://comprar.gob.ar/BuscarAvanzado.aspx"
SAF_ID           = "591"        # Jefatura de Gabinete de Ministros
FECHA_DESDE      = "01/01/2023"
FILAS_POR_PAGINA = 10          # filas reales por página del GridView
DELAY_SEG        = 1.5

HEADERS_GET = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9",
}

HEADERS_AJAX = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "es-AR,es;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "X-MicrosoftAjax": "Delta=true",
    "Cache-Control": "no-cache",
    "Referer": BASE_URL,
    "Origin": "https://comprar.gob.ar",
}

COLUMNAS_CSV = [
    "numero_proceso", "expediente", "nombre_proceso",
    "tipo_proceso", "fecha_apertura", "estado",
    "unidad_ejecutora", "saf", "scraped_at",
]


# ── ViewState ─────────────────────────────────────────────────────────────────
def extraer_viewstate(soup):
    def v(id_):
        el = soup.find("input", {"id": id_})
        return el["value"] if el else ""
    dxs = soup.find("input", {"name": "DXScript"})
    return {
        "__VIEWSTATE":          v("__VIEWSTATE"),
        "__EVENTVALIDATION":    v("__EVENTVALIDATION"),
        "__VIEWSTATEGENERATOR": v("__VIEWSTATEGENERATOR"),
        "DXScript":             dxs["value"] if dxs else "1_103,1_105,2_13,2_12,2_7,1_96,1_100,1_83,2_6",
    }


def actualizar_viewstate(texto_crudo, vs):
    for campo in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
        m = re.search(rf'\d+\|hiddenField\|{re.escape(campo)}\|(.*?)\|', texto_crudo)
        if m:
            vs[campo] = m.group(1)


def extraer_panel(texto_crudo, panel_id):
    pattern = rf'\d+\|updatePanel\|{re.escape(panel_id)}\|(.*?)(?=\d+\|(?:updatePanel|hiddenField|arrayDeclaration|scriptBlock|pageTitle|focus|error|asyncPostBack)\|)'
    m = re.search(pattern, texto_crudo, re.DOTALL)
    if m:
        return m.group(1)
    m2 = re.search(rf'\d+\|updatePanel\|{re.escape(panel_id)}\|(.*)', texto_crudo, re.DOTALL)
    return m2.group(1).rstrip("|") if m2 else ""


# ── Payload base ──────────────────────────────────────────────────────────────
def payload_base(vs, saf_id, fecha_desde, fecha_hasta, fecha_hasta_ts):
    dt_desde = datetime.strptime(fecha_desde, "%d/%m/%Y")
    ts_desde = str(int(dt_desde.timestamp() * 1000))
    return {
        "DXScript":                                                   vs["DXScript"],
        "__EVENTTARGET":                                              "",
        "__EVENTARGUMENT":                                            "",
        "__LASTFOCUS":                                                "",
        "__VIEWSTATE":                                                vs["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR":                                       vs["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":                                          vs["__EVENTVALIDATION"],
        "ctl00$CtrlMenuPortal$logIn$txtUsername$txtTextBox":          "",
        "ctl00$CtrlMenuPortal$logIn$textBoxUserRecuperoContrasenia":  "",
        "ctl00$CtrlMenuPortal$logIn$txtMail$txtTextBox":              "",
        "ctl00$CtrlMenuPortal$logIn$txtMail2$txtTextBox":             "",
        "ctl00$CPH1$txtNumeroProceso":                                "",
        "ctl00$CPH1$txtExpediente":                                   "",
        "ctl00$CPH1$txtNombrePliego":                                 "",
        "ctl00$CPH1$ddlJurisdicion":                                  saf_id,
        "ctl00$CPH1$ddlUnidadEjecutora":                              "-2",
        "ctl00$CPH1$ddlTipoProceso":                                  "-2",
        "ctl00$CPH1$ddlEstadoProceso":                                "-2",
        "ctl00$CPH1$ddlRubro":                                        "-2",
        "ctl00$CPH1$devCbPnlNombreProveedor$txtNombreProveedor":      "",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw":                 ts_desde,
        "ctl00$CPH1$devDteEdtFechaAperturaDesde":                     fecha_desde,
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_DDDWS":               "0:0:12000:30:1584:0:0:0",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_DDD_C_FNPWS":         "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde$DDD$C":               f"{fecha_desde}:{fecha_desde}",
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw":                 fecha_hasta_ts,
        "ctl00$CPH1$devDteEdtFechaAperturaHasta":                     fecha_hasta,
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_DDDWS":               "0:0:12000:30:1584:0:0:0",
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_DDD_C_FNPWS":         "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta$DDD$C":               fecha_hasta,
        "ctl00$CPH1$ddlResultadoOrdenadoPor":                         "PLI.Pliego.NumeroPliego",
        "ctl00$CPH1$ddlTipoOperacion":                                "-2",
        "ctl00$CPH1$hidEstadoListaPliegos":                           "NOREPORTEEXCEL",
        "ctl00$CPH1$devCbPnlPopupListarProveedor$txtPopupNombreProveedor": "",
        "ctl00$CPH1$devCbPnlPopupListarProveedor$txtPopupCuitProveedor":   "",
        "ctl00_CPH1_devPopupListarProveedorWS":                       "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$hdnFldIdProveedorSeleccionado":                   "",
        "ctl00_CPH1_devPopupVistaPreviaProcesoCompraCiudadanoWS":     "0:0:-1:0:0:0:0:0:",
        "ctl00_CPH1_devPopupVistaPreviaPliegoWS":                     "0:0:-1:0:0:0:0:0:",
    }


# ── Parser ────────────────────────────────────────────────────────────────────
def parsear_tabla(panel_html, scraped_at):
    """
    Extrae filas de datos de GridListaPliegos.
    Filtro robusto: solo filas cuya celda 0 contiene un <a id="...lnkNumeroProceso">.
    Las filas del paginador nunca tienen ese link — solo tienen números o "...".
    """
    soup  = BeautifulSoup(panel_html, "html.parser")
    tabla = soup.find("table", {"id": "ctl00_CPH1_GridListaPliegos"})
    if not tabla:
        return []

    tbody = tabla.find("tbody")
    filas = tbody.find_all("tr") if tbody else tabla.find_all("tr")[1:]

    resultados = []
    for fila in filas:
        celdas = fila.find_all("td")
        if not celdas:
            continue

        # Filtro definitivo: solo filas con link de proceso real
        link = celdas[0].find("a", id=re.compile(r"lnkNumeroProceso"))
        if not link:
            continue

        numero = link.get_text(strip=True)

        resultados.append({
            "numero_proceso":   numero,
            "expediente":       celdas[1].get_text(strip=True) if len(celdas) > 1 else "",
            "nombre_proceso":   celdas[2].get_text(strip=True) if len(celdas) > 2 else "",
            "tipo_proceso":     celdas[3].get_text(strip=True) if len(celdas) > 3 else "",
            "fecha_apertura":   celdas[4].get_text(strip=True) if len(celdas) > 4 else "",
            "estado":           celdas[5].get_text(strip=True) if len(celdas) > 5 else "",
            "unidad_ejecutora": celdas[6].get_text(strip=True) if len(celdas) > 6 else "",
            "saf":              celdas[7].get_text(strip=True) if len(celdas) > 7 else "",
            "scraped_at":       scraped_at,
        })

    return resultados


def cantidad_total(panel_html):
    m = re.search(r'encontrado.*?\((\d+)\)', panel_html)
    return int(m.group(1)) if m else 0


# ── Request de una página ─────────────────────────────────────────────────────
def scrapear_pagina(session, vs, saf_id, fecha_desde, fecha_hasta, fecha_hasta_ts,
                    numero_pagina, scraped_at):
    p = payload_base(vs, saf_id, fecha_desde, fecha_hasta, fecha_hasta_ts)

    if numero_pagina == 1:
        p["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$btnListarPliegoAvanzado"
        p["__EVENTTARGET"]        = "ctl00$CPH1$btnListarPliegoAvanzado"
        p["__EVENTARGUMENT"]      = "undefined"
        p["__ASYNCPOST"]          = "true"
    else:
        # El GridView acepta Page$N para cualquier N válido directamente,
        # sin necesidad de que el número esté visible en el paginador HTML.
        p["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$GridListaPliegos"
        p["__EVENTTARGET"]        = "ctl00$CPH1$GridListaPliegos"
        p["__EVENTARGUMENT"]      = f"Page${numero_pagina}"
        p["__ASYNCPOST"]          = "true"

    r = session.post(BASE_URL, data=p, headers=HEADERS_AJAX, verify=False)
    if r.status_code != 200:
        print(f"  ⚠️  Página {numero_pagina}: HTTP {r.status_code}")
        return [], r.text

    panel1 = extraer_panel(r.text, "ctl00_CPH1_UpdatePanel1")
    actualizar_viewstate(r.text, vs)
    return parsear_tabla(panel1, scraped_at), r.text


# ── Scraping completo ─────────────────────────────────────────────────────────
def scrapear_todos(verbose=True):
    session        = requests.Session()
    scraped_at     = datetime.now().isoformat()
    fecha_hasta    = datetime.today().strftime("%d/%m/%Y")
    fecha_hasta_ts = str(int(datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000))

    if verbose:
        print("=" * 60)
        print(f"Scraping comprar.gob.ar — SAF {SAF_ID} (JGM)")
        print(f"Período: {FECHA_DESDE} → {fecha_hasta}")
        print("=" * 60)

    # PASO 1: GET
    r1   = session.get(BASE_URL, headers=HEADERS_GET, verify=False)
    soup = BeautifulSoup(r1.text, "html.parser")
    vs   = extraer_viewstate(soup)
    if verbose:
        print(f"  ViewState: {bool(vs['__VIEWSTATE'])}")

    # PASO 2: Seleccionar SAF → actualiza dropdown unidades ejecutoras
    time.sleep(DELAY_SEG)
    p2 = payload_base(vs, SAF_ID, FECHA_DESDE, fecha_hasta, fecha_hasta_ts)
    p2["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$ddlJurisdicion"
    p2["__EVENTTARGET"]        = "ctl00$CPH1$ddlJurisdicion"
    p2["__EVENTARGUMENT"]      = ""
    r2 = session.post(BASE_URL, data=p2, headers=HEADERS_AJAX, verify=False)
    actualizar_viewstate(r2.text, vs)
    if verbose:
        panel2 = extraer_panel(r2.text, "ctl00_CPH1_UpdatePanel2")
        soup2  = BeautifulSoup(panel2, "html.parser")
        ues    = [o.get_text(strip=True) for o in soup2.find_all("option") if o.get("value")]
        print(f"  Unidades ejecutoras: {len(ues)}")

    # PASO 3: Página 1 — obtener total real de resultados
    time.sleep(DELAY_SEG)
    todos = []
    filas_p1, raw_p1 = scrapear_pagina(
        session, vs, SAF_ID, FECHA_DESDE, fecha_hasta, fecha_hasta_ts,
        numero_pagina=1, scraped_at=scraped_at,
    )
    todos.extend(filas_p1)
    panel1        = extraer_panel(raw_p1, "ctl00_CPH1_UpdatePanel1")
    total         = cantidad_total(panel1)
    total_paginas = math.ceil(total / FILAS_POR_PAGINA)

    if verbose:
        print(f"\n  Total resultados: {total}")
        print(f"  Total páginas:    {total_paginas}")
        print(f"  Página 1: {len(filas_p1)} filas")

    # PASO 4: Páginas 2..N calculadas matemáticamente
    for num_pag in range(2, total_paginas + 1):
        time.sleep(DELAY_SEG)
        filas, _ = scrapear_pagina(
            session, vs, SAF_ID, FECHA_DESDE, fecha_hasta, fecha_hasta_ts,
            numero_pagina=num_pag, scraped_at=scraped_at,
        )
        todos.extend(filas)
        if verbose:
            print(f"  Página {num_pag}/{total_paginas}: {len(filas)} filas  acumulado={len(todos)}")

    if verbose:
        print(f"\n  ✅ Total scrapeado: {len(todos)} / {total} contratos")
        if len(todos) != total:
            print(f"  ⚠️  Diferencia de {abs(total - len(todos))} filas")

    return todos


# ── CSV ───────────────────────────────────────────────────────────────────────
def guardar_csv(resultados, path="contratos_jgm.csv"):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNAS_CSV)
        writer.writeheader()
        writer.writerows(resultados)
    print(f"  💾 CSV: {path} ({len(resultados)} filas)")


# ── PostgreSQL ────────────────────────────────────────────────────────────────
def guardar_db(resultados):
    try:
        import psycopg2
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError as e:
        print(f"  ⚠️  Dependencia faltante: {e}")
        return

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("  ⚠️  DATABASE_URL no definida en .env")
        return

    try:
        conn = psycopg2.connect(dsn)
    except psycopg2.OperationalError as e:
        print(f"  ⚠️  No se pudo conectar a la DB: {e}")
        return

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contratos_comprar (
            id                SERIAL PRIMARY KEY,
            numero_proceso    VARCHAR(50) UNIQUE,
            expediente        TEXT,
            nombre_proceso    TEXT,
            tipo_proceso      VARCHAR(100),
            fecha_apertura    VARCHAR(30),
            estado            VARCHAR(50),
            unidad_ejecutora  TEXT,
            saf               TEXT,
            scraped_at        TIMESTAMP
        )
    """)
    conn.commit()

    insertados = actualizados = 0
    for r in resultados:
        cur.execute("""
            INSERT INTO contratos_comprar
                (numero_proceso, expediente, nombre_proceso, tipo_proceso,
                 fecha_apertura, estado, unidad_ejecutora, saf, scraped_at)
            VALUES (%(numero_proceso)s, %(expediente)s, %(nombre_proceso)s,
                    %(tipo_proceso)s, %(fecha_apertura)s, %(estado)s,
                    %(unidad_ejecutora)s, %(saf)s, %(scraped_at)s)
            ON CONFLICT (numero_proceso) DO UPDATE SET
                estado     = EXCLUDED.estado,
                scraped_at = EXCLUDED.scraped_at
            RETURNING (xmax = 0)
        """, r)
        if cur.fetchone()[0]:
            insertados += 1
        else:
            actualizados += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"  💾 DB: {insertados} nuevos, {actualizados} actualizados")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper comprar.gob.ar JGM")
    parser.add_argument("--db",  action="store_true", help="Guardar en PostgreSQL")
    parser.add_argument("--csv", action="store_true", help="Guardar en CSV")
    args = parser.parse_args()

    resultados = scrapear_todos(verbose=True)

    if not resultados:
        print("⚠️  Sin resultados")
        sys.exit(1)

    print(f"\nMuestra (primeras 3 filas):")
    for r in resultados[:3]:
        print(f"  {r['numero_proceso']} | {r['tipo_proceso']} | {r['estado']} | {r['fecha_apertura']}")

    if args.db:
        guardar_db(resultados)
    if args.csv:
        guardar_csv(resultados)
    if not args.db and not args.csv:
        print("\n(Sin --db ni --csv: solo se imprimieron los resultados)")
        print("Usar: python scripts/scraper_comprar.py --csv")