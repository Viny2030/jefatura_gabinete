"""
scraper_comprar_postback.py
===========================
Scraper COMPRAR con postback ASP.NET, parsing correcto y paginación.

Estructura tabla confirmada:
  col 0: Número proceso
  col 1: Expediente
  col 2: Nombre proceso
  col 3: Tipo de Proceso
  col 4: Fecha de apertura
  col 5: Estado
  col 6: Unidad Ejecutora
  col 7: Servicio Administrativo Financiero

Grid ID: ctl00_CPH1_GridListaPliegos

Uso:
    python scripts/scraper_comprar_postback.py
    python scripts/scraper_comprar_postback.py --saf 591
    python scripts/scraper_comprar_postback.py --todos
"""

import json
import os
import re
import time
import argparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL   = "https://comprar.gob.ar/BuscarAvanzado.aspx"
GRID_ID    = "ctl00_CPH1_GridListaPliegos"
GRID_ID_PB = "ctl00$CPH1$GridListaPliegos"  # para postback

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "src", "frontend", "data")
DATA_DIR   = os.path.join(SCRIPT_DIR, "..", "data")

FECHA_INICIO_DT = datetime(2023, 12, 10)

ORGANISMOS = {
    "jgm":        {"saf": "591",  "nombre": "Jefatura de Gabinete de Ministros"},
    "presidencia":{"saf": "588",  "nombre": "Secretaría General Presidencia"},
    "innovacion": {"saf": "1742", "nombre": "Secretaría de Innovación Pública"},
    "legal":      {"saf": "586",  "nombre": "Secretaría Legal y Técnica"},
    "medios":     {"saf": "1771", "nombre": "Secretaría de Comunicación y Medios"},
    "cultura":    {"saf": "1736", "nombre": "Secretaría de Cultura"},
    "interior":   {"saf": "1732", "nombre": "Ministerio del Interior"},
    "economia":   {"saf": "1739", "nombre": "Ministerio de Economía"},
    "defensa":    {"saf": "647",  "nombre": "Ministerio de Defensa"},
    "seguridad":  {"saf": "637",  "nombre": "Ministerio de Seguridad"},
    "transporte": {"saf": "645",  "nombre": "Ministerio de Transporte"},
    "salud":      {"saf": "1728", "nombre": "Ministerio de Salud"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "es-AR,es;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://comprar.gob.ar",
    "Referer": BASE_URL,
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _get_viewstate(session):
    r = session.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
    soup = BeautifulSoup(r.text, "html.parser")
    return {
        "__VIEWSTATE":          soup.find("input", {"id": "__VIEWSTATE"})["value"],
        "__VIEWSTATEGENERATOR": soup.find("input", {"id": "__VIEWSTATEGENERATOR"})["value"],
    }


def _update_viewstate(viewstate, soup):
    vs = soup.find("input", {"id": "__VIEWSTATE"})
    if vs:
        viewstate["__VIEWSTATE"] = vs["value"]


def _build_payload(viewstate, saf_id, eventtarget, eventarg=""):
    return {
        "__EVENTTARGET":        eventtarget,
        "__EVENTARGUMENT":      eventarg,
        "__LASTFOCUS":          "",
        "__VIEWSTATE":          viewstate["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": viewstate["__VIEWSTATEGENERATOR"],
        "ctl00$CPH1$txtNumeroProceso":               "",
        "ctl00$CPH1$txtExpediente":                  "",
        "ctl00$CPH1$txtNombrePliego":                "",
        "ctl00$CPH1$ddlJurisdicion":                 saf_id,
        "ctl00$CPH1$ddlUnidadEjecutora":             "-2",
        "ctl00$CPH1$ddlTipoProceso":                 "-2",
        "ctl00$CPH1$ddlEstadoProceso":               "-2",  # Todos los estados
        "ctl00$CPH1$ddlRubro":                       "-2",
        # Sin filtro de fecha — filtramos en código por fecha >= 10/12/2023
        "ctl00$CPH1$devDteEdtFechaAperturaDesde":    "",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde_I":  "",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta":    "",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta_I":  "",
        "ctl00$CPH1$ddlResultadoOrdenadoPor":        "PLI.PliegoCronograma.FechaActoApertura",
        "ctl00$CPH1$ddlTipoOperacion":               "-2",
        "ctl00$CPH1$hidEstadoListaPliegos":          "NOREPORTEEXCEL",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw": "N",
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw": "N",
    }


def _parse_filas(tabla, nombre_org, saf_id):
    """Parsea filas de la tabla de resultados."""
    resultados = []
    filas = tabla.find_all("tr")[1:]  # skip header

    for fila in filas:
        celdas = fila.find_all("td")
        if len(celdas) < 6:
            continue
        # Saltar fila del paginador: sus celdas contienen solo números o "..."
        primer_texto = celdas[0].get_text(strip=True)
        if primer_texto.isdigit() or primer_texto == "...":
            continue
        textos = [c.get_text(strip=True) for c in celdas]

        # Parsear fecha
        fecha_str = textos[4][:10] if len(textos) > 4 else ""
        fecha_dt = None
        try:
            fecha_dt = datetime.strptime(fecha_str, "%d/%m/%Y")
        except Exception:
            pass

        # Filtrar período Milei
        if fecha_dt and fecha_dt < FECHA_INICIO_DT:
            continue

        link_tag = fila.find("a", href=True)
        link = ""
        if link_tag:
            href = link_tag["href"]
            if not href.startswith("javascript"):
                link = f"https://comprar.gob.ar/{href}" if not href.startswith("http") else href

        resultados.append({
            "numero_proceso":    textos[0],
            "expediente":        textos[1],
            "nombre_proceso":    textos[2],
            "tipo_proceso":      textos[3],
            "fecha_apertura":    textos[4],
            "estado":            textos[5],
            "unidad_ejecutora":  textos[6] if len(textos) > 6 else "",
            "saf_nombre":        textos[7] if len(textos) > 7 else nombre_org,
            "organismo":         nombre_org,
            "saf_id":            saf_id,
            "link":              link,
            "fuente":            "COMPRAR",
        })

    return resultados


def _get_paginas_disponibles(soup):
    """Extrae números de página y el link ... del paginador del grid."""
    panel = soup.find(id="ctl00_CPH1_pnlListaPliegos")
    if not panel:
        return [], None

    tabla = panel.find("table", {"id": GRID_ID})
    if not tabla:
        return [], None

    ultima_fila = tabla.find_all("tr")[-1]
    links_pag = ultima_fila.find_all("a", href=True)

    paginas = []
    siguiente_bloque = None
    for a in links_pag:
        txt = a.get_text(strip=True)
        href = a.get("href", "")
        if "Page$" in href:
            import re
            m = re.search(r"Page\\$(\d+)", href)
            if not m:
                m = re.search(r"Page\$(\d+)", href)
            if not m:
                m = re.search(r"Page.(\d+)", href)
            if m:
                num = int(m.group(1))
                if txt == "...":
                    siguiente_bloque = num
                else:
                    paginas.append(num)

    return paginas, siguiente_bloque


def scrape_organismo(session, saf_id, nombre_org, viewstate):
    print(f"\n  → {nombre_org} (SAF {saf_id})")
    todos = []
    pagina_actual = 1

    # Primera búsqueda
    payload = _build_payload(viewstate, saf_id, "ctl00$CPH1$btnListarPliegoAvanzado")
    try:
        r = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30, verify=False)
    except Exception as e:
        print(f"    ERROR: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    _update_viewstate(viewstate, soup)

    lbl = soup.find(id="ctl00_CPH1_lblCantidadListaPliegos")
    print(f"    {lbl.get_text(strip=True) if lbl else '?'}")

    panel = soup.find(id="ctl00_CPH1_pnlListaPliegos")
    if not panel:
        print("    Sin resultados")
        return []

    tabla = panel.find("table", {"id": GRID_ID})
    if not tabla:
        print("    Tabla no encontrada")
        return []

    res = _parse_filas(tabla, nombre_org, saf_id)
    todos.extend(res)
    print(f"    Pág 1: {len(res)} en período Milei")

    # Paginación completa — recorre todos los bloques de páginas
    # El GridView muestra bloques de 10 páginas. Para ir al siguiente bloque
    # hay que hacer click en "..." que es Page$11, Page$21, etc.
    pagina_actual = 1
    max_paginas = 200  # límite de seguridad

    while pagina_actual <= max_paginas:
        paginas, siguiente_bloque = _get_paginas_disponibles(soup)
        if not paginas and siguiente_bloque is None:
            break

        # Encontrar la siguiente página a visitar
        siguiente = None
        for p in sorted(paginas):
            if p > pagina_actual:
                siguiente = p
                break

        # Si no hay más en este bloque, saltar al siguiente bloque (link "...")
        if siguiente is None:
            if siguiente_bloque:
                siguiente = siguiente_bloque
            else:
                break

        time.sleep(0.8)
        payload_pag = _build_payload(
            viewstate, saf_id,
            eventtarget=GRID_ID_PB,
            eventarg=f"Page${siguiente}"
        )
        try:
            r_pag = session.post(BASE_URL, data=payload_pag, headers=HEADERS, timeout=30, verify=False)
            soup_pag = BeautifulSoup(r_pag.text, "html.parser")
            _update_viewstate(viewstate, soup_pag)

            panel_pag = soup_pag.find(id="ctl00_CPH1_pnlListaPliegos")
            if panel_pag:
                tabla_pag = panel_pag.find("table", {"id": GRID_ID})
                if tabla_pag:
                    res_pag = _parse_filas(tabla_pag, nombre_org, saf_id)
                    todos.extend(res_pag)
                    print(f"    Pág {siguiente}: {len(res_pag)} en período Milei")
                    soup = soup_pag  # actualizar para detectar nuevo bloque de páginas
                    pagina_actual = siguiente
                else:
                    break
            else:
                break
        except Exception as e:
            print(f"    ERROR pág {siguiente}: {e}")
            break

    print(f"    Subtotal: {len(todos)}")
    return todos


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--saf",   help="SAF ID específico")
    parser.add_argument("--todos", action="store_true")
    args = parser.parse_args()

    print("\n" + "═"*60)
    print("🔍 SCRAPER COMPRAR — POSTBACK + PAGINACIÓN")
    print(f"   Filtro: desde 10/12/2023 (gestión Milei)")
    print("═"*60)

    session = requests.Session()
    print("\n  Obteniendo ViewState...", end=" ", flush=True)
    viewstate = _get_viewstate(session)
    print("OK")

    todos = []

    if args.saf:
        res = scrape_organismo(session, args.saf, f"SAF {args.saf}", viewstate)
        todos.extend(res)
    elif args.todos:
        for clave, info in ORGANISMOS.items():
            res = scrape_organismo(session, info["saf"], info["nombre"], viewstate)
            todos.extend(res)
            time.sleep(1.5)
    else:
        for clave in ["jgm", "presidencia", "innovacion", "legal", "medios"]:
            info = ORGANISMOS[clave]
            res = scrape_organismo(session, info["saf"], info["nombre"], viewstate)
            todos.extend(res)
            time.sleep(1)

    print(f"\n\n  TOTAL período Milei: {len(todos)}")

    if todos:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        path = os.path.join(OUTPUT_DIR, "contratos_comprar_raw.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(todos, f, ensure_ascii=False, indent=2)
        print(f"  [JSON] {path}")

        print(f"\n  Muestra:")
        for r in todos[:3]:
            print(f"    {r['numero_proceso']} | {r['nombre_proceso'][:40]} | {r['fecha_apertura'][:10]}")

    print("\n" + "═"*60 + "\n")


if __name__ == "__main__":
    main()