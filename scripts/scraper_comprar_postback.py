"""
scraper_comprar_postback.py
===========================
Scraper COMPRAR — postback ASP.NET, paginación correcta, 135 organismos del PEN.

Modo append: cada corrida por --area acumula en contratos_comprar_raw.json
sin sobreescribir lo ya recolectado.

Uso:
    python scripts/scraper_comprar_postback.py --area presidencia
    python scripts/scraper_comprar_postback.py --area sgp
    python scripts/scraper_comprar_postback.py --area jgm
    python scripts/scraper_comprar_postback.py --area interior
    python scripts/scraper_comprar_postback.py --area exteriores
    python scripts/scraper_comprar_postback.py --area economia
    python scripts/scraper_comprar_postback.py --area infraestructura
    python scripts/scraper_comprar_postback.py --area justicia
    python scripts/scraper_comprar_postback.py --area seguridad
    python scripts/scraper_comprar_postback.py --area defensa
    python scripts/scraper_comprar_postback.py --area salud
    python scripts/scraper_comprar_postback.py --area capital_humano
    python scripts/scraper_comprar_postback.py --area control
    python scripts/scraper_comprar_postback.py --saf 591
    python scripts/scraper_comprar_postback.py --todos   (corre todo de una)
    python scripts/scraper_comprar_postback.py --reset   (borra acumulado y empieza de cero)
"""

import json
import os
import re
import sys
import time
import argparse
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

# Importar clasificación del PEN
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from organismos_pen_clasificados import TODOS_LOS_SAF, AREAS

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BASE_URL    = "https://comprar.gob.ar/BuscarAvanzado.aspx"
GRID_ID     = "ctl00_CPH1_GridListaPliegos"
GRID_ID_PB  = "ctl00$CPH1$GridListaPliegos"

OUTPUT_DIR      = os.path.join(SCRIPT_DIR, "..", "src", "frontend", "data")
OUTPUT_DIR_AREA = os.path.join(OUTPUT_DIR, "por_area")
PATH_RAW        = os.path.join(OUTPUT_DIR, "contratos_comprar_raw.json")
PATH_PROGRESO   = os.path.join(OUTPUT_DIR, "scraper_progreso.json")
PATH_RESUMEN    = os.path.join(OUTPUT_DIR, "resumen_pen.json")

FECHA_INICIO_DT = datetime(2023, 12, 10)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "es-AR,es;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://comprar.gob.ar",
    "Referer": BASE_URL,
}


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESO — saber qué áreas ya fueron procesadas
# ─────────────────────────────────────────────────────────────────────────────
def cargar_progreso():
    if os.path.exists(PATH_PROGRESO):
        with open(PATH_PROGRESO, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"areas_completadas": [], "safs_completados": []}


def guardar_progreso(progreso):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(PATH_PROGRESO, "w", encoding="utf-8") as f:
        json.dump(progreso, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# ACUMULADOR — carga el JSON existente y agrega sin duplicar
# ─────────────────────────────────────────────────────────────────────────────
def cargar_existentes():
    if os.path.exists(PATH_RAW):
        with open(PATH_RAW, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def guardar_acumulado(todos):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR_AREA, exist_ok=True)

    # JSON consolidado
    with open(PATH_RAW, "w", encoding="utf-8") as f:
        json.dump(todos, f, ensure_ascii=False, indent=2)

    # JSONs por área
    por_area = {}
    for c in todos:
        area = c.get("area", "sin_area")
        por_area.setdefault(area, []).append(c)

    for area, contratos in por_area.items():
        path_area = os.path.join(OUTPUT_DIR_AREA, f"contratos_{area}.json")
        with open(path_area, "w", encoding="utf-8") as f:
            json.dump(contratos, f, ensure_ascii=False, indent=2)

    # Resumen
    resumen = []
    for area_key, area_data in AREAS.items():
        contratos_area = por_area.get(area_key, [])
        resumen.append({
            "area":       area_key,
            "area_label": area_data["label"],
            "total":      len(contratos_area),
            "organismos": len(set(c["saf_id"] for c in contratos_area)),
        })
    with open(PATH_RESUMEN, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS HTTP
# ─────────────────────────────────────────────────────────────────────────────
def _fresh_session():
    session = requests.Session()
    r = session.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
    soup = BeautifulSoup(r.text, "html.parser")
    viewstate = {
        "__VIEWSTATE":          soup.find("input", {"id": "__VIEWSTATE"})["value"],
        "__VIEWSTATEGENERATOR": soup.find("input", {"id": "__VIEWSTATEGENERATOR"})["value"],
    }
    return session, viewstate


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
        "ctl00$CPH1$ddlEstadoProceso":               "-2",
        "ctl00$CPH1$ddlRubro":                       "-2",
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


def _parse_filas(tabla, nombre_org, saf_id, area, area_label):
    resultados = []
    for fila in tabla.find_all("tr")[1:]:
        celdas = fila.find_all("td")
        if len(celdas) != 8:
            continue
        textos = [c.get_text(strip=True) for c in celdas]
        fecha_str = textos[4][:10] if len(textos) > 4 else ""
        fecha_dt = None
        try:
            fecha_dt = datetime.strptime(fecha_str, "%d/%m/%Y")
        except Exception:
            pass
        if fecha_dt and fecha_dt < FECHA_INICIO_DT:
            continue
        link_tag = fila.find("a", href=True)
        link = ""
        if link_tag:
            href = link_tag["href"]
            if not href.startswith("javascript"):
                link = f"https://comprar.gob.ar/{href}" if not href.startswith("http") else href
        resultados.append({
            "numero_proceso":   textos[0],
            "expediente":       textos[1],
            "nombre_proceso":   textos[2],
            "tipo_proceso":     textos[3],
            "fecha_apertura":   textos[4],
            "estado":           textos[5],
            "unidad_ejecutora": textos[6],
            "saf_nombre":       textos[7],
            "organismo":        nombre_org,
            "saf_id":           saf_id,
            "area":             area,
            "area_label":       area_label,
            "link":             link,
            "fuente":           "COMPRAR",
        })
    return resultados


def _get_siguiente_pagina(tabla, paginas_visitadas):
    ultima_fila = tabla.find_all("tr")[-1]
    pagina_actual = 1
    span = ultima_fila.find("span")
    if span:
        try:
            pagina_actual = int(span.get_text(strip=True))
        except Exception:
            pass
    paginas_bloque = []
    siguiente_bloque = None
    for td in ultima_fila.find_all("td"):
        a = td.find("a", href=True)
        if not a:
            continue
        txt = a.get_text(strip=True)
        href = a.get("href", "")
        m = re.search(r"Page\$(\d+)", href)
        if not m:
            continue
        num = int(m.group(1))
        if txt == "...":
            if num > pagina_actual:
                siguiente_bloque = num
        else:
            paginas_bloque.append(num)
    candidatos = sorted(p for p in paginas_bloque if p not in paginas_visitadas)
    if candidatos:
        return candidatos[0]
    if siguiente_bloque and siguiente_bloque not in paginas_visitadas:
        return siguiente_bloque
    return None


# ─────────────────────────────────────────────────────────────────────────────
# SCRAPE ORGANISMO
# ─────────────────────────────────────────────────────────────────────────────
def scrape_organismo(saf_id, nombre_org, area, area_label):
    print(f"\n  [{area}] {nombre_org} (SAF {saf_id})")
    todos = []
    session, viewstate = _fresh_session()

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

    res = _parse_filas(tabla, nombre_org, saf_id, area, area_label)
    todos.extend(res)
    print(f"    Pág 1: {len(res)} en período Milei")

    paginas_visitadas = {1}
    while True:
        siguiente = _get_siguiente_pagina(tabla, paginas_visitadas)
        if siguiente is None:
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
            if not panel_pag:
                break
            tabla_pag = panel_pag.find("table", {"id": GRID_ID})
            if not tabla_pag:
                break
            res_pag = _parse_filas(tabla_pag, nombre_org, saf_id, area, area_label)
            todos.extend(res_pag)
            paginas_visitadas.add(siguiente)
            print(f"    Pág {siguiente}: {len(res_pag)} en período Milei")
            tabla = tabla_pag
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
    parser.add_argument("--area",  help="Área ministerial (ej: jgm, economia, salud)")
    parser.add_argument("--todos", action="store_true")
    parser.add_argument("--reset", action="store_true", help="Borra acumulado y empieza de cero")
    args = parser.parse_args()

    # Reset
    if args.reset:
        for path in [PATH_RAW, PATH_PROGRESO, PATH_RESUMEN]:
            if os.path.exists(path):
                os.remove(path)
        import glob
        for f in glob.glob(os.path.join(OUTPUT_DIR_AREA, "contratos_*.json")):
            os.remove(f)
        print("Reset completo. Podés empezar de cero.")
        return

    print("\n" + "═"*60)
    print("🔍 SCRAPER COMPRAR — PEN COMPLETO (modo append)")
    print(f"   Filtro: desde 10/12/2023 (gestión Milei)")
    print(f"   Organismos disponibles: {len(TODOS_LOS_SAF)}")
    print("═"*60)

    # Cargar acumulado existente
    existentes = cargar_existentes()
    progreso = cargar_progreso()

    # Deduplicar por numero_proceso + saf_id
    keys_existentes = set(
        (c["numero_proceso"], c["saf_id"]) for c in existentes
    )
    print(f"\n  Contratos ya acumulados: {len(existentes)}")
    print(f"  Áreas ya completadas: {progreso['areas_completadas']}")

    nuevos = []

    if args.saf:
        info = TODOS_LOS_SAF.get(args.saf, {
            "nombre": f"SAF {args.saf}",
            "area": "sin_area",
            "area_label": "Sin clasificar"
        })
        res = scrape_organismo(args.saf, info["nombre"], info["area"], info["area_label"])
        nuevos.extend(res)

    elif args.area:
        if args.area not in AREAS:
            print(f"Área '{args.area}' no encontrada.")
            print(f"Disponibles: {list(AREAS.keys())}")
            return

        if args.area in progreso["areas_completadas"]:
            print(f"\n  ⚠️  Área '{args.area}' ya fue procesada.")
            print(f"  Usá --reset si querés volver a scrapearla.")
            return

        area_data = AREAS[args.area]
        print(f"\n{'─'*60}")
        print(f"  {area_data['label']} ({len(area_data['organismos'])} organismos)")
        print(f"{'─'*60}")

        for saf, nombre in area_data["organismos"].items():
            res = scrape_organismo(saf, nombre, args.area, area_data["label"])
            nuevos.extend(res)
            time.sleep(1)

        progreso["areas_completadas"].append(args.area)

    elif args.todos:
        areas_pendientes = [a for a in AREAS.keys() if a not in progreso["areas_completadas"]]
        print(f"\n  Áreas pendientes: {areas_pendientes}")

        for area_key in areas_pendientes:
            area_data = AREAS[area_key]
            print(f"\n{'─'*60}")
            print(f"  {area_data['label']}")
            print(f"{'─'*60}")
            for saf, nombre in area_data["organismos"].items():
                res = scrape_organismo(saf, nombre, area_key, area_data["label"])
                nuevos.extend(res)
                time.sleep(1)
            progreso["areas_completadas"].append(area_key)
            # Guardar progreso parcial después de cada área
            todos_hasta_ahora = existentes + [
                c for c in nuevos
                if (c["numero_proceso"], c["saf_id"]) not in keys_existentes
            ]
            guardar_acumulado(todos_hasta_ahora)
            guardar_progreso(progreso)
            print(f"\n  ✓ Área '{area_key}' guardada. Total acumulado: {len(todos_hasta_ahora)}")

    else:
        # Sin flags: mostrar estado actual
        print(f"\n  Estado del scraping:")
        todas_areas = list(AREAS.keys())
        for area in todas_areas:
            estado = "✓" if area in progreso["areas_completadas"] else "⏳"
            label = AREAS[area]["label"]
            print(f"    {estado} [{area}] {label}")
        print(f"\n  Usá --area <nombre> para scrapear un área.")
        return

    # Deduplicar y acumular
    nuevos_unicos = [
        c for c in nuevos
        if (c["numero_proceso"], c["saf_id"]) not in keys_existentes
    ]
    todos = existentes + nuevos_unicos

    print(f"\n\n{'═'*60}")
    print(f"  Nuevos contratos esta corrida: {len(nuevos_unicos)}")
    print(f"  TOTAL acumulado: {len(todos)}")

    if nuevos_unicos:
        guardar_acumulado(todos)
        guardar_progreso(progreso)
        print(f"\n  JSONs guardados en: {OUTPUT_DIR}")

        areas_completadas = progreso["areas_completadas"]
        areas_totales = list(AREAS.keys())
        pendientes = [a for a in areas_totales if a not in areas_completadas]
        print(f"\n  Áreas completadas: {len(areas_completadas)}/{len(areas_totales)}")
        if pendientes:
            print(f"  Próxima área a correr:")
            print(f"    python scripts/scraper_comprar_postback.py --area {pendientes[0]}")

        print(f"\n  Muestra:")
        for r in nuevos_unicos[:3]:
            print(f"    {r['numero_proceso']} | {r['area']} | {r['nombre_proceso'][:35]} | {r['fecha_apertura'][:10]}")

    print("═"*60 + "\n")


if __name__ == "__main__":
    main()