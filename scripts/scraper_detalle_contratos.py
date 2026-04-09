#!/usr/bin/env python3
"""
scraper_detalle_contratos.py  v2
=================================
Obtiene monto_adjudicado + proveedor_razon + proveedor_cuit para cada
contrato adjudicado en la DB, usando el buscador de COMPRAR.

FLUJO POR CONTRATO:
  1. Postback a /Compras.aspx con txtNroProceso = NUP
  2. Parsear el unico resultado -> capturar href con token qs=
  3. GET a esa URL (VistaPreviaPliegoCiudadano.aspx?qs=...)
  4. Parsear tabla "Documento contractual por proveedor"
  5. UPDATE en DB

MODOS:
  --diag NUP          diagnostico de un NUP especifico (guarda diag_busqueda.html)
  --limit N           procesar solo N contratos
  --area jgm          filtrar por area
  --dry-run           sin escritura en DB
  --reset             ignorar checkpoint

REQUISITOS:
  pip install requests beautifulsoup4 psycopg2-binary python-dotenv
"""

import os, re, sys, time, json, logging, argparse, urllib3
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
urllib3.disable_warnings()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/jefatura_gabinete"
)

BASE_URL     = "https://comprar.gob.ar/BuscarAvanzadoPublicacion.aspx"
BUSCAR_URL   = "https://comprar.gob.ar/BuscarAvanzadoPublicacion.aspx"
DETALLE_BASE = "https://comprar.gob.ar"

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "es-AR,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://comprar.gob.ar",
    "Referer": BASE_URL,
}

FECHA_INICIO    = "10/12/2023"
DELAY_OK        = 1.5
DELAY_ERR       = 6.0
MAX_RETRIES     = 3
CHECKPOINT_CADA = 200

SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(
    SCRIPT_DIR, "..", "src", "frontend", "data", "_detalle_checkpoint.json"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# VIEWSTATE
# ---------------------------------------------------------------------------

def obtener_viewstate(session):
    """Obtiene ViewState del buscador de COMPRAR."""
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(BASE_URL, headers=HEADERS, timeout=30, verify=False)
            soup = BeautifulSoup(r.text, "html.parser")

            def val(id_):
                tag = soup.find("input", {"id": id_})
                return tag["value"] if tag and tag.get("value") else ""

            vs = {
                "__VIEWSTATE":          val("__VIEWSTATE"),
                "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
                "__EVENTVALIDATION":    val("__EVENTVALIDATION"),
            }
            if vs["__VIEWSTATE"]:
                return vs
            log.warning("  ViewState vacio en intento {}".format(intento))
        except Exception as e:
            log.warning("  ViewState error intento {}: {}".format(intento, e))
        time.sleep(DELAY_ERR)
    return None


def build_payload(vs, nup):
    return {
        "__EVENTTARGET":        "ctl00$CPH1$btnListarPublicacionNumero",
        "__EVENTARGUMENT":      "",
        "__VIEWSTATE":          vs["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": vs["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":    vs["__EVENTVALIDATION"],
        "ctl00$CPH1$txtNumeroPublicacion": nup,
        "ctl00$CPH1$txtPublicacionObjeto": "",
        "ctl00$CPH1$ddlSAF":              "",
        "ctl00$CPH1$ddlUOC":              "",
        "ctl00$CPH1$ddlTipoPublicacion":  "",
        "ctl00$CPH1$ddlTipoProcedimiento": "",
    }
# ---------------------------------------------------------------------------
# PASO 1: buscar NUP -> obtener URL de detalle
# ---------------------------------------------------------------------------

def buscar_url_detalle(session, vs, nup, diag=False):
    """
    Hace postback buscando el NUP exacto.
    Si diag=True guarda el HTML de respuesta en diag_busqueda.html.
    Devuelve la URL completa de VistaPreviaPliegoCiudadano o None.
    """
    payload = build_payload(vs, nup)

    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(
                BUSCAR_URL,
                data=payload,
                headers=HEADERS,
                timeout=45,
                verify=False
            )

            # Guardar HTML para diagnostico
            if diag:
                with open("diag_busqueda.html", "w", encoding="utf-8") as f:
                    f.write(r.text)
                print("    HTML guardado en diag_busqueda.html ({} bytes)".format(len(r.text)))

            soup = BeautifulSoup(r.text, "html.parser")

            # Buscar cualquier link con token qs= o VistaPreviaPliegoCiudadano
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "VistaPreviaPliegoCiudadano" in href or (
                        "qs=" in href and "Compras.aspx" not in href):
                    return urljoin(DETALLE_BASE, href)

            # Buscar en atributos onclick
            for a in soup.find_all("a"):
                onclick = a.get("onclick", "")
                if "VistaPreviaPliegoCiudadano" in onclick:
                    m = re.search(r"['\"]([^'\"]*VistaPreviaPliegoCiudadano[^'\"]*)['\"]", onclick)
                    if m:
                        return urljoin(DETALLE_BASE, m.group(1))
                if "qs=" in onclick:
                    m = re.search(r"['\"]([^'\"]*qs=[^'\"]*)['\"]", onclick)
                    if m and "Compras.aspx" not in m.group(1):
                        return urljoin(DETALLE_BASE, m.group(1))

            # No encontro link — puede ser que el NUP no devuelva resultados
            return None

        except Exception as e:
            log.warning("  buscar_url intento {}: {}".format(intento, e))
            time.sleep(DELAY_ERR * intento)

    return None

# ---------------------------------------------------------------------------
# PASO 2: parsear pagina de detalle
# ---------------------------------------------------------------------------

def limpiar_monto(texto):
    if not texto:
        return None
    t = re.sub(r"[^\d.,]", "", texto.strip())
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    elif t.count(".") > 1:
        t = t.replace(".", "")
    try:
        v = float(t)
        return v if v >= 1.0 else None
    except ValueError:
        return None


def limpiar_cuit(texto):
    if not texto:
        return None
    solo = re.sub(r"\D", "", texto)
    if len(solo) == 11:
        return "{}-{}-{}".format(solo[:2], solo[2:10], solo[10])
    m = re.search(r"(\d{2})[-.\s](\d{8})[-.\s](\d)", texto)
    if m:
        return "{}-{}-{}".format(m.group(1), m.group(2), m.group(3))
    return None


def _parsear_tabla_contractual(tabla):
    """
    Parsea tabla con columnas:
    Numero | Nombre proveedor | CUIT | Tipo | Estado | Fecha | Monto | Moneda
    """
    resultado = {"monto_adjudicado": None, "proveedor_razon": None, "proveedor_cuit": None}
    rows = tabla.find_all("tr")
    if not rows:
        return resultado

    # Detectar indices de columnas desde encabezado
    col_nombre = col_cuit = col_monto = col_tipo = None
    header_cells = rows[0].find_all(["th", "td"])
    for i, cell in enumerate(header_cells):
        t = cell.get_text(strip=True).lower()
        if ("nombre" in t or "proveedor" in t) and "cuit" not in t:
            col_nombre = i
        elif "cuit" in t or "cuil" in t:
            col_cuit = i
        elif "monto" in t or "importe" in t:
            col_monto = i
        elif t == "tipo":
            col_tipo = i

    # Posiciones por defecto si no hay encabezado claro
    # Numero(0) | Nombre(1) | CUIT(2) | Tipo(3) | Estado(4) | Fecha(5) | Monto(6) | Moneda(7)
    if col_nombre is None: col_nombre = 1
    if col_cuit   is None: col_cuit   = 2
    if col_monto  is None: col_monto  = 6

    mejor_monto = None
    mejor_proveedor = None
    mejor_cuit = None

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 4:
            continue
        texts = [c.get_text(strip=True) for c in cells]
        # Capturar URL de detalle del primer link de la fila
        primer_link = row.find("a", href=True)
        url_detalle = ("https://comprar.gob.ar" + primer_link["href"]) if primer_link else None
        # Filtrar filas de paginación...

        nombre = tds[col_nombre].get_text(strip=True) if col_nombre < len(tds) else ""
        cuit   = limpiar_cuit(tds[col_cuit].get_text(strip=True)) if col_cuit < len(tds) else None
        monto  = limpiar_monto(tds[col_monto].get_text(strip=True)) if col_monto < len(tds) else None

        if monto is not None and (mejor_monto is None or monto > mejor_monto):
            mejor_monto     = monto
            mejor_proveedor = nombre
            mejor_cuit      = cuit

    resultado["monto_adjudicado"] = mejor_monto
    resultado["proveedor_razon"]  = mejor_proveedor[:255] if mejor_proveedor else None
    resultado["proveedor_cuit"]   = mejor_cuit
    return resultado


def parsear_detalle(html):
    """Parsea VistaPreviaPliegoCiudadano y extrae monto/proveedor/CUIT."""
    soup = BeautifulSoup(html, "html.parser")
    resultado = {"monto_adjudicado": None, "proveedor_razon": None, "proveedor_cuit": None}

    # Buscar seccion "Documento contractual por proveedor"
    for tag in soup.find_all(["h2","h3","h4","strong","b","span","td","th","p"]):
        txt = tag.get_text(strip=True).lower()
        if "documento contractual" in txt or "contractual por proveedor" in txt:
            tabla = tag.find_next("table")
            if tabla:
                r = _parsear_tabla_contractual(tabla)
                if r["monto_adjudicado"] is not None:
                    resultado.update(r)
                    return resultado

    # Buscar cualquier tabla con columnas de monto y cuit
    for tabla in soup.find_all("table"):
        headers = []
        primera = tabla.find("tr")
        if primera:
            headers = [c.get_text(strip=True).lower()
                       for c in primera.find_all(["th", "td"])]
        if any("monto" in h for h in headers) and any("cuit" in h for h in headers):
            r = _parsear_tabla_contractual(tabla)
            if r["monto_adjudicado"] is not None:
                resultado.update(r)
                return resultado

    # Fallback: regex en texto
    texto = soup.get_text(separator="\n")
    m = re.search(r"([\d.,]{4,})\s*\n?\s*Peso Argentino", texto)
    if m:
        resultado["monto_adjudicado"] = limpiar_monto(m.group(1))
    c = re.search(r"\b(\d{2})-(\d{8})-(\d)\b", texto)
    if c:
        resultado["proveedor_cuit"] = "{}-{}-{}".format(c.group(1), c.group(2), c.group(3))

    return resultado


def obtener_detalle_pagina(session, url):
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=30, verify=False)
            if r.status_code == 200:
                return parsear_detalle(r.text)
        except Exception as e:
            log.warning("  GET detalle intento {}: {}".format(intento, e))
            time.sleep(DELAY_ERR * intento)
    return None

# ---------------------------------------------------------------------------
# BASE DE DATOS
# ---------------------------------------------------------------------------

def conectar_db():
    return psycopg2.connect(DATABASE_URL)


def obtener_pendientes(conn, area=None, limit=None):
    base = """
        SELECT id, numero_proceso, organismo, area
        FROM contratos
        WHERE monto_adjudicado IS NULL
          AND detalle_scrapeado = FALSE
          AND numero_proceso IS NOT NULL
          AND trim(numero_proceso) != ''
          AND estado ILIKE '%djudicado%'
    """
    params = []
    if area:
        base += " AND area = %s"
        params.append(area)
    base += " ORDER BY area, organismo, id"
    if limit:
        base += " LIMIT %s"
        params.append(limit)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(base, params)
        return [dict(r) for r in cur.fetchall()]


def actualizar_contrato(conn, id_, datos, dry_run=False):
    campos = {"detalle_scrapeado": True}
    if datos.get("monto_adjudicado") is not None:
        campos["monto_adjudicado"] = datos["monto_adjudicado"]
    if datos.get("proveedor_razon"):
        campos["proveedor_razon"] = datos["proveedor_razon"]
    if datos.get("proveedor_cuit"):
        campos["proveedor_cuit"] = datos["proveedor_cuit"]

    if dry_run:
        return bool(datos.get("monto_adjudicado"))

    set_clause = ", ".join("{} = %s".format(k) for k in campos)
    vals = list(campos.values()) + [id_]
    with conn.cursor() as cur:
        cur.execute("UPDATE contratos SET {} WHERE id = %s".format(set_clause), vals)
    conn.commit()
    return bool(datos.get("monto_adjudicado"))

# ---------------------------------------------------------------------------
# CHECKPOINT
# ---------------------------------------------------------------------------

def cargar_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, encoding="utf-8") as f:
                return set(json.load(f).get("procesados", []))
        except Exception:
            pass
    return set()


def guardar_checkpoint(procesados):
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"procesados": list(procesados),
                   "actualizado": datetime.now().isoformat(),
                   "total": len(procesados)}, f)

# ---------------------------------------------------------------------------
# DIAGNOSTICO
# ---------------------------------------------------------------------------

def modo_diagnostico(nup):
    print("\n" + "="*65)
    print("DIAGNOSTICO — NUP: {}".format(nup))
    print("="*65)

    session = requests.Session()
    session.verify = False

    print("\n[1] Obteniendo ViewState...")
    vs = obtener_viewstate(session)
    if not vs:
        print("ERROR: no se pudo obtener ViewState")
        return
    print("    OK — VIEWSTATE len={}".format(len(vs["__VIEWSTATE"])))

    print("\n[2] Buscando NUP en COMPRAR (guardando HTML en diag_busqueda.html)...")
    url_detalle = buscar_url_detalle(session, vs, nup, diag=True)

    if not url_detalle:
        print("    No se encontro URL de detalle.")
        print("    Revisa diag_busqueda.html para ver que devolvio el servidor.")
        print("\n    Pista rapida:")
        print("    Select-String -Path diag_busqueda.html -Pattern 'VistaPreviaPliego|qs=|encontrado|resultado' | Select-Object -First 15")
        return

    print("    URL: {}...".format(url_detalle[:90]))

    print("\n[3] Descargando pagina de detalle...")
    r = session.get(url_detalle, headers=HEADERS, timeout=30, verify=False)
    print("    Status: {}".format(r.status_code))

    with open("diag_detalle.html", "w", encoding="utf-8") as f:
        f.write(r.text)
    print("    HTML guardado en diag_detalle.html")

    print("\n[4] Parseando...")
    resultado = parsear_detalle(r.text)
    print("\n    Resultado:")
    for k, v in resultado.items():
        print("    {:22s}: {}".format(k, v))

    if resultado["monto_adjudicado"] is None:
        soup = BeautifulSoup(r.text, "html.parser")
        tablas = soup.find_all("table")
        print("\n    Tablas encontradas: {}".format(len(tablas)))
        for i, t in enumerate(tablas[:8]):
            rows = t.find_all("tr")
            print("\n    Tabla {} ({} filas):".format(i+1, len(rows)))
            for row in rows[:4]:
                tds = row.find_all(["th","td"])
                print("      {}".format([td.get_text(strip=True)[:45] for td in tds]))

    print("\n" + "="*65)

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--diag",    type=str, default=None,
                        help="Diagnostico de un NUP especifico")
    parser.add_argument("--limit",   type=int, default=None)
    parser.add_argument("--area",    type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset",   action="store_true")
    args = parser.parse_args()

    if args.diag:
        modo_diagnostico(args.diag)
        return

    print("=" * 65)
    print("SCRAPER DETALLE v2 — COMPRAR.GOB.AR")
    print("   {}".format(datetime.now().strftime("%d/%m/%Y %H:%M")))
    if args.dry_run: print("   DRY-RUN")
    if args.area:    print("   Area: {}".format(args.area))
    print("=" * 65)

    try:
        conn = conectar_db()
        log.info("Conectado a PostgreSQL")
    except Exception as e:
        log.error("DB error: {}".format(e))
        sys.exit(1)

    pendientes = obtener_pendientes(conn, area=args.area, limit=args.limit)
    log.info("{} contratos adjudicados sin monto".format(len(pendientes)))

    if not pendientes:
        print("No hay contratos pendientes.")
        conn.close()
        return

    procesados = set() if args.reset else cargar_checkpoint()
    pendientes = [p for p in pendientes if p["numero_proceso"] not in procesados]
    log.info("{} a procesar (checkpoint: {} ya visitados)".format(
        len(pendientes), len(procesados)))

    total   = len(pendientes)
    session = requests.Session()
    session.verify = False

    log.info("Obteniendo ViewState...")
    vs = obtener_viewstate(session)
    if not vs:
        log.error("No se pudo obtener ViewState.")
        sys.exit(1)
    log.info("ViewState OK")

    ok       = 0
    sin_datos = 0
    t_inicio = time.time()

    print("\nProcesando {} contratos...\n".format(total))

    for i, contrato in enumerate(pendientes, 1):
        nup = contrato["numero_proceso"]

        # Refrescar ViewState cada 300 contratos
        if i % 300 == 0:
            vs_nuevo = obtener_viewstate(session)
            if vs_nuevo:
                vs = vs_nuevo
                log.info("  ViewState refrescado")

        # Progreso
        if i % 50 == 0 or i <= 3:
            elapsed = time.time() - t_inicio
            vel = i / elapsed if elapsed > 0 else 0
            resta = (total - i) / vel if vel > 0 else 0
            eta = datetime.fromtimestamp(time.time() + resta).strftime("%H:%M")
            log.info("[{:>5}/{}] OK={} sin={} | {:.2f}/s ETA~{}".format(
                i, total, ok, sin_datos, vel, eta))

        url_detalle = buscar_url_detalle(session, vs, nup)
        procesados.add(nup)

        if not url_detalle:
            actualizar_contrato(conn, contrato["id"], {}, dry_run=args.dry_run)
            sin_datos += 1
            time.sleep(DELAY_OK)
            continue

        time.sleep(0.4)
        resultado = obtener_detalle_pagina(session, url_detalle)

        if resultado and resultado.get("monto_adjudicado") is not None:
            actualizar_contrato(conn, contrato["id"], resultado, dry_run=args.dry_run)
            ok += 1
            if ok <= 5:
                log.info("  OK {}: ${:,.0f} | {}".format(
                    nup,
                    resultado["monto_adjudicado"],
                    str(resultado.get("proveedor_razon", ""))[:40]))
        else:
            actualizar_contrato(conn, contrato["id"], {}, dry_run=args.dry_run)
            sin_datos += 1

        if i % CHECKPOINT_CADA == 0:
            guardar_checkpoint(procesados)
            log.info("  Checkpoint {}/{}".format(i, total))

        time.sleep(DELAY_OK)

    guardar_checkpoint(procesados)

    elapsed = time.time() - t_inicio
    print("\n" + "=" * 65)
    print("RESUMEN")
    print("   Procesados   : {}".format(total))
    print("   Con monto    : {}  ({:.1f}%)".format(ok, ok/total*100 if total else 0))
    print("   Sin datos    : {}".format(sin_datos))
    print("   Tiempo       : {:.1f} min".format(elapsed / 60))
    if ok > 0:
        print("\n   Verificar:")
        print("   SELECT COUNT(*) FROM contratos WHERE monto_adjudicado IS NOT NULL;")
    print("=" * 65)
    conn.close()


if __name__ == "__main__":
    main()
