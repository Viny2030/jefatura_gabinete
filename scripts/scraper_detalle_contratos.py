#!/usr/bin/env python3
"""
scraper_detalle_contratos.py  v5
=================================
Obtiene monto_adjudicado + proveedor_razon + proveedor_cuit para cada
contrato adjudicado en la DB usando BuscarAvanzado.aspx.

FLUJO POR CONTRATO:
  1. GET  → obtener ViewState
  2. POST → seleccionar SAF 591 (carga unidades ejecutoras)
  3. POST → buscar NUP con btnListarPliegoAvanzado + ddlJurisdicion=591
  4. Identificar ctl dinámico del NUP en los resultados
  5. POST → click en lnkNumeroProceso → redirect VistaPreviaPliegoCiudadano
  6. Parsear tabla "Documento contractual por proveedor"
  7. UPDATE en DB

MODOS:
  --diag NUP     diagnóstico de un NUP específico
  --limit N      procesar solo N contratos
  --dry-run      sin escritura en DB
  --reset        ignorar checkpoint
"""

import os
import re
import sys
import time
import json
import logging
import argparse
import warnings
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5433/jgm_anticorrupcion")
BASE_URL     = "https://comprar.gob.ar/BuscarAvanzado.aspx"
DETALLE_BASE = "https://comprar.gob.ar"
SAF_ID       = "591"  # Jefatura de Gabinete de Ministros

DELAY_OK        = 1.5
DELAY_ERR       = 6.0
MAX_RETRIES     = 3
REFRESH_VS_CADA = 200
CHECKPOINT_CADA = 100

CHECKPOINT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "src", "frontend", "data", "_detalle_checkpoint.json"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS_GET = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9",
}

HEADERS_AJAX = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":           "*/*",
    "Accept-Language":  "es-AR,es;q=0.9",
    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "X-MicrosoftAjax":  "Delta=true",
    "Cache-Control":    "no-cache",
    "Referer":          BASE_URL,
    "Origin":           "https://comprar.gob.ar",
}


# ── ViewState ─────────────────────────────────────────────────────────────────
def obtener_viewstate(session):
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(BASE_URL, headers=HEADERS_GET, timeout=30, verify=False)
            soup = BeautifulSoup(r.text, "html.parser")
            vs = {}
            for f in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
                el = soup.find("input", {"id": f})
                vs[f] = el["value"] if el else ""
            dx = soup.find("input", {"name": "DXScript"})
            vs["DXScript"] = dx["value"] if dx else "1_103,1_105,2_13,2_12,2_7,1_96,1_100,1_83,2_6"
            if vs["__VIEWSTATE"]:
                return vs
        except Exception as e:
            log.warning(f"ViewState intento {intento}: {e}")
            time.sleep(DELAY_ERR)
    return None


def actualizar_viewstate(texto, vs):
    for f in ["__VIEWSTATE", "__EVENTVALIDATION", "__VIEWSTATEGENERATOR"]:
        m = re.search(rf"\d+\|hiddenField\|{re.escape(f)}\|(.*?)\|", texto)
        if m:
            vs[f] = m.group(1)


def extraer_panel(texto_crudo, panel_id):
    pattern = rf"\d+\|updatePanel\|{re.escape(panel_id)}\|(.*?)(?=\d+\|(?:updatePanel|hiddenField|arrayDeclaration|scriptBlock|pageTitle|focus|error|asyncPostBack)\|)"
    m = re.search(pattern, texto_crudo, re.DOTALL)
    if m:
        return m.group(1)
    m2 = re.search(rf"\d+\|updatePanel\|{re.escape(panel_id)}\|(.*)", texto_crudo, re.DOTALL)
    return m2.group(1).rstrip("|") if m2 else ""


def payload_base(vs, nup):
    hoy = datetime.today().strftime("%d/%m/%Y")
    ts  = str(int(datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000))
    return {
        "DXScript":                              vs["DXScript"],
        "__EVENTTARGET":                         "",
        "__EVENTARGUMENT":                       "",
        "__LASTFOCUS":                           "",
        "__VIEWSTATE":                           vs["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR":                  vs["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":                     vs["__EVENTVALIDATION"],
        "ctl00$CtrlMenuPortal$logIn$txtUsername$txtTextBox":         "",
        "ctl00$CtrlMenuPortal$logIn$textBoxUserRecuperoContrasenia": "",
        "ctl00$CtrlMenuPortal$logIn$txtMail$txtTextBox":             "",
        "ctl00$CtrlMenuPortal$logIn$txtMail2$txtTextBox":            "",
        "ctl00$CPH1$txtNumeroProceso":           nup,
        "ctl00$CPH1$txtExpediente":              "",
        "ctl00$CPH1$txtNombrePliego":            "",
        "ctl00$CPH1$ddlJurisdicion":             SAF_ID,   # ← 591, no -2
        "ctl00$CPH1$ddlUnidadEjecutora":         "-2",
        "ctl00$CPH1$ddlTipoProceso":             "-2",
        "ctl00$CPH1$ddlEstadoProceso":           "-2",
        "ctl00$CPH1$ddlRubro":                   "-2",
        "ctl00$CPH1$devCbPnlNombreProveedor$txtNombreProveedor": "",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw":  "1702166400000",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde":      "10/12/2023",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_DDDWS": "0:0:12000:30:1584:0:0:0",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_DDD_C_FNPWS": "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde$DDD$C": "10/12/2023:10/12/2023",
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw":  ts,
        "ctl00$CPH1$devDteEdtFechaAperturaHasta":      hoy,
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_DDDWS": "0:0:12000:30:1584:0:0:0",
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_DDD_C_FNPWS": "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta$DDD$C": hoy,
        "ctl00$CPH1$ddlResultadoOrdenadoPor":    "PLI.Pliego.NumeroPliego",
        "ctl00$CPH1$ddlTipoOperacion":           "-2",
        "ctl00$CPH1$hidEstadoListaPliegos":      "NOREPORTEEXCEL",
        "ctl00$CPH1$devCbPnlPopupListarProveedor$txtPopupNombreProveedor": "",
        "ctl00$CPH1$devCbPnlPopupListarProveedor$txtPopupCuitProveedor":   "",
        "ctl00_CPH1_devPopupListarProveedorWS":  "0:0:-1:0:0:0:0:0:",
        "ctl00$CPH1$hdnFldIdProveedorSeleccionado": "",
        "ctl00_CPH1_devPopupVistaPreviaProcesoCompraCiudadanoWS": "0:0:-1:0:0:0:0:0:",
        "ctl00_CPH1_devPopupVistaPreviaPliegoWS": "0:0:-1:0:0:0:0:0:",
    }


def seleccionar_saf(session, vs, nup):
    """Paso previo obligatorio: seleccionar SAF 591 para cargar unidades ejecutoras."""
    p = payload_base(vs, nup)
    p["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$ddlJurisdicion"
    p["__EVENTTARGET"]        = "ctl00$CPH1$ddlJurisdicion"
    p["__EVENTARGUMENT"]      = ""
    r = session.post(BASE_URL, data=p, headers=HEADERS_AJAX, timeout=30, verify=False)
    actualizar_viewstate(r.text, vs)
    return r


# ── Identificar ctl correcto para el NUP ─────────────────────────────────────
def encontrar_ctl_para_nup(panel_html, nup):
    soup = BeautifulSoup(panel_html, "html.parser")
    for a in soup.find_all("a", id=re.compile(r"lnkNumeroProceso")):
        if a.get_text(strip=True) == nup:
            m = re.search(r"GridListaPliegos_(ctl\d+)_lnkNumeroProceso", a.get("id", ""))
            if m:
                return m.group(1)
    return None


# ── Buscar NUP → URL de detalle ───────────────────────────────────────────────
def obtener_url_detalle(session, vs, nup):
    for intento in range(1, MAX_RETRIES + 1):
        try:
            # PASO A: seleccionar SAF 591
            seleccionar_saf(session, vs, nup)
            time.sleep(0.5)

            # PASO B: buscar NUP dentro de SAF 591
            p = payload_base(vs, nup)
            p["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$btnListarPliegoAvanzado"
            p["__EVENTTARGET"]        = "ctl00$CPH1$btnListarPliegoAvanzado"
            p["__EVENTARGUMENT"]      = "undefined"
            p["__ASYNCPOST"]          = "true"

            r = session.post(BASE_URL, data=p, headers=HEADERS_AJAX,
                             timeout=45, verify=False)
            actualizar_viewstate(r.text, vs)

            if "GridListaPliegos" not in r.text:
                return None

            panel = extraer_panel(r.text, "ctl00_CPH1_UpdatePanel1")
            ctl   = encontrar_ctl_para_nup(panel, nup)

            if not ctl:
                log.debug(f"  {nup} no encontrado en resultados del panel")
                return None

            # PASO C: click en el link correcto
            time.sleep(0.5)
            p2 = payload_base(vs, nup)
            target = f"ctl00$CPH1$GridListaPliegos${ctl}$lnkNumeroProceso"
            p2["ctl00$ScriptManager1"] = f"ctl00$ScriptManager1|{target}"
            p2["__EVENTTARGET"]        = target
            p2["__EVENTARGUMENT"]      = ""

            r2 = session.post(BASE_URL, data=p2, headers=HEADERS_GET,
                              timeout=45, verify=False, allow_redirects=True)

            if "VistaPreviaPliegoCiudadano" in r2.url:
                return r2.url

            m = re.search(
                r"((?:https://comprar\.gob\.ar)?/PLIEGO/VistaPreviaPliegoCiudadano\.aspx\?qs=[^\s\"'<]+)",
                r2.text,
            )
            if m:
                url = m.group(1)
                if not url.startswith("http"):
                    url = DETALLE_BASE + url
                return url

            return None

        except Exception as e:
            log.warning(f"  obtener_url intento {intento} [{nup}]: {e}")
            time.sleep(DELAY_ERR * intento)

    return None


# ── Parser de detalle ─────────────────────────────────────────────────────────
def limpiar_monto(texto):
    if not texto:
        return None
    t = re.sub(r"[^\d,.]", "", texto.strip())
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
        return f"{solo[:2]}-{solo[2:10]}-{solo[10]}"
    return None


def parsear_detalle(html):
    soup = BeautifulSoup(html, "html.parser")
    resultado = {"monto_adjudicado": None, "proveedor_razon": None, "proveedor_cuit": None}

    tabla = None
    for tag in soup.find_all(["h4", "h3", "h2", "strong", "b"]):
        if "contractual" in tag.get_text().lower():
            tabla = tag.find_next("table")
            break

    if not tabla:
        for t in soup.find_all("table"):
            primera = t.find("tr")
            if not primera:
                continue
            headers = [c.get_text(strip=True).lower() for c in primera.find_all(["th", "td"])]
            if any("cuit" in h for h in headers) and any("monto" in h for h in headers):
                tabla = t
                break

    if not tabla:
        return resultado

    filas = tabla.find_all("tr")
    if not filas:
        return resultado

    col_nombre = col_cuit = col_monto = col_tipo = None
    for i, cell in enumerate(filas[0].find_all(["th", "td"])):
        t = cell.get_text(strip=True).lower()
        if "nombre" in t and "cuit" not in t:
            col_nombre = i
        elif "cuit" in t or "cuil" in t:
            col_cuit = i
        elif "monto" in t or "importe" in t:
            col_monto = i
        elif t == "tipo":
            col_tipo = i

    if col_nombre is None: col_nombre = 1
    if col_cuit   is None: col_cuit   = 2
    if col_monto  is None: col_monto  = 6

    mejor_monto = mejor_proveedor = mejor_cuit = None

    for fila in filas[1:]:
        cells = fila.find_all("td")
        if len(cells) < 4:
            continue
        nombre = cells[col_nombre].get_text(strip=True) if col_nombre < len(cells) else ""
        cuit   = limpiar_cuit(cells[col_cuit].get_text(strip=True)) if col_cuit < len(cells) else None
        monto  = limpiar_monto(cells[col_monto].get_text(strip=True)) if col_monto < len(cells) else None
        tipo   = cells[col_tipo].get_text(strip=True).lower() if col_tipo and col_tipo < len(cells) else ""

        if monto is not None:
            if tipo == "original":
                mejor_monto = monto
                mejor_proveedor = nombre
                mejor_cuit = cuit
                break
            if mejor_monto is None or monto > mejor_monto:
                mejor_monto = monto
                mejor_proveedor = nombre
                mejor_cuit = cuit

    resultado["monto_adjudicado"] = mejor_monto
    resultado["proveedor_razon"]  = mejor_proveedor[:255] if mejor_proveedor else None
    resultado["proveedor_cuit"]   = mejor_cuit
    return resultado


def obtener_detalle(session, url):
    for intento in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS_GET, timeout=30, verify=False)
            if r.status_code == 200:
                return parsear_detalle(r.text)
        except Exception as e:
            log.warning(f"  GET detalle intento {intento}: {e}")
            time.sleep(DELAY_ERR * intento)
    return None


# ── Base de datos ─────────────────────────────────────────────────────────────
def conectar_db():
    return psycopg2.connect(DATABASE_URL)


def obtener_pendientes(conn, limit=None):
    query = """
        SELECT id, numero_proceso, organismo, estado
        FROM contratos
        WHERE monto_adjudicado IS NULL
          AND detalle_scrapeado = FALSE
          AND numero_proceso IS NOT NULL
          AND trim(numero_proceso) != ''
          AND estado ILIKE '%djudicado%'
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
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

    set_clause = ", ".join(f"{k} = %s" for k in campos)
    vals = list(campos.values()) + [id_]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE contratos SET {set_clause} WHERE id = %s", vals)
    conn.commit()
    return bool(datos.get("monto_adjudicado"))


# ── Checkpoint ────────────────────────────────────────────────────────────────
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
        json.dump({
            "procesados":  list(procesados),
            "actualizado": datetime.now().isoformat(),
            "total":       len(procesados),
        }, f)


# ── Modo diagnóstico ──────────────────────────────────────────────────────────
def modo_diagnostico(nup):
    print(f"\n{'='*65}")
    print(f"DIAGNÓSTICO — NUP: {nup}")
    print(f"{'='*65}")

    session = requests.Session()
    session.verify = False

    print("\n[1] Obteniendo ViewState...")
    vs = obtener_viewstate(session)
    if not vs:
        print("ERROR: no se pudo obtener ViewState")
        return
    print(f"    OK — len={len(vs['__VIEWSTATE'])}")

    print(f"\n[2] Seleccionando SAF {SAF_ID}...")
    seleccionar_saf(session, vs, nup)
    print("    OK")

    print(f"\n[3] Buscando NUP {nup} en SAF {SAF_ID}...")
    p = payload_base(vs, nup)
    p["ctl00$ScriptManager1"] = "ctl00$ScriptManager1|ctl00$CPH1$btnListarPliegoAvanzado"
    p["__EVENTTARGET"]        = "ctl00$CPH1$btnListarPliegoAvanzado"
    p["__EVENTARGUMENT"]      = "undefined"
    p["__ASYNCPOST"]          = "true"
    r = session.post(BASE_URL, data=p, headers=HEADERS_AJAX, timeout=45, verify=False)
    actualizar_viewstate(r.text, vs)

    panel = extraer_panel(r.text, "ctl00_CPH1_UpdatePanel1")
    ctl   = encontrar_ctl_para_nup(panel, nup)
    print(f"    ctl detectado: {ctl}")

    if not ctl:
        soup = BeautifulSoup(panel, "html.parser")
        nups = [a.get_text(strip=True)
                for a in soup.find_all("a", id=re.compile(r"lnkNumeroProceso"))]
        print(f"    NUPs en resultados: {nups[:5]}")
        return

    print("\n[4] Obteniendo URL de detalle...")
    url = obtener_url_detalle(session, vs, nup)
    if not url:
        print("    ⚠️  No se obtuvo URL")
        return
    print(f"    URL: {url[:100]}...")

    print("\n[5] Descargando y parseando detalle...")
    r2 = session.get(url, headers=HEADERS_GET, timeout=30, verify=False)
    resultado = parsear_detalle(r2.text)
    print("\n    Resultado:")
    for k, v in resultado.items():
        print(f"    {k:22s}: {v}")
    print(f"\n{'='*65}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scraper detalle contratos v5")
    parser.add_argument("--diag",    type=str,  default=None)
    parser.add_argument("--limit",   type=int,  default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset",   action="store_true")
    args = parser.parse_args()

    if args.diag:
        modo_diagnostico(args.diag)
        return

    print("=" * 65)
    print("SCRAPER DETALLE v5 — COMPRAR.GOB.AR")
    print(f"   {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    if args.dry_run: print("   DRY-RUN")
    print("=" * 65)

    try:
        conn = conectar_db()
        log.info("Conectado a PostgreSQL")
    except Exception as e:
        log.error(f"DB error: {e}")
        sys.exit(1)

    pendientes = obtener_pendientes(conn, limit=args.limit)
    log.info(f"{len(pendientes)} contratos adjudicados sin monto")

    if not pendientes:
        print("No hay contratos pendientes.")
        conn.close()
        return

    procesados = set() if args.reset else cargar_checkpoint()
    pendientes = [p for p in pendientes if p["numero_proceso"] not in procesados]
    log.info(f"{len(pendientes)} a procesar (checkpoint: {len(procesados)} ya visitados)")

    session = requests.Session()
    session.verify = False

    log.info("Obteniendo ViewState...")
    vs = obtener_viewstate(session)
    if not vs:
        log.error("No se pudo obtener ViewState")
        sys.exit(1)

    total    = len(pendientes)
    ok       = 0
    sin_dato = 0
    t_inicio = time.time()

    for i, contrato in enumerate(pendientes, 1):
        nup = contrato["numero_proceso"]

        if i % REFRESH_VS_CADA == 0:
            vs_nuevo = obtener_viewstate(session)
            if vs_nuevo:
                vs = vs_nuevo
                log.info("ViewState refrescado")

        if i % 25 == 0 or i <= 3:
            elapsed = time.time() - t_inicio
            vel = i / elapsed if elapsed > 0 else 0
            eta_seg = (total - i) / vel if vel > 0 else 0
            eta = datetime.fromtimestamp(time.time() + eta_seg).strftime("%H:%M")
            log.info(f"[{i:>5}/{total}] ok={ok} sin={sin_dato} | {vel:.2f}/s ETA~{eta}")

        url = obtener_url_detalle(session, vs, nup)
        procesados.add(nup)

        if not url:
            actualizar_contrato(conn, contrato["id"], {}, dry_run=args.dry_run)
            sin_dato += 1
            time.sleep(DELAY_OK)
            continue

        time.sleep(0.4)
        resultado = obtener_detalle(session, url)

        if resultado and resultado.get("monto_adjudicado") is not None:
            actualizar_contrato(conn, contrato["id"], resultado, dry_run=args.dry_run)
            ok += 1
            if ok <= 10:
                log.info(f"  ✅ {nup}: ${resultado['monto_adjudicado']:,.0f} | "
                         f"{str(resultado.get('proveedor_razon',''))[:40]}")
        else:
            actualizar_contrato(conn, contrato["id"], {}, dry_run=args.dry_run)
            sin_dato += 1

        if i % CHECKPOINT_CADA == 0:
            guardar_checkpoint(procesados)
            log.info(f"  Checkpoint {i}/{total}")

        time.sleep(DELAY_OK)

    guardar_checkpoint(procesados)
    conn.close()

    elapsed = time.time() - t_inicio
    print(f"\n{'='*65}")
    print("RESUMEN")
    print(f"   Procesados : {total}")
    if total:
        print(f"   Con monto  : {ok}  ({ok/total*100:.1f}%)")
    print(f"   Sin datos  : {sin_dato}")
    print(f"   Tiempo     : {elapsed/60:.1f} min")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()