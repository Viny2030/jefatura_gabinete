#!/usr/bin/env python3
"""
scraper_comprar_postback.py
Scraper COMPRAR.gob.ar — PEN completo (modo append por área)
Filtro: contratos desde 10/12/2023 (gestión Milei)

Uso:
    python scripts/scraper_comprar_postback.py --area jgm
    python scripts/scraper_comprar_postback.py --area sgp
    python scripts/scraper_comprar_postback.py --area economia
    python scripts/scraper_comprar_postback.py --reset   # borra estado y empieza de cero

Estado persistido en: src/frontend/data/contratos_comprar_raw.json
                      src/frontend/data/_scraper_state.json
"""

import requests
import argparse
import json
import os
import time
import warnings
from datetime import datetime
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

# ─── Rutas ────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.join(os.path.dirname(__file__), "..", "src", "frontend", "data")
OUTPUT_JSON = os.path.join(BASE_DIR, "contratos_comprar_raw.json")
STATE_JSON  = os.path.join(BASE_DIR, "_scraper_state.json")

# ─── Constantes ───────────────────────────────────────────────────────────────
BASE_URL     = "https://comprar.gob.ar/Compras.aspx?qs=MiS2XpfDCPAf/mLOiOBgxg=="
FECHA_INICIO = "10/12/2023"
DESDE_DT     = datetime(2023, 12, 10)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-AR,es;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": BASE_URL,
}

# ─── Mapa de áreas y organismos ───────────────────────────────────────────────
AREAS = {
    "presidencia": {
        "label": "Presidencia de la Nación",
        "organismos": [
            {"clave": "presidencia",  "nombre": "Presidencia de la Nación (SAF 588)",             "saf": "588"},
            {"clave": "legal",        "nombre": "Secretaría Legal y Técnica (SAF 586)",            "saf": "586"},
            {"clave": "medios",       "nombre": "Secretaría de Comunicación y Medios (SAF 1771)",  "saf": "1771"},
            {"clave": "sgp_alt",      "nombre": "Secretaría General Presidencia (SAF 301)",        "saf": "301"},
        ],
    },
    "jgm": {
        "label": "Jefatura de Gabinete de Ministros",
        "organismos": [
            {"clave": "jgm",          "nombre": "Jefatura de Gabinete de Ministros (SAF 591)",               "saf": "591"},
            {"clave": "innovacion",   "nombre": "Secretaría de Innovación Pública (SAF 1742)",               "saf": "1742"},
            {"clave": "innov_cyt",    "nombre": "Secretaría de Innovación, Ciencia y Tecnología (SAF 1926)", "saf": "1926"},
            {"clave": "desreg",       "nombre": "Ministerio de Desregulación y Transformación del Estado (SAF 1938)", "saf": "1938"},
            {"clave": "sigen",        "nombre": "Sindicatura General de la Nación (SIGEN) (SAF 619)",        "saf": "619"},
            {"clave": "aabe",         "nombre": "Agencia de Administración de Bienes del Estado (AABE) (SAF 593)", "saf": "593"},
            {"clave": "aaip",         "nombre": "Agencia de Acceso a la Información Pública (AAIP) (SAF 1354)", "saf": "1354"},
            {"clave": "andis",        "nombre": "Agencia Nacional de Discapacidad (ANDIS) (SAF 903)",        "saf": "903"},
            {"clave": "conicet",      "nombre": "CONICET (SAF 1051)",                                        "saf": "1051"},
            {"clave": "cnea",         "nombre": "Comisión Nacional de Energía Atómica (CNEA) (SAF 933)",     "saf": "933"},
            {"clave": "conae",        "nombre": "Comisión Nacional de Actividades Espaciales (CONAE) (SAF 1316)", "saf": "1316"},
            {"clave": "ina",          "nombre": "Instituto Nacional del Agua (INA) (SAF 942)",               "saf": "942"},
            {"clave": "arn",          "nombre": "Autoridad Regulatoria Nuclear (ARN) (SAF 675)",             "saf": "675"},
            {"clave": "agencia_idi",  "nombre": "Agencia Nacional de Promoción de la Investigación y Desarrollo (AGENCIA I+D) (SAF 1829)", "saf": "1829"},
            {"clave": "lillo",        "nombre": "Fundación Miguel Lillo (SAF 912)",                          "saf": "912"},
            {"clave": "citedef",      "nombre": "Instituto de Investigaciones Científicas para la Defensa (CITEDEF) (SAF 710)", "saf": "710"},
        ],
    },
    "sgp": {
        "label": "Secretaría General de la Presidencia (SGP)",
        "organismos": [
            {"clave": "sgp",          "nombre": "Secretaría General de la Presidencia (SAF 588)",  "saf": "588"},
            {"clave": "casarosada",   "nombre": "Casa Rosada / SGP (SAF 305)",                     "saf": "305"},
        ],
    },
    "interior": {
        "label": "Ministerio del Interior",
        "organismos": [
            {"clave": "interior",     "nombre": "Ministerio del Interior (SAF 1732)",              "saf": "1732"},
            {"clave": "renaper",      "nombre": "Registro Nacional de las Personas (RENAPER) (SAF 703)", "saf": "703"},
            {"clave": "migraciones",  "nombre": "Dirección Nacional de Migraciones (SAF 697)",     "saf": "697"},
            {"clave": "inadi",        "nombre": "INADI (SAF 1725)",                                "saf": "1725"},
            {"clave": "ex_ambiente",  "nombre": "Ex Ministerio de Ambiente y Desarrollo Sostenible (SAF 1730)", "saf": "1730"},
            {"clave": "parques",      "nombre": "Administración de Parques Nacionales (SAF 910)",  "saf": "910"},
            {"clave": "acumar",       "nombre": "Autoridad de Cuenca Matanza Riachuelo (ACUMAR) (SAF 801)", "saf": "801"},
            {"clave": "ex_turismo",   "nombre": "Ex Ministerio de Turismo y Deportes (SAF 1731)",  "saf": "1731"},
            {"clave": "dep_nacional", "nombre": "Agencia de Deportes Nacional (SAF 1376)",         "saf": "1376"},
        ],
    },
    "exteriores": {
        "label": "Ministerio de Relaciones Exteriores, Comercio Internacional y Culto",
        "organismos": [
            {"clave": "exteriores",   "nombre": "Ministerio de Relaciones Exteriores, Comercio Internacional y Culto (SAF 1727)", "saf": "1727"},
        ],
    },
    "economia": {
        "label": "Ministerio de Economía",
        "organismos": [
            {"clave": "economia",     "nombre": "Ministerio de Economía (SAF 1739)",               "saf": "1739"},
            {"clave": "indec",        "nombre": "INDEC (SAF 663)",                                 "saf": "663"},
            {"clave": "cnce",         "nombre": "Comisión Nacional de Comercio Exterior (SAF 679)", "saf": "679"},
            {"clave": "cnv",          "nombre": "Comisión Nacional de Valores (CNV) (SAF 693)",    "saf": "693"},
            {"clave": "ssn",          "nombre": "Superintendencia de Seguros de la Nación (SAF 726)", "saf": "726"},
            {"clave": "tfn",          "nombre": "Tribunal Fiscal de la Nación (SAF 687)",          "saf": "687"},
            {"clave": "ttn",          "nombre": "Tribunal de Tasaciones de la Nación (SAF 752)",   "saf": "752"},
            {"clave": "ex_finanzas",  "nombre": "Ministerio de Finanzas (SAF 944)",                "saf": "944"},
            {"clave": "bice_fid",     "nombre": "BICE Fideicomisos S.A. (SAF 1378)",              "saf": "1378"},
            {"clave": "ex_agro",      "nombre": "Ex Ministerio de Agricultura, Ganadería y Pesca (SAF 1741)", "saf": "1741"},
            {"clave": "senasa",       "nombre": "SENASA (SAF 718)",                                "saf": "718"},
            {"clave": "inidep",       "nombre": "INIDEP (SAF 899)",                                "saf": "899"},
            {"clave": "inv",          "nombre": "Instituto Nacional de Vitivinicultura (INV) (SAF 1047)", "saf": "1047"},
            {"clave": "inase",        "nombre": "Instituto Nacional de Semillas (INASE) (SAF 1744)", "saf": "1744"},
            {"clave": "inta",         "nombre": "INTA (SAF 1898)",                                 "saf": "1898"},
            {"clave": "ex_prod",      "nombre": "Ex Ministerio de Desarrollo Productivo (SAF 1740)", "saf": "1740"},
            {"clave": "inti",         "nombre": "INTI (SAF 705)",                                  "saf": "705"},
            {"clave": "inpi",         "nombre": "INPI (SAF 916)",                                  "saf": "916"},
            {"clave": "sec_energia",  "nombre": "Secretaría de Energía (SAF 1733)",               "saf": "1733"},
            {"clave": "enargas",      "nombre": "ENARGAS (SAF 1265)",                              "saf": "1265"},
            {"clave": "enre",         "nombre": "ENRE (SAF 1318)",                                 "saf": "1318"},
            {"clave": "cfee",         "nombre": "Consejo Federal de la Energía Eléctrica (SAF 1571)", "saf": "1571"},
            {"clave": "enarge",       "nombre": "Ente Nacional Regulador del Gas y la Electricidad (SAF 1982)", "saf": "1982"},
            {"clave": "segemar",      "nombre": "SEGEMAR (SAF 937)",                               "saf": "937"},
        ],
    },
    "infraestructura": {
        "label": "Ministerio de Infraestructura",
        "organismos": [
            {"clave": "ex_transporte","nombre": "Ex Ministerio de Transporte (SAF 645)",            "saf": "645"},
            {"clave": "dnv",          "nombre": "Dirección Nacional de Vialidad (DNV) (SAF 708)",  "saf": "708"},
            {"clave": "anac",         "nombre": "Administración Nacional de Aviación Civil (ANAC) (SAF 742)", "saf": "742"},
            {"clave": "cnrt",         "nombre": "Comisión Nacional de Regulación del Transporte (CNRT) (SAF 712)", "saf": "712"},
            {"clave": "orsna",        "nombre": "Organismo Regulador del Sistema Nacional de Aeropuertos (ORSNA) (SAF 796)", "saf": "796"},
            {"clave": "jst",          "nombre": "Junta de Seguridad en el Transporte (SAF 794)",   "saf": "794"},
            {"clave": "orsp",         "nombre": "Organismo Regulador de Seguridad de Presas (SAF 738)", "saf": "738"},
            {"clave": "enohsa",       "nombre": "ENOHSA (SAF 720)",                                "saf": "720"},
            {"clave": "sofse",        "nombre": "Operadora Ferroviaria Sociedad del Estado (SOFSE) (SAF 1340)", "saf": "1340"},
            {"clave": "anp",          "nombre": "Agencia Nacional de Puertos y Navegación (SAF 1955)", "saf": "1955"},
            {"clave": "ex_obras",     "nombre": "Ex Ministerio de Obras Públicas (SAF 1756)",      "saf": "1756"},
            {"clave": "infra_eys",    "nombre": "Infraestructura Económica y Social (SAF 1907)",   "saf": "1907"},
            {"clave": "ex_habitat",   "nombre": "Ex Ministerio de Desarrollo Territorial y Hábitat (SAF 1757)", "saf": "1757"},
            {"clave": "ex_com",       "nombre": "Ex Ministerio de Comunicaciones (SAF 611)",       "saf": "611"},
            {"clave": "enacom",       "nombre": "ENACOM (SAF 683)",                                "saf": "683"},
            {"clave": "sec_medios",   "nombre": "Secretaría de Medios y Comunicación Pública (SAF 1737)", "saf": "1737"},
            {"clave": "defpub",       "nombre": "Defensoría del Público de Servicios de Comunicación Audiovisual (SAF 1508)", "saf": "1508"},
        ],
    },
    "justicia": {
        "label": "Ministerio de Justicia",
        "organismos": [
            {"clave": "justicia",     "nombre": "Ministerio de Justicia y Derechos Humanos (SAF 599)", "saf": "599"},
            {"clave": "cipdh",        "nombre": "Centro Internacional para la Promoción de los Derechos Humanos (SAF 792)", "saf": "792"},
            {"clave": "ansv",         "nombre": "Agencia Nacional de Seguridad Vial (ANSV) (SAF 914)", "saf": "914"},
            {"clave": "uif",          "nombre": "Unidad de Información Financiera (UIF) (SAF 691)", "saf": "691"},
        ],
    },
    "seguridad": {
        "label": "Ministerio de Seguridad",
        "organismos": [
            {"clave": "seguridad",    "nombre": "Ministerio de Seguridad (SAF 637)",               "saf": "637"},
            {"clave": "gendarmeria",  "nombre": "Gendarmería Nacional (SAF 1129)",                 "saf": "1129"},
            {"clave": "prefectura",   "nombre": "Prefectura Naval Argentina (SAF 1081)",           "saf": "1081"},
            {"clave": "psa",          "nombre": "Policía de Seguridad Aeroportuaria (PSA) (SAF 695)", "saf": "695"},
            {"clave": "spf",          "nombre": "Servicio Penitenciario Federal (SPF) (SAF 659)",  "saf": "659"},
            {"clave": "anmac",        "nombre": "Agencia Nacional de Materiales Controlados (ANMaC) (SAF 701)", "saf": "701"},
            {"clave": "sup_pfa",      "nombre": "Superintendencia de la Policía Federal (SAF 1263)", "saf": "1263"},
            {"clave": "caja_pfa",     "nombre": "Caja de Retiros Jubilaciones Policía Federal (SAF 1127)", "saf": "1127"},
            {"clave": "bien_pfa",     "nombre": "Superintendencia de Bienestar de la Policía Federal (SAF 1344)", "saf": "1344"},
            {"clave": "ente_spf",     "nombre": "Ente de Cooperación Técnica Servicio Penitenciario Federal (SAF 1832)", "saf": "1832"},
            {"clave": "os_spf",       "nombre": "Dirección de Obra Social del Servicio Penitenciario Federal (SAF 1475)", "saf": "1475"},
        ],
    },
    "defensa": {
        "label": "Ministerio de Defensa",
        "organismos": [
            {"clave": "defensa",      "nombre": "Ministerio de Defensa (SAF 647)",                 "saf": "647"},
            {"clave": "emco",         "nombre": "Estado Mayor Conjunto (EMCO) (SAF 1049)",         "saf": "1049"},
            {"clave": "ejercito",     "nombre": "Estado Mayor General del Ejército (SAF 939)",     "saf": "939"},
            {"clave": "armada",       "nombre": "Estado Mayor General de la Armada (SAF 949)",     "saf": "949"},
            {"clave": "fuerza_aerea", "nombre": "Estado Mayor General de la Fuerza Aérea (SAF 808)", "saf": "808"},
            {"clave": "logdef",       "nombre": "Subsecretaría de Planeamiento Operativo y Logística de la Defensa (SAF 1743)", "saf": "1743"},
            {"clave": "dgfm",         "nombre": "Dirección General de Fabricaciones Militares (DGFM) (SAF 728)", "saf": "728"},
            {"clave": "ign",          "nombre": "Instituto Geográfico Nacional (IGN) (SAF 724)",   "saf": "724"},
            {"clave": "smn",          "nombre": "Servicio Meteorológico Nacional (SMN) (SAF 722)", "saf": "722"},
            {"clave": "iaf",          "nombre": "Instituto de Ayuda Financiera para Retiros y Pensiones Militares (SAF 1104)", "saf": "1104"},
            {"clave": "iosfa",        "nombre": "Instituto de Obra Social de las Fuerzas Armadas (IOSFA) (SAF 1693)", "saf": "1693"},
            {"clave": "undef",        "nombre": "Universidad de la Defensa Nacional (UNDEF) (SAF 1500)", "saf": "1500"},
        ],
    },
    "salud": {
        "label": "Ministerio de Salud",
        "organismos": [
            {"clave": "salud",        "nombre": "Ministerio de Salud (SAF 1728)",                  "saf": "1728"},
            {"clave": "anmat",        "nombre": "ANMAT (SAF 1280)",                                "saf": "1280"},
            {"clave": "anlis",        "nombre": "ANLIS Dr. Carlos Malbrán (SAF 962)",              "saf": "962"},
            {"clave": "laura_bon",    "nombre": "Hospital Nacional Salud Mental Laura Bonaparte (SAF 935)", "saf": "935"},
            {"clave": "sommer",       "nombre": "Hospital Nacional Dr. Baldomero Sommer (SAF 1311)", "saf": "1311"},
            {"clave": "posadas",      "nombre": "Hospital Nacional Prof. Alejandro Posadas (SAF 1746)", "saf": "1746"},
            {"clave": "montes_oca",   "nombre": "Colonia Nacional Dr. Manuel Montes de Oca (SAF 968)", "saf": "968"},
            {"clave": "inreps",       "nombre": "Instituto Nacional de Rehabilitación Psicofísica del Sur (SAF 1747)", "saf": "1747"},
            {"clave": "snr",          "nombre": "Servicio Nacional de Rehabilitación (SAF 803)",   "saf": "803"},
            {"clave": "supersalud",   "nombre": "Superintendencia de Servicios de Salud (SAF 1748)", "saf": "1748"},
            {"clave": "inc",          "nombre": "Instituto Nacional del Cáncer (SAF 790)",         "saf": "790"},
            {"clave": "anlap",        "nombre": "Agencia Nacional de Laboratorios Públicos (ANLAP) (SAF 784)", "saf": "784"},
            {"clave": "incucai",      "nombre": "INCUCAI (SAF 1256)",                              "saf": "1256"},
            {"clave": "anes",         "nombre": "Administración Nacional de Establecimientos de Salud (SAF 1977)", "saf": "1977"},
        ],
    },
    "capital_humano": {
        "label": "Ministerio de Capital Humano",
        "organismos": [
            {"clave": "cap_humano",   "nombre": "Ministerio de Capital Humano (SAF 1906)",         "saf": "1906"},
            {"clave": "dh_es",        "nombre": "Desarrollo Humano y Economía Solidaria (SAF 1925)", "saf": "1925"},
            {"clave": "educacion",    "nombre": "Secretaría de Educación (SAF 1734)",              "saf": "1734"},
            {"clave": "coneau",       "nombre": "CONEAU (SAF 744)",                                "saf": "744"},
            {"clave": "cultura",      "nombre": "Secretaría de Cultura (SAF 1736)",               "saf": "1736"},
            {"clave": "tnc",          "nombre": "Teatro Nacional Cervantes (SAF 805)",             "saf": "805"},
            {"clave": "bn",           "nombre": "Biblioteca Nacional (SAF 1102)",                  "saf": "1102"},
            {"clave": "int_teatro",   "nombre": "Instituto Nacional del Teatro (SAF 798)",         "saf": "798"},
            {"clave": "fna",          "nombre": "Fondo Nacional de las Artes (SAF 905)",           "saf": "905"},
            {"clave": "incaa",        "nombre": "INCAA (SAF 1496)",                                "saf": "1496"},
            {"clave": "inai",         "nombre": "Instituto Nacional de Asuntos Indígenas (INAI) (SAF 665)", "saf": "665"},
            {"clave": "ex_trabajo",   "nombre": "Ex Ministerio de Trabajo, Empleo y Seguridad Social (SAF 1738)", "saf": "1738"},
            {"clave": "anses",        "nombre": "ANSES (SAF 736)",                                 "saf": "736"},
            {"clave": "senaf",        "nombre": "Secretaría Nacional de Niñez, Adolescencia y Familia (SENAF) (SAF 740)", "saf": "740"},
            {"clave": "cncp",         "nombre": "Consejo Nacional de Coordinación de Políticas Sociales (SAF 661)", "saf": "661"},
            {"clave": "srt",          "nombre": "Superintendencia de Riesgos del Trabajo (SRT) (SAF 1745)", "saf": "1745"},
            {"clave": "pami",         "nombre": "INSSJP (PAMI) (SAF 1383)",                       "saf": "1383"},
            {"clave": "inam",         "nombre": "Instituto Nacional de las Mujeres (SAF 908)",     "saf": "908"},
            {"clave": "ex_mujeres",   "nombre": "Ex Ministerio de las Mujeres, Géneros y Diversidad (SAF 1754)", "saf": "1754"},
            {"clave": "inaes",        "nombre": "Instituto Nacional de Asociativismo y Economía Social (INAES) (SAF 734)", "saf": "734"},
        ],
    },
    "control": {
        "label": "Organismos de Control",
        "organismos": [
            {"clave": "agn",          "nombre": "Auditoría General de la Nación (AGN) (SAF 1764)", "saf": "1764"},
            {"clave": "bndg",         "nombre": "Banco Nacional de Datos Genéticos (SAF 1425)",    "saf": "1425"},
        ],
    },
}

AREA_ORDER = [
    "presidencia", "jgm", "sgp", "interior", "exteriores",
    "economia", "infraestructura", "justicia", "seguridad",
    "defensa", "salud", "capital_humano", "control",
]


# ─── Estado ───────────────────────────────────────────────────────────────────

def _load_state():
    """
    Carga estado persistido.
    Fusiona _scraper_state.json con scraper_progreso.json (formato anterior)
    para no perder el progreso previo.
    """
    PROGRESO_JSON = os.path.join(BASE_DIR, "scraper_progreso.json")

    areas = []

    # Leer formato anterior si existe
    if os.path.exists(PROGRESO_JSON):
        with open(PROGRESO_JSON, encoding="utf-8") as f:
            prog = json.load(f)
            areas = prog.get("areas_completadas", [])

    # Leer estado nuevo si existe
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            st = json.load(f)
            for a in st.get("areas_completadas", []):
                if a not in areas:
                    areas.append(a)

    return {"areas_completadas": areas, "total": 0}


def _save_state(state):
    os.makedirs(BASE_DIR, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _load_contratos():
    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_contratos(contratos):
    os.makedirs(BASE_DIR, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(contratos, f, ensure_ascii=False, indent=2)


# ─── HTTP / Scraping ──────────────────────────────────────────────────────────

def _get_viewstate(session):
    r = session.get(BASE_URL, headers=HEADERS, timeout=30, verify=False)
    soup = BeautifulSoup(r.text, "html.parser")
    vs   = soup.find("input", {"id": "__VIEWSTATE"})
    evv  = soup.find("input", {"id": "__EVENTVALIDATION"})
    vsg  = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
    return {
        "__VIEWSTATE":          vs["value"]  if vs  else "",
        "__EVENTVALIDATION":    evv["value"] if evv else "",
        "__VIEWSTATEGENERATOR": vsg["value"] if vsg else "",
    }


def _build_payload(viewstate, saf_id, eventtarget="", eventarg="", page=1):
    today = datetime.today().strftime("%d/%m/%Y")
    return {
        "__EVENTTARGET":          eventtarget,
        "__EVENTARGUMENT":        eventarg,
        "__VIEWSTATE":            viewstate["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR":   viewstate["__VIEWSTATEGENERATOR"],
        "__EVENTVALIDATION":      viewstate["__EVENTVALIDATION"],
        # Filtros de búsqueda
        "ctl00$MainContent$FiltroCompras1$txtFechaDesde":   FECHA_INICIO,
        "ctl00$MainContent$FiltroCompras1$txtFechaHasta":   today,
        "ctl00$MainContent$FiltroCompras1$txtFechaDesde_I": FECHA_INICIO,
        "ctl00$MainContent$FiltroCompras1$txtFechaHasta_I": today,
        "ctl00$MainContent$FiltroCompras1$ddlOrganismo":    saf_id,
        "ctl00$MainContent$FiltroCompras1$ddlOrganismo_I":  saf_id,
        "ctl00$MainContent$FiltroCompras1$ddlEstado":       "",
        "ctl00$MainContent$FiltroCompras1$txtNroProceso":   "",
        "ctl00$MainContent$FiltroCompras1$txtObjeto":       "",
        "ctl00$MainContent$FiltroCompras1$btnBuscar":       "Buscar" if not eventtarget else "",
    }


def _parse_cantidad(soup):
    """Extrae la cantidad total de resultados del mensaje de la página."""
    for tag in soup.find_all(["span", "div", "p"]):
        txt = tag.get_text(strip=True)
        if "resultados" in txt.lower() and "encontrado" in txt.lower():
            return txt
    return "?"


def _parse_grid(soup, area_key, saf_id, nombre_org):
    """
    Parsea la tabla de resultados y devuelve lista de contratos.
    Cada fila tiene: numero_proceso, nombre_proceso, expediente,
                     fecha_apertura, estado, area, saf, organismo.
    """
    contratos = []

    # Buscar la tabla del grid
    grid = soup.find("table", id=lambda x: x and "Grid" in x)
    if not grid:
        # Fallback: primera tabla con filas de datos
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            if len(rows) > 2:
                grid = tbl
                break

    if not grid:
        return contratos

    rows = grid.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        texts = [c.get_text(strip=True) for c in cells]
        primer_link = row.find("a", href=True)
        url_detalle = ("https://comprar.gob.ar" + primer_link["href"]) if primer_link else None
        if len(cells) < 4:
            continue

        # Filtrar filas de paginación (solo números) o cabecera
        if not texts[0] or texts[0].isdigit():
            continue
        # Detectar fila de cabecera
        if any(h in texts[0].lower() for h in ["proceso", "número", "objeto"]):
            continue

        # Columnas reales de comprar.gob.ar:
        # [0] Número de Proceso
        # [1] Nombre descriptivo de Proceso
        # [2] Tipo de Proceso
        # [3] Fecha de Apertura  (ej: "09/04/2026 07:00 Hrs.")
        # [4] Estado
        # [5] Unidad Ejecutora
        # [6] Servicio Administrativo Financiero
        if len(texts) < 4:
            continue

        numero   = texts[0]
        objeto   = texts[1]
        tipo     = texts[2] if len(texts) > 2 else ""
        fecha    = texts[3] if len(texts) > 3 else ""
        estado   = texts[4] if len(texts) > 4 else ""
        unidad   = texts[5] if len(texts) > 5 else ""
        saf_txt  = texts[6] if len(texts) > 6 else ""

        # Descartar si el número no parece un proceso (ej: "12345678910...")
        if not numero or not any(c == "-" for c in numero):
            continue

        # Limpiar fecha — viene como "09/04/2026 07:00 Hrs."
        fecha_limpia = fecha[:10].strip()

        # Filtrar por fecha (período Milei)
        en_periodo = False
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                f = datetime.strptime(fecha_limpia, fmt)
                if f >= DESDE_DT:
                    en_periodo = True
                break
            except ValueError:
                continue

        if not en_periodo:
            continue

        contratos.append({
            "numero_proceso": numero,
            "nombre_proceso": objeto,
            "tipo_proceso":   tipo,
            "fecha_apertura": fecha_limpia,
            "estado":         estado,
            "unidad_ejecutora": unidad,
            "saf_texto":      saf_txt,
            "area":           area_key,
            "saf":            saf_id,
            "organismo":      nombre_org,
            "url_detalle": url_detalle,
        })

    return contratos


def _get_total_pages(soup, grid_id):
    """Devuelve lista de argumentos de paginación ('Page$2', 'Page$3', ...)."""
    paginas = []
    # Buscar links de paginación en el grid
    grid = soup.find("table", id=grid_id)
    if not grid:
        return paginas
    for a in grid.find_all("a", href=True):
        href = a.get("href", "")
        if "Page$" in href:
            arg = href.split("'")[1] if "'" in href else href
            if arg not in paginas:
                paginas.append(arg)
    # También buscar en javascript:__doPostBack
    for a in grid.find_all("a"):
        onclick = a.get("onclick", "")
        if "Page$" in onclick:
            try:
                arg = onclick.split("'")[3]
                if arg.startswith("Page$") and arg not in paginas:
                    paginas.append(arg)
            except Exception:
                pass
    return paginas


def _get_grid_id(soup):
    """Detecta el ID del GridView de resultados."""
    for tbl in soup.find_all("table", id=True):
        tid = tbl.get("id", "")
        if "Grid" in tid or "grd" in tid.lower() or "Listado" in tid:
            return tid
    return ""


def scrape_organismo(session, saf_id, nombre_org, area_key, viewstate):
    """
    Scrapa todos los contratos del período Milei para un organismo dado.
    Retorna lista de contratos.
    """
    todos = []

    # Búsqueda inicial
    payload = _build_payload(viewstate, saf_id)
    try:
        resp = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=45, verify=False)
    except Exception as e:
        print(f"    ERROR al conectar: {e}")
        return todos

    soup = BeautifulSoup(resp.text, "html.parser")

    cant_txt = _parse_cantidad(soup)
    print(f"    {cant_txt}")

    grid_id = _get_grid_id(soup)

    # Página 1
    pag1 = _parse_grid(soup, area_key, saf_id, nombre_org)
    todos.extend(pag1)
    print(f"    Pág 1: {len(pag1)} en período Milei")

    # Paginación dinámica — navegamos página a página
    # En comprar.gob.ar la paginación es __doPostBack sobre el grid
    pagina_actual = 1
    while True:
        pagina_actual += 1
        # Construir arg de paginación
        page_arg = f"Page${pagina_actual}"
        target   = grid_id.replace("_", "$") if grid_id else "ctl00$MainContent$grdResultados"

        payload_pag = _build_payload(viewstate, saf_id,
                                     eventtarget=target,
                                     eventarg=page_arg)
        try:
            r_pag = session.post(BASE_URL, data=payload_pag, headers=HEADERS, timeout=45, verify=False)
        except Exception as e:
            print(f"    ERROR pág {pagina_actual}: {e}")
            break

        soup_pag = BeautifulSoup(r_pag.text, "html.parser")

        # Actualizar viewstate para la siguiente iteración
        vs_new = soup_pag.find("input", {"id": "__VIEWSTATE"})
        ev_new = soup_pag.find("input", {"id": "__EVENTVALIDATION"})
        if vs_new:
            viewstate["__VIEWSTATE"] = vs_new["value"]
        if ev_new:
            viewstate["__EVENTVALIDATION"] = ev_new["value"]

        pag_contratos = _parse_grid(soup_pag, area_key, saf_id, nombre_org)
        print(f"    Pág {pagina_actual}: {len(pag_contratos)} en período Milei")

        todos.extend(pag_contratos)

        # Condición de parada: si 10 páginas consecutivas sin resultados
        # Detectar si el botón de "siguiente" desapareció
        hay_siguiente = False
        grid_pag = soup_pag.find("table", id=grid_id) if grid_id else None
        if grid_pag:
            for a in grid_pag.find_all("a"):
                texto = a.get_text(strip=True)
                onclick = a.get("onclick", "")
                if f"Page${pagina_actual + 1}" in onclick or texto == str(pagina_actual + 1) or texto == ">":
                    hay_siguiente = True
                    break

        if not hay_siguiente:
            # Verificar también si la página que acabamos de recibir
            # repite exactamente los mismos datos que la página anterior
            # (señal de que llegamos al final)
            break

        time.sleep(0.6)

    print(f"    Subtotal: {len(todos)}")
    return todos


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Scraper COMPRAR.gob.ar — modo append por área")
    parser.add_argument("--area",  choices=list(AREAS.keys()), help="Área a procesar")
    parser.add_argument("--reset", action="store_true",        help="Borra estado y empieza de cero")
    parser.add_argument("--list",  action="store_true",        help="Lista áreas disponibles")
    args = parser.parse_args()

    if args.list:
        print("\nÁreas disponibles:")
        for k, v in AREAS.items():
            n = len(v["organismos"])
            print(f"  --area {k:<20} ({n} organismos)  {v['label']}")
        print()
        return

    if args.reset:
        for f in [OUTPUT_JSON, STATE_JSON]:
            if os.path.exists(f):
                os.remove(f)
                print(f"  Eliminado: {f}")
        print("  Estado reiniciado.\n")
        return

    if not args.area:
        parser.print_help()
        return

    area_key  = args.area
    area_info = AREAS[area_key]

    # ── Estado
    state     = _load_state()
    contratos = _load_contratos()

    completadas = state.get("areas_completadas", [])

    print("\n" + "═" * 60)
    print("🔍 SCRAPER COMPRAR — PEN COMPLETO (modo append)")
    print(f"   Filtro: desde {FECHA_INICIO} (gestión Milei)")
    print(f"   Organismos disponibles: {sum(len(a['organismos']) for a in AREAS.values())}")
    print("═" * 60)
    print(f"\n  Contratos ya acumulados: {len(contratos)}")
    print(f"  Áreas ya completadas: {completadas}")

    if area_key in completadas:
        print(f"\n  ⚠️  Área '{area_key}' ya fue procesada.")
        print("  Usá --reset si querés volver a scrapearla.")
        return

    print(f"\n{'─'*60}")
    print(f"  {area_info['label']} ({len(area_info['organismos'])} organismos)")
    print(f"{'─'*60}\n")

    session = requests.Session()
    try:
        viewstate = _get_viewstate(session)
    except Exception as e:
        print(f"  ERROR obteniendo ViewState: {e}")
        return

    nuevos_area = []

    for org in area_info["organismos"]:
        print(f"  [{area_key}] {org['nombre']}")
        time.sleep(0.5)
        res = scrape_organismo(session, org["saf"], org["nombre"], area_key, viewstate)
        nuevos_area.extend(res)
        time.sleep(1)

    # Persistir
    contratos.extend(nuevos_area)
    _save_contratos(contratos)

    completadas.append(area_key)
    state["areas_completadas"] = completadas
    state["total"] = len(contratos)
    _save_state(state)

    # ── Resumen
    areas_totales = len(AREAS)
    idx_actual    = AREA_ORDER.index(area_key) if area_key in AREA_ORDER else -1
    siguiente     = AREA_ORDER[idx_actual + 1] if idx_actual >= 0 and idx_actual + 1 < len(AREA_ORDER) else None

    print("\n" + "═" * 60)
    print(f"  Nuevos contratos esta corrida: {len(nuevos_area)}")
    print(f"  TOTAL acumulado: {len(contratos)}")
    print(f"\n  JSONs guardados en: {os.path.abspath(BASE_DIR)}")
    print(f"\n  Áreas completadas: {len(completadas)}/{areas_totales}")
    if siguiente and siguiente not in completadas:
        print(f"  Próxima área a correr:")
        print(f"    python scripts/scraper_comprar_postback.py --area {siguiente}")

    if nuevos_area:
        print(f"\n  Muestra:")
        for c in nuevos_area[:3]:
            print(f"    {c['numero_proceso']} | {area_key} | {c['nombre_proceso'][:35]} | {c['fecha_apertura'][:10]}")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
