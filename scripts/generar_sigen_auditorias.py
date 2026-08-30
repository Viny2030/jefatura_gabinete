"""
generar_sigen_auditorias.py
============================
Genera src/frontend/data/sigen_auditorias.json a partir del listado público
de informes de auditoría que publica SIGEN (Sindicatura General de la
Nación) en su Archivo Web.

Fuente: https://www.sigen.gob.ar/ArchivoWeb/Informes.aspx

Importante — verificado contra el contenido real de la fuente: este listado
son los informes de la Unidad de Auditoría Interna DE LA PROPIA SIGEN sobre
su propia gestión (SAF 109) — cierres de ejercicio, certificaciones de
remanentes presupuestarios, compras y contrataciones, DDJJ patrimoniales del
personal de SIGEN, etc. NO son auditorías de SIGEN sobre otros organismos
del Poder Ejecutivo (JGM, Presidencia, SGP). No se encontró en esta página
un filtro por organismo auditado que permita listar auditorías de terceros,
así que no hay que presentar este dato como si cubriera a otros organismos.

Uso:
    python scripts/generar_sigen_auditorias.py
    python scripts/generar_sigen_auditorias.py --output-dir src/frontend/data
"""

import argparse
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

URL_INFORMES = "https://www.sigen.gob.ar/ArchivoWeb/Informes.aspx"
URL_VER_BASE = "https://www.sigen.gob.ar/ArchivoWeb/ArchivosAdjuntos_Ver.aspx?IdDocumento="

# Cada fila de la grilla es un <tr> con dos <td>: descripción del informe, y
# un botón "Ver Documento" con data-src="ArchivosAdjuntos_Ver.aspx?IdDocumento=NNN"
FILA_RE = re.compile(
    r'<b>INFORME N°:\s*([\d/]+)</b>\s*-\s*(.*?)</td>.*?IdDocumento=(\d+)',
    re.IGNORECASE | re.DOTALL,
)


def descargar_html(url: str) -> str:
    log.info("Descargando %s ...", url)
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (MonitorEjecutivoJGM/1.0; transparencia publica)"})
    with urlopen(req, timeout=60) as r:
        html = r.read().decode("utf-8", errors="replace")
    log.info("  -> %.0f KB descargados", len(html) / 1024)
    return html


def limpiar_texto(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("&nbsp;", " ").replace("&aacute;", "á").replace("&eacute;", "é")
    s = s.replace("&iacute;", "í").replace("&oacute;", "ó").replace("&uacute;", "ú")
    s = s.replace("&ntilde;", "ñ").replace("&Ntilde;", "Ñ").replace("&#39;", "'")
    s = s.replace("&amp;", "&").replace("&quot;", '"')
    return s


def parsear_informes(html: str) -> list[dict]:
    informes = []
    for m in FILA_RE.finditer(html):
        numero, descripcion, doc_id = m.groups()
        anio_m = re.search(r"/(\d{4})$", numero)
        anio = int(anio_m.group(1)) if anio_m else None
        informes.append({
            "numero": numero,
            "anio": anio,
            "descripcion": limpiar_texto(descripcion),
            "documento_id": doc_id,
            "url": f"{URL_VER_BASE}{doc_id}",
        })
    # Orden descendente por año y luego por número de informe dentro del año
    def _clave(i):
        try:
            n = int(i["numero"].split("/")[0])
        except (ValueError, IndexError):
            n = 0
        return (i["anio"] or 0, n)
    informes.sort(key=_clave, reverse=True)
    return informes


def main():
    parser = argparse.ArgumentParser(description="Genera sigen_auditorias.json desde el Archivo Web de SIGEN")
    parser.add_argument("--output-dir", type=Path, default=Path("src/frontend/data"))
    parser.add_argument("--html-local", type=Path, default=None, help="Usar un HTML ya descargado en vez de bajarlo")
    args = parser.parse_args()

    try:
        html = args.html_local.read_text(encoding="utf-8", errors="replace") if args.html_local else descargar_html(URL_INFORMES)
    except URLError as e:
        log.error("No se pudo descargar el listado de SIGEN: %s", e)
        raise SystemExit(1)

    informes = parsear_informes(html)
    if not informes:
        log.error("No se encontraron informes en la página — puede haber cambiado el HTML de SIGEN.")
        raise SystemExit(1)

    anios = sorted({i["anio"] for i in informes if i["anio"]})
    resultado = {
        "total": len(informes),
        "anio_desde": anios[0] if anios else None,
        "anio_hasta": anios[-1] if anios else None,
        "informes": informes,
        "fuente": "Sindicatura General de la Nación (SIGEN) — Archivo Web de informes",
        "fuente_url": URL_INFORMES,
        "generado": datetime.now().isoformat(timespec="seconds"),
        "nota": (
            "Informes de la Unidad de Auditoría Interna de la propia SIGEN sobre su gestión "
            "(SAF 109): cierres de ejercicio, certificaciones presupuestarias, compras y "
            "contrataciones, declaraciones juradas patrimoniales del personal, etc. No son "
            "auditorías de SIGEN sobre otros organismos del Poder Ejecutivo — la fuente no "
            "publica ese cruce en esta página. No implica acusación ni determinación de "
            "responsabilidad sobre ninguna persona."
        ),
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_path = args.output_dir / "sigen_auditorias.json"
    out_path.write_text(json.dumps(resultado, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Guardado: %s (%d informes, %s-%s)", out_path, len(informes), resultado["anio_desde"], resultado["anio_hasta"])


if __name__ == "__main__":
    main()
