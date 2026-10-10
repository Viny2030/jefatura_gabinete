"""
gestiones.py
============
Fuente única para:
  - las gestiones de Jefatura de Gabinete (fecha de inicio de cada una), y
  - la fecha de designación que se lee de la norma publicada en el BORA.

La usan scraper_nomina.py y corregir_personal.py, para que la "gestión al
ingreso" se calcule igual en todos lados.
"""

import re
from datetime import date

# (nombre, fecha de inicio). Ordenadas de la más reciente a la más antigua.
GESTIONES = [
    ("Santilli", date(2026, 6, 30)),
    ("Adorni",   date(2025, 11, 4)),
    ("Francos",  date(2024, 5, 27)),
    ("Posse",    date(2023, 12, 10)),
]
PRE_MILEI = "Pre-Milei"
DESCONOCIDO = "Desconocido"

# Las URL del BORA terminan en /AAAAMMDD, a veces seguidas de "?busqueda=1",
# ")" o ";". Antes se exigía "?" o un espacio después y casi nunca coincidía.
_RE_FECHA_URL = re.compile(r"/(?:detalleAviso/\w+|#!DetalleNorma)/\d+/(\d{8})(?!\d)")
_RE_NORMA_ANIO = re.compile(r"\b\d{1,5}/(\d{4})\b")


def gestion_para_fecha(d):
    """Gestión de JGM vigente en la fecha `d` (datetime.date)."""
    if d is None:
        return DESCONOCIDO
    for nombre, desde in GESTIONES:
        if d >= desde:
            return nombre
    return PRE_MILEI


def gestion_para_anio(anio):
    """Sólo con el año se puede asignar gestión si ese año entero cae en una
    sola. Si no, se devuelve "Desconocido" en vez de adivinar."""
    if anio is None:
        return DESCONOCIDO
    if anio <= 2022:
        return PRE_MILEI
    return DESCONOCIDO  # 2023-2026 tienen más de una gestión dentro del año


def fecha_designacion(norma):
    """Fecha (date) de la designación más antigua citada en `norma`, leída de la
    URL del BORA. Devuelve None si no hay fecha completa: no se inventa el 1/1."""
    if not norma or not isinstance(norma, str):
        return None
    fechas = []
    for m in _RE_FECHA_URL.finditer(norma):
        s = m.group(1)
        try:
            fechas.append(date(int(s[:4]), int(s[4:6]), int(s[6:])))
        except ValueError:
            continue
    return min(fechas) if fechas else None


def anio_norma(norma):
    """Año de la norma ("Decreto 784/2025" → 2025) cuando no hay fecha completa."""
    if not norma or not isinstance(norma, str):
        return None
    anios = [int(a) for a in _RE_NORMA_ANIO.findall(norma) if 1990 <= int(a) <= 2100]
    return min(anios) if anios else None


def gestion_para_norma(norma):
    """(fecha ISO o None, gestión) a partir del texto de la norma de designación."""
    d = fecha_designacion(norma)
    if d is not None:
        return d.isoformat(), gestion_para_fecha(d)
    return None, gestion_para_anio(anio_norma(norma))
