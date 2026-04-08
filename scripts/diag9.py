"""
diag9.py — test filtro de fecha en formulario COMPRAR
Prueba si el campo FechaAperturaDesde filtra correctamente desde 10/12/2023
"""
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
GRID_ID  = "ctl00_CPH1_GridListaPliegos"
SAF      = "588"  # SGP — 1060 resultados sin filtro, buen caso de prueba

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://comprar.gob.ar",
    "Referer": BASE_URL,
}

session = requests.Session()
r = session.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
soup = BeautifulSoup(r.text, "html.parser")
vs = {
    "__VIEWSTATE":          soup.find("input", {"id": "__VIEWSTATE"})["value"],
    "__VIEWSTATEGENERATOR": soup.find("input", {"id": "__VIEWSTATEGENERATOR"})["value"],
}

# Intentar varias combinaciones de formato de fecha
formatos = [
    {
        "desc": "Formato DD/MM/YYYY",
        "desde": "10/12/2023",
        "desde_i": "10/12/2023",
        "raw": "S",
    },
    {
        "desc": "Formato YYYY-MM-DD",
        "desde": "2023-12-10",
        "desde_i": "2023-12-10",
        "raw": "S",
    },
    {
        "desc": "Solo desde_I vacío, desde con fecha",
        "desde": "10/12/2023",
        "desde_i": "",
        "raw": "N",
    },
    {
        "desc": "Ambos vacíos (control — sin filtro)",
        "desde": "",
        "desde_i": "",
        "raw": "N",
    },
]

for fmt in formatos:
    print(f"\n{'─'*50}")
    print(f"Probando: {fmt['desc']}")
    print(f"  desde='{fmt['desde']}' | desde_I='{fmt['desde_i']}' | raw='{fmt['raw']}'")

    # Fresh session para cada intento
    session2 = requests.Session()
    r0 = session2.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
    soup0 = BeautifulSoup(r0.text, "html.parser")
    vs2 = {
        "__VIEWSTATE":          soup0.find("input", {"id": "__VIEWSTATE"})["value"],
        "__VIEWSTATEGENERATOR": soup0.find("input", {"id": "__VIEWSTATEGENERATOR"})["value"],
    }

    payload = {
        "__EVENTTARGET":        "ctl00$CPH1$btnListarPliegoAvanzado",
        "__EVENTARGUMENT":      "",
        "__LASTFOCUS":          "",
        "__VIEWSTATE":          vs2["__VIEWSTATE"],
        "__VIEWSTATEGENERATOR": vs2["__VIEWSTATEGENERATOR"],
        "ctl00$CPH1$txtNumeroProceso":               "",
        "ctl00$CPH1$txtExpediente":                  "",
        "ctl00$CPH1$txtNombrePliego":                "",
        "ctl00$CPH1$ddlJurisdicion":                 SAF,
        "ctl00$CPH1$ddlUnidadEjecutora":             "-2",
        "ctl00$CPH1$ddlTipoProceso":                 "-2",
        "ctl00$CPH1$ddlEstadoProceso":               "-2",
        "ctl00$CPH1$ddlRubro":                       "-2",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde":    fmt["desde"],
        "ctl00$CPH1$devDteEdtFechaAperturaDesde_I":  fmt["desde_i"],
        "ctl00$CPH1$devDteEdtFechaAperturaHasta":    "",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta_I":  "",
        "ctl00$CPH1$ddlResultadoOrdenadoPor":        "PLI.PliegoCronograma.FechaActoApertura",
        "ctl00$CPH1$ddlTipoOperacion":               "-2",
        "ctl00$CPH1$hidEstadoListaPliegos":          "NOREPORTEEXCEL",
        "ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw": fmt["raw"],
        "ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw": "N",
    }

    r2 = session2.post(BASE_URL, data=payload, headers=HEADERS, timeout=30, verify=False)
    soup2 = BeautifulSoup(r2.text, "html.parser")

    lbl = soup2.find(id="ctl00_CPH1_lblCantidadListaPliegos")
    total = lbl.get_text(strip=True) if lbl else "No encontrado"
    print(f"  → Resultado: {total}")

    # Ver primeras fechas de la tabla
    tabla = soup2.find("table", {"id": GRID_ID})
    if tabla:
        filas = tabla.find_all("tr")[1:4]  # primeras 3 filas de datos
        for fila in filas:
            celdas = fila.find_all("td")
            if len(celdas) == 8:
                print(f"     Fecha: {celdas[4].get_text(strip=True)[:10]} | Nro: {celdas[0].get_text(strip=True)}")
    else:
        print("  → Sin tabla de resultados")