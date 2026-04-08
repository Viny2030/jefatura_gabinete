"""
diag5.py — captura HTML crudo del paginador para debug de paginación
"""
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
GRID_ID  = "ctl00_CPH1_GridListaPliegos"
HEADERS  = {
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

# SAF 1742 (Innovación) — 382 resultados, paginador más manejable
payload = {
    "__EVENTTARGET":        "ctl00$CPH1$btnListarPliegoAvanzado",
    "__EVENTARGUMENT":      "",
    "__LASTFOCUS":          "",
    "__VIEWSTATE":          vs["__VIEWSTATE"],
    "__VIEWSTATEGENERATOR": vs["__VIEWSTATEGENERATOR"],
    "ctl00$CPH1$txtNumeroProceso":               "",
    "ctl00$CPH1$txtExpediente":                  "",
    "ctl00$CPH1$txtNombrePliego":                "",
    "ctl00$CPH1$ddlJurisdicion":                 "1742",
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

print("Haciendo búsqueda SAF 1742...")
r2 = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30, verify=False)
soup2 = BeautifulSoup(r2.text, "html.parser")
vs["__VIEWSTATE"] = soup2.find("input", {"id": "__VIEWSTATE"})["value"]

tabla = soup2.find("table", {"id": GRID_ID})
if tabla:
    filas = tabla.find_all("tr")
    ultima_fila = filas[-1]
    print("\n=== HTML PAGINADOR PÁG 1 ===")
    print(ultima_fila.prettify())

    # Saltar al bloque siguiente via Page$11
    payload2 = payload.copy()
    payload2["__EVENTTARGET"]   = "ctl00$CPH1$GridListaPliegos"
    payload2["__EVENTARGUMENT"] = "Page$11"
    payload2["__VIEWSTATE"]     = vs["__VIEWSTATE"]

    print("\nSaltando a Page$11...")
    r3 = session.post(BASE_URL, data=payload2, headers=HEADERS, timeout=30, verify=False)
    soup3 = BeautifulSoup(r3.text, "html.parser")
    vs["__VIEWSTATE"] = soup3.find("input", {"id": "__VIEWSTATE"})["value"]

    tabla3 = soup3.find("table", {"id": GRID_ID})
    if tabla3:
        ultima_fila3 = tabla3.find_all("tr")[-1]
        print("\n=== HTML PAGINADOR PÁG 11 ===")
        print(ultima_fila3.prettify())

        # Saltar al bloque siguiente via Page$21
        payload3 = payload2.copy()
        payload3["__EVENTARGUMENT"] = "Page$21"
        payload3["__VIEWSTATE"]     = vs["__VIEWSTATE"]

        print("\nSaltando a Page$21...")
        r4 = session.post(BASE_URL, data=payload3, headers=HEADERS, timeout=30, verify=False)
        soup4 = BeautifulSoup(r4.text, "html.parser")
        tabla4 = soup4.find("table", {"id": GRID_ID})
        if tabla4:
            ultima_fila4 = tabla4.find_all("tr")[-1]
            print("\n=== HTML PAGINADOR PÁG 21 ===")
            print(ultima_fila4.prettify())
        else:
            print("Tabla no encontrada en pág 21")
    else:
        print("Tabla no encontrada en pág 11")
else:
    print("Tabla no encontrada en pág 1")