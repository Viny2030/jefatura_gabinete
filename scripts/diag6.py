"""
diag6.py — captura HTML paginador SAF 588 (Presidencia)
"""
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
GRID_ID  = "ctl00_CPH1_GridListaPliegos"
SAF      = "588"   # Presidencia — 1060 resultados, salta a pág 73

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

payload = {
    "__EVENTTARGET":        "ctl00$CPH1$btnListarPliegoAvanzado",
    "__EVENTARGUMENT":      "",
    "__LASTFOCUS":          "",
    "__VIEWSTATE":          vs["__VIEWSTATE"],
    "__VIEWSTATEGENERATOR": vs["__VIEWSTATEGENERATOR"],
    "ctl00$CPH1$txtNumeroProceso":               "",
    "ctl00$CPH1$txtExpediente":                  "",
    "ctl00$CPH1$txtNombrePliego":                "",
    "ctl00$CPH1$ddlJurisdicion":                 SAF,
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

print(f"Buscando SAF {SAF}...")
r2 = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30, verify=False)
soup2 = BeautifulSoup(r2.text, "html.parser")
vs["__VIEWSTATE"] = soup2.find("input", {"id": "__VIEWSTATE"})["value"]

tabla = soup2.find("table", {"id": GRID_ID})
if tabla:
    ultima_fila = tabla.find_all("tr")[-1]
    print("\n=== HTML PAGINADOR PÁG 1 ===")
    print(ultima_fila.prettify())

    # Saltar a Page$2 (siguiente página normal)
    p2 = payload.copy()
    p2["__EVENTTARGET"]   = "ctl00$CPH1$GridListaPliegos"
    p2["__EVENTARGUMENT"] = "Page$2"
    p2["__VIEWSTATE"]     = vs["__VIEWSTATE"]

    print("\nSaltando a Page$2...")
    r3 = session.post(BASE_URL, data=p2, headers=HEADERS, timeout=30, verify=False)
    soup3 = BeautifulSoup(r3.text, "html.parser")
    tabla3 = soup3.find("table", {"id": GRID_ID})
    if tabla3:
        uf3 = tabla3.find_all("tr")[-1]
        print("\n=== HTML PAGINADOR PÁG 2 ===")
        print(uf3.prettify())
    else:
        print("Tabla no encontrada en pág 2")
else:
    print("Tabla no encontrada en pág 1")