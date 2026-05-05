import requests, re, time, warnings
from bs4 import BeautifulSoup
warnings.filterwarnings("ignore")

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
HEADERS_GET = {"User-Agent": "Mozilla/5.0"}
HEADERS_AJAX = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "X-MicrosoftAjax": "Delta=true",
}

def extraer_vs(soup):
    def v(id_):
        el = soup.find("input", {"id": id_})
        return el["value"] if el else ""
    return {
        "__VIEWSTATE":          v("__VIEWSTATE"),
        "__EVENTVALIDATION":    v("__EVENTVALIDATION"),
        "__VIEWSTATEGENERATOR": v("__VIEWSTATEGENERATOR"),
    }

session = requests.Session()
print("GET inicial...")
r0 = session.get(BASE_URL, headers=HEADERS_GET, verify=False, timeout=30)
vs = extraer_vs(BeautifulSoup(r0.text, "html.parser"))
print("ViewState OK\n")

for saf in ["300","301","302","303","304","306","307","308","309","322","337","338","591"]:
    p = {
        "__VIEWSTATE":          vs["__VIEWSTATE"],
        "__EVENTVALIDATION":    vs["__EVENTVALIDATION"],
        "__VIEWSTATEGENERATOR": vs["__VIEWSTATEGENERATOR"],
        "ctl00$CPH1$ddlJurisdicion":            saf,
        "ctl00$ScriptManager1":                 "ctl00$ScriptManager1|ctl00$CPH1$btnListarPliegoAvanzado",
        "__EVENTTARGET":                        "ctl00$CPH1$btnListarPliegoAvanzado",
        "__EVENTARGUMENT":                      "undefined",
        "__ASYNCPOST":                          "true",
        "ctl00$CPH1$hidEstadoListaPliegos":     "NOREPORTEEXCEL",
        "ctl00$CPH1$ddlUnidadEjecutora":        "-2",
        "ctl00$CPH1$ddlTipoProceso":            "-2",
        "ctl00$CPH1$ddlEstadoProceso":          "-2",
        "ctl00$CPH1$ddlRubro":                  "-2",
        "ctl00$CPH1$devDteEdtFechaAperturaDesde": "01/01/2023",
        "ctl00$CPH1$devDteEdtFechaAperturaHasta": "05/05/2026",
        "ctl00$CPH1$ddlResultadoOrdenadoPor":   "PLI.Pliego.NumeroPliego",
    }
    try:
        r = session.post(BASE_URL, data=p, headers=HEADERS_AJAX, verify=False, timeout=20)
        m = re.search(r"encontrado.*?\((\d+)\)", r.text)
        total = m.group(1) if m else "0"
        print(f"SAF {saf:>4}: {total:>5} contratos")
    except Exception as e:
        print(f"SAF {saf:>4}: ERROR {e}")
    time.sleep(1)