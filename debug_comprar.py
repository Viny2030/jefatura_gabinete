#!/usr/bin/env python3
"""
debug_comprar.py — diagnóstico del POST a BuscarAvanzado.aspx
Correr desde la raíz del proyecto:
    python debug_comprar.py
"""
import requests
import warnings
from datetime import datetime
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": BASE_URL,
}

s = requests.Session()

# 1. GET para obtener viewstate
print("1. Obteniendo ViewState...")
r = s.get(BASE_URL, headers=HEADERS, verify=False)
soup = BeautifulSoup(r.text, "html.parser")

vs  = soup.find("input", {"id": "__VIEWSTATE"})
evv = soup.find("input", {"id": "__EVENTVALIDATION"})
vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})

vs_val  = vs["value"]  if vs  else ""
evv_val = evv["value"] if evv else ""
vsg_val = vsg["value"] if vsg else ""

print(f"   VIEWSTATE presente: {bool(vs_val)}")
print(f"   EVENTVALIDATION presente: {bool(evv_val)}")

# 2. POST con SAF 591 (JGM)
print("\n2. Enviando búsqueda SAF 591 (JGM)...")
today = datetime.today().strftime("%d/%m/%Y")

payload = {
    "__EVENTTARGET":   "",
    "__EVENTARGUMENT": "",
    "__VIEWSTATE":            vs_val,
    "__VIEWSTATEGENERATOR":   vsg_val,
    "__EVENTVALIDATION":      evv_val,
    "ctl00$CPH1$txtNumeroProceso":                   "",
    "ctl00$CPH1$txtExpediente":                      "",
    "ctl00$CPH1$txtNombrePliego":                    "",
    "ctl00$CPH1$ddlJurisdicion":                     "591",
    "ctl00$CPH1$ddlUnidadEjecutora":                 "-1",
    "ctl00$CPH1$ddlTipoProceso":                     "-1",
    "ctl00$CPH1$ddlEstadoProceso":                   "-1",
    "ctl00$CPH1$ddlRubro":                           "-1",
    "ctl00$CPH1$devDteEdtFechaAperturaDesde_I":      "10/12/2023",
    "ctl00$CPH1$devDteEdtFechaAperturaDesde_DDDWS":  "0:0:-10000:-10000:0:0:0:0:0",
    "ctl00$CPH1$devDteEdtFechaAperturaDesde$DDD$C":  "",
    "ctl00$CPH1$devDteEdtFechaAperturaHasta_I":      today,
    "ctl00$CPH1$devDteEdtFechaAperturaHasta_DDDWS":  "0:0:-10000:-10000:0:0:0:0:0",
    "ctl00$CPH1$devDteEdtFechaAperturaHasta$DDD$C":  "",
    "ctl00$CPH1$ddlResultadoOrdenadoPor":            "1",
    "ctl00$CPH1$ddlTipoOperacion":                   "-1",
    "ctl00$CPH1$hidEstadoListaPliegos":              "",
    "ctl00$CPH1$hdnFldIdProveedorSeleccionado":      "",
    "ctl00$CPH1$btnBuscar":                          "Buscar",
}

resp = s.post(BASE_URL, data=payload, headers=HEADERS, verify=False)
print(f"   Status: {resp.status_code}")
print(f"   URL final: {resp.url}")

# 3. Analizar respuesta
soup2 = BeautifulSoup(resp.text, "html.parser")

# Guardar HTML completo
with open("debug_jgm_591.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
print("\n3. HTML guardado en debug_jgm_591.html")

# IDs de todas las tablas
tablas = [(t.get("id", "sin-id"), len(t.find_all("tr"))) for t in soup2.find_all("table")]
print(f"\n4. Tablas encontradas ({len(tablas)} total):")
for tid, nrows in tablas:
    print(f"   id='{tid}' | filas={nrows}")

# Buscar texto de resultados
print("\n5. Fragmentos de texto relevantes:")
for tag in soup2.find_all(["span", "div", "td", "p"]):
    txt = tag.get_text(strip=True)
    if any(k in txt.lower() for k in ["resultado", "proceso", "encontrado", "no se encontr"]):
        if 5 < len(txt) < 200:
            print(f"   [{tag.name}] {txt[:120]}")

# Buscar links con números de proceso
print("\n6. Links que parecen procesos (primeros 5):")
count = 0
for a in soup2.find_all("a", href=True):
    txt = a.get_text(strip=True)
    if "-" in txt and len(txt) > 5 and len(txt) < 40:
        print(f"   href={a['href'][:80]} | texto={txt}")
        count += 1
        if count >= 5:
            break