"""
diag8.py — captura todos los organismos del PEN en COMPRAR
"""
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

BASE_URL = "https://comprar.gob.ar/BuscarAvanzado.aspx"
HEADERS  = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://comprar.gob.ar",
    "Referer": BASE_URL,
}

session = requests.Session()
r = session.get(BASE_URL, headers=HEADERS, timeout=20, verify=False)
soup = BeautifulSoup(r.text, "html.parser")

dropdown = soup.find("select", {"id": "ctl00_CPH1_ddlJurisdicion"})
if dropdown:
    opciones = []
    for opt in dropdown.find_all("option"):
        val = opt.get("value", "").strip()
        txt = opt.get_text(strip=True)
        if val and val != "-2":
            opciones.append((val, txt))

    print(f"Total organismos en COMPRAR: {len(opciones)}\n")
    print(f"{'SAF':<10} {'Nombre'}")
    print("-" * 70)
    for val, txt in opciones:
        print(f"{val:<10} {txt}")
else:
    print("Dropdown ddlJurisdicion no encontrado")