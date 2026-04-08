"""
diag7.py — captura opciones del dropdown ddlResultadoOrdenadoPor
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

dropdown = soup.find("select", {"id": "ctl00_CPH1_ddlResultadoOrdenadoPor"})
if dropdown:
    print("Opciones de ddlResultadoOrdenadoPor:")
    for opt in dropdown.find_all("option"):
        print(f"  value='{opt.get('value', '')}' | texto='{opt.get_text(strip=True)}'")
else:
    print("Dropdown no encontrado — buscando todos los select:")
    for sel in soup.find_all("select"):
        print(f"\n  id='{sel.get('id', '')}' name='{sel.get('name', '')}'")
        for opt in sel.find_all("option"):
            print(f"    value='{opt.get('value', '')}' | texto='{opt.get_text(strip=True)}'")