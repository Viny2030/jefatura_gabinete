import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

s = requests.Session()
r = s.get('https://comprar.gob.ar/BuscarAvanzado.aspx', verify=False, timeout=15)
soup = BeautifulSoup(r.text, 'html.parser')

payload = {
    '__EVENTTARGET':        'ctl00$CPH1$btnListarPliegoAvanzado',
    '__EVENTARGUMENT':      '',
    '__LASTFOCUS':          '',
    '__VIEWSTATE':          soup.find('input', {'id': '__VIEWSTATE'})['value'],
    '__VIEWSTATEGENERATOR': soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value'],
    'ctl00$CPH1$ddlJurisdicion':                '591',
    'ctl00$CPH1$ddlEstadoProceso':              '21',
    'ctl00$CPH1$devDteEdtFechaAperturaDesde':   '10/12/2023',
    'ctl00$CPH1$devDteEdtFechaAperturaDesde_I': '10/12/2023',
    'ctl00$CPH1$devDteEdtFechaAperturaHasta':   '07/04/2026',
    'ctl00$CPH1$devDteEdtFechaAperturaHasta_I': '07/04/2026',
    'ctl00$CPH1$ddlUnidadEjecutora':            '-2',
    'ctl00$CPH1$ddlTipoProceso':                '-2',
    'ctl00$CPH1$ddlRubro':                      '-2',
    'ctl00$CPH1$ddlResultadoOrdenadoPor':       'PLI.PliegoCronograma.FechaActoApertura',
    'ctl00$CPH1$ddlTipoOperacion':              '-2',
    'ctl00$CPH1$hidEstadoListaPliegos':         'NOREPORTEEXCEL',
    'ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw': 'N',
    'ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw': 'N',
    'ctl00$CPH1$txtNumeroProceso': '',
    'ctl00$CPH1$txtExpediente': '',
    'ctl00$CPH1$txtNombrePliego': '',
}

print("POST...")
r2 = s.post('https://comprar.gob.ar/BuscarAvanzado.aspx', data=payload, verify=False, timeout=30)
soup2 = BeautifulSoup(r2.text, 'html.parser')

panel = soup2.find(id='ctl00_CPH1_pnlListaPliegos')
if not panel:
    print("Panel no encontrado")
    exit()

tabla = panel.find('table')
print(f"Grid ID: {tabla.get('id')}")

filas = tabla.find_all('tr')
print(f"Total filas: {len(filas)}")
print()

# Mostrar TODAS las filas con su contenido raw
for i, fila in enumerate(filas[:5]):
    celdas = fila.find_all(['td', 'th'])
    print(f"Fila {i}: {len(celdas)} celdas")
    for j, c in enumerate(celdas):
        print(f"  [{j}] clase='{c.get('class', '')}' texto='{c.get_text(strip=True)[:60]}'")
    print()

# Ver si hay sub-tablas
subtablas = tabla.find_all('table')
print(f"Sub-tablas dentro: {len(subtablas)}")