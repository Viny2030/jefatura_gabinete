import re
import requests
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()

s = requests.Session()
r = s.get('https://comprar.gob.ar/BuscarAvanzado.aspx', verify=False, timeout=10)
soup = BeautifulSoup(r.text, 'html.parser')
vs  = soup.find('input', {'id': '__VIEWSTATE'})['value']
vsg = soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value']

payload = {
    '__EVENTTARGET':        'ctl00$CPH1$btnListarPliegoAvanzado',
    '__EVENTARGUMENT':      '',
    '__LASTFOCUS':          '',
    '__VIEWSTATE':          vs,
    '__VIEWSTATEGENERATOR': vsg,
    'ctl00$CPH1$ddlJurisdicion':               '591',
    'ctl00$CPH1$ddlEstadoProceso':             '21',
    'ctl00$CPH1$devDteEdtFechaAperturaDesde':  '10/12/2023',
    'ctl00$CPH1$devDteEdtFechaAperturaHasta':  '07/04/2026',
    'ctl00$CPH1$ddlUnidadEjecutora':           '-2',
    'ctl00$CPH1$ddlTipoProceso':               '-2',
    'ctl00$CPH1$ddlRubro':                     '-2',
    'ctl00$CPH1$ddlResultadoOrdenadoPor':      'PLI.PliegoCronograma.FechaActoApertura',
    'ctl00$CPH1$ddlTipoOperacion':             '-2',
    'ctl00$CPH1$hidEstadoListaPliegos':        'NOREPORTEEXCEL',
    'ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw': 'N',
    'ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw': 'N',
}

print("Haciendo POST...")
r2 = s.post('https://comprar.gob.ar/BuscarAvanzado.aspx', data=payload, verify=False, timeout=30)
soup2 = BeautifulSoup(r2.text, 'html.parser')

tablas = soup2.find_all('table')
print(f"Tablas: {len(tablas)}")

t = tablas[2]
filas = t.find_all('tr')
print(f"Filas en tabla[2]: {len(filas)}")

# Headers
ths = t.find_all('th')
print(f"Headers: {[h.get_text(strip=True) for h in ths]}")

# Primera fila de datos
if len(filas) > 1:
    print(f"Fila 1: {[c.get_text(strip=True) for c in filas[1].find_all('td')][:6]}")
if len(filas) > 2:
    print(f"Fila 2: {[c.get_text(strip=True) for c in filas[2].find_all('td')][:6]}")

# Paginacion
postbacks = re.findall(r"__doPostBack\('([^']+)','(Page[^']+)'\)", r2.text)
print(f"Paginacion postbacks: {postbacks[:5]}")

# Buscar el panel de resultados
panel = soup2.find(id='ctl00_CPH1_pnlListaPliegos')
if panel:
    tablas_panel = panel.find_all('table')
    print(f"\nTablas en panel resultados: {len(tablas_panel)}")
    for i, t in enumerate(tablas_panel[:3]):
        filas = t.find_all('tr')
        ths = t.find_all('th')
        print(f"  tabla[{i}]: {len(filas)} filas, headers: {[h.get_text(strip=True) for h in ths[:6]]}")
        if len(filas) > 1:
            print(f"    fila1: {[c.get_text(strip=True)[:30] for c in filas[1].find_all('td')[:5]]}")
else:
    print("Panel no encontrado")
    # Buscar UpdatePanel
    up = soup2.find(id='ctl00_CPH1_UpdatePanel1')
    if up:
        print("UpdatePanel1 encontrado, contenido:")
        print(up.get_text(strip=True)[:500])