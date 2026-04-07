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
    'ctl00$CPH1$ddlJurisdicion':               '591',
    'ctl00$CPH1$ddlEstadoProceso':             '-2',
    'ctl00$CPH1$devDteEdtFechaAperturaDesde':  '',
    'ctl00$CPH1$devDteEdtFechaAperturaDesde_I':'',
    'ctl00$CPH1$devDteEdtFechaAperturaHasta':  '',
    'ctl00$CPH1$devDteEdtFechaAperturaHasta_I':'',
    'ctl00$CPH1$ddlUnidadEjecutora':           '-2',
    'ctl00$CPH1$ddlTipoProceso':               '-2',
    'ctl00$CPH1$ddlRubro':                     '-2',
    'ctl00$CPH1$ddlResultadoOrdenadoPor':      'PLI.PliegoCronograma.FechaActoApertura',
    'ctl00$CPH1$ddlTipoOperacion':             '-2',
    'ctl00$CPH1$hidEstadoListaPliegos':        'NOREPORTEEXCEL',
    'ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw': 'N',
    'ctl00_CPH1_devDteEdtFechaAperturaHasta_Raw': 'N',
    'ctl00$CPH1$txtNumeroProceso': '',
    'ctl00$CPH1$txtExpediente': '',
    'ctl00$CPH1$txtNombrePliego': '',
}

r2 = s.post('https://comprar.gob.ar/BuscarAvanzado.aspx', data=payload, verify=False, timeout=30)
soup2 = BeautifulSoup(r2.text, 'html.parser')

panel = soup2.find(id='ctl00_CPH1_pnlListaPliegos')
tabla = panel.find('table', {'id': 'ctl00_CPH1_GridListaPliegos'})
filas = tabla.find_all('tr')

print(f"Total filas: {len(filas)}")
print()

# Simular _parse_filas con el fix
for i, fila in enumerate(filas[1:], 1):
    celdas = fila.find_all('td')
    primer_texto = celdas[0].get_text(strip=True) if celdas else ''
    print(f"Fila {i}: {len(celdas)} celdas | primer_texto='{primer_texto}' | isdigit={primer_texto.isdigit()}")
    if len(celdas) >= 6 and not primer_texto.isdigit() and primer_texto != '...':
        textos = [c.get_text(strip=True) for c in celdas]
        print(f"  → INCLUIDA: nro='{textos[0]}' fecha='{textos[4]}'")
    else:
        print(f"  → DESCARTADA")