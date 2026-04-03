import requests

def buscar_en_bora(apellido="Perez"):
    print(f"--- Buscando designaciones de '{apellido}' en el BORA ---")
    # Endpoint de búsqueda del Boletín Oficial
    url = f"https://www.boletinoficial.gob.ar/seccion/primera/busqueda?q={apellido}"
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        response = requests.get(url, headers=headers)
        # Aquí procesarías el JSON o el HTML retornado
        if response.status_code == 200:
            print("Conexión exitosa. Analizando decretos y resoluciones...")
            # Lógica de extracción de nombres y cargos
        else:
            print("Portal del BORA temporalmente inaccesible.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    buscar_en_bora("Gomez")
