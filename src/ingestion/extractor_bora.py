"""
extractor_bora.py
=================
Scraper del Boletín Oficial de la República Argentina (BORA).
Fuente: https://www.boletinoficial.gob.ar/

Extrae resoluciones, decretos y disposiciones de la Jefatura de Gabinete
para detectar nombramientos, contratos y movimientos de personal.

Uso:
    python extractor_bora.py                  # hoy
    python extractor_bora.py --fecha 2026-04-01
    python extractor_bora.py --dias 7         # ultima semana
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JefaturaMonitor/1.0)"}
TIMEOUT = 30
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")

# Terminos a buscar relacionados con Jefatura de Gabinete
TERMINOS_JGM = [
    "jefatura de gabinete",
    "secretaria de innovacion",
    "secretaria general",
    "subsecretaria",
    "designacion",
    "nombramiento",
    "contratacion directa",
]


def fetch_bora_dia(fecha: datetime) -> list:
    """
    Descarga las publicaciones del BORA para una fecha dada.
    El BORA expone sus secciones como JSON via una API interna no documentada.
    """
    fecha_str = fecha.strftime("%Y%m%d")
    url = f"https://www.boletinoficial.gob.ar/seccion/primera/{fecha_str}"

    registros = []
    try:
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if res.status_code == 404:
            print(f"[BORA] Sin publicaciones para {fecha_str}")
            return []
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")

        # El BORA lista las publicaciones en elementos con clase "aviso"
        avisos = soup.find_all(["article", "div"], class_=re.compile(r"aviso|norma|publicacion", re.I))

        if not avisos:
            # Intentar API JSON interna
            api_url = f"https://www.boletinoficial.gob.ar/norma/detallePrimaryList/{fecha_str}/1"
            try:
                r2 = requests.get(api_url, headers=HEADERS, timeout=TIMEOUT)
                if r2.status_code == 200:
                    data = r2.json()
                    avisos_json = data if isinstance(data, list) else data.get("avisos", [])
                    for a in avisos_json:
                        titulo = a.get("titulo") or a.get("titulo_completo") or ""
                        texto = a.get("texto") or a.get("bajada") or ""
                        organismo = a.get("organismo") or a.get("reparticion") or ""
                        registros.append({
                            "fuente": "BORA",
                            "fecha": fecha.strftime("%Y-%m-%d"),
                            "tipo": a.get("tipo") or a.get("categoria") or "ND",
                            "numero": a.get("numero") or a.get("nro") or "",
                            "organismo": organismo,
                            "titulo": titulo,
                            "texto_resumen": texto[:500],
                            "url": f"https://www.boletinoficial.gob.ar/detalleAviso/primera/{a.get('id','')}",
                            "relevante_jgm": _es_relevante(titulo + " " + texto + " " + organismo)
                        })
                    return registros
            except Exception:
                pass

        # Parseo HTML fallback
        for aviso in avisos[:100]:
            titulo = aviso.get_text(separator=" ", strip=True)[:300]
            if not titulo:
                continue
            registros.append({
                "fuente": "BORA",
                "fecha": fecha.strftime("%Y-%m-%d"),
                "tipo": "ND",
                "numero": "",
                "organismo": "",
                "titulo": titulo,
                "texto_resumen": "",
                "url": url,
                "relevante_jgm": _es_relevante(titulo)
            })

        print(f"[BORA] {fecha_str}: {len(registros)} publicaciones")
        return registros

    except Exception as e:
        print(f"[ERROR] BORA {fecha_str}: {e}")
        return []


def _es_relevante(texto: str) -> bool:
    """Marca como relevante si menciona Jefatura o términos de interés."""
    t = texto.lower()
    return any(term in t for term in TERMINOS_JGM)


def run(fecha_inicio: datetime = None, dias: int = 1) -> list:
    if fecha_inicio is None:
        fecha_inicio = datetime.now()

    todos = []
    for i in range(dias):
        fecha = fecha_inicio - timedelta(days=i)
        registros = fetch_bora_dia(fecha)
        todos.extend(registros)
        if dias > 1:
            time.sleep(1)  # respetar rate limit

    # Filtrar solo los relevantes para JGM
    relevantes = [r for r in todos if r.get("relevante_jgm")]
    print(f"[BORA] Total: {len(todos)} publicaciones, {len(relevantes)} relevantes JGM")
    return todos  # devolver todos, el motor filtra


def save(registros: list, path: str = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(OUTPUT_DIR, "bora_raw.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)
    print(f"[BORA] Guardado en {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fecha", help="Fecha YYYY-MM-DD (default: hoy)")
    parser.add_argument("--dias", type=int, default=1, help="Cantidad de días hacia atrás")
    args = parser.parse_args()

    fecha = datetime.strptime(args.fecha, "%Y-%m-%d") if args.fecha else datetime.now()
    datos = run(fecha_inicio=fecha, dias=args.dias)
    save(datos)
