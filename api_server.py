"""
api_server.py
=============
API REST para el portal anticorrupción JGM.
Sirve los datos generados por pipeline.py como endpoints JSON.
También sirve el dashboard HTML estático.

Endpoints:
  GET /                     → health check
  GET /dashboard            → dashboard HTML
  GET /api/inteligencia     → datos completos (alertas + grafo)
  GET /api/alertas          → solo alertas (filtros: nivel, tipo)
  GET /api/grafo            → solo grafo de nodos
  GET /api/contratos        → contratos/licitaciones JGM
  GET /api/bora             → publicaciones BORA relevantes
  GET /api/resumen          → KPIs ejecutivos
  POST /api/refresh         → dispara pipeline (requiere X-Refresh-Token)

Variables de entorno:
  REFRESH_TOKEN  → token secreto (default: "dev")
  PORT           → puerto (default: 8000)
"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse

DATA_DIR = Path(__file__).parent.parent / "data"
FRONTEND_DIR = Path(__file__).parent / "frontend"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "dev")
PORT = int(os.getenv("PORT", 8000))

app = FastAPI(
    title="Monitor Anticorrupción JGM — API",
    description="Datos de alertas de parentesco, conflictos societarios y desvíos de flujo de fondos",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"]
)


def _load(nombre: str) -> dict:
    path = DATA_DIR / nombre
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/")
def health():
    intel = _load("inteligencia.json")
    meta = intel.get("meta", {})
    return {
        "status": "ok",
        "servicio": "Monitor Anticorrupción JGM",
        "ultima_actualizacion": meta.get("ultima_actualizacion"),
        "total_alertas": meta.get("total_alertas", 0),
        "alertas_alta": meta.get("alertas_alta", 0),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Sirve el dashboard HTML con los datos incrustados."""
    html_path = FRONTEND_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="Dashboard no encontrado")

    intel = _load("inteligencia.json")
    html = html_path.read_text(encoding="utf-8")

    # Inyectar datos como variable global JS
    inject = f"\n<script>window.__JGM_DATA__ = {json.dumps(intel, ensure_ascii=False)};</script>\n"
    html = html.replace("</head>", inject + "</head>", 1)
    return HTMLResponse(content=html)


@app.get("/api/inteligencia")
def get_inteligencia():
    """Datos completos: alertas + grafo + meta."""
    data = _load("inteligencia.json")
    if not data:
        raise HTTPException(status_code=503, detail="Datos no disponibles. Correr pipeline.py primero.")
    return data


@app.get("/api/alertas")
def get_alertas(
    nivel: str = Query(None, description="ALTA o MEDIA"),
    tipo: str = Query(None, description="NEPOTISMO, CONFLICTO_SOCIETARIO, DESVIO_IAP_GLOBAL, etc.")
):
    """Lista de alertas con filtros opcionales."""
    data = _load("inteligencia.json")
    alertas = data.get("alertas", [])

    if nivel:
        alertas = [a for a in alertas if a.get("nivel", "").upper() == nivel.upper()]
    if tipo:
        alertas = [a for a in alertas if tipo.upper() in (a.get("tipo_alerta", "")).upper()]

    return {
        "total": len(alertas),
        "alertas": alertas,
        "meta": data.get("meta", {})
    }


@app.get("/api/grafo")
def get_grafo():
    """Grafo de nodos para visualización (funcionarios, empresas, vínculos)."""
    data = _load("inteligencia.json")
    grafo = data.get("grafo", {})
    if not grafo:
        raise HTTPException(status_code=404, detail="Grafo no disponible todavía")
    return grafo


@app.get("/api/contratos")
def get_contratos(
    tipo: str = Query(None, description="adjudicacion o convocatoria"),
    monto_min: float = Query(None, description="Monto mínimo en ARS"),
    limit: int = Query(100, description="Máximo de resultados")
):
    """Contratos y licitaciones de JGM."""
    data = _load("comprar_raw.json")
    contratos = data.get("contratos", [])

    if tipo:
        contratos = [c for c in contratos if c.get("tipo") == tipo]
    if monto_min:
        contratos = [c for c in contratos if (c.get("monto_estimado") or 0) >= monto_min]

    contratos = sorted(contratos, key=lambda x: x.get("monto_estimado") or 0, reverse=True)

    return {
        "total": len(contratos),
        "contratos": contratos[:limit],
        "meta": data.get("meta", {})
    }


@app.get("/api/bora")
def get_bora(relevante_jgm: bool = Query(True)):
    """Publicaciones del BORA relevantes para JGM."""
    data = _load("bora_raw.json")
    if isinstance(data, list):
        publicaciones = data
    else:
        publicaciones = []

    if relevante_jgm:
        publicaciones = [p for p in publicaciones if p.get("relevante_jgm")]

    return {
        "total": len(publicaciones),
        "publicaciones": publicaciones[:200]
    }


@app.get("/api/resumen")
def get_resumen():
    """KPIs ejecutivos para el header del dashboard."""
    data = _load("resumen.json")
    if not data:
        # Calcular desde inteligencia.json si resumen.json no existe
        intel = _load("inteligencia.json")
        if not intel:
            raise HTTPException(status_code=503, detail="Sin datos disponibles")
        meta = intel.get("meta", {})
        return {
            "kpis": {
                "total_alertas": meta.get("total_alertas", 0),
                "alertas_alta": meta.get("alertas_alta", 0),
                "alertas_media": meta.get("alertas_media", 0),
            },
            "ultima_actualizacion": meta.get("ultima_actualizacion")
        }
    return data


@app.post("/api/refresh")
def refresh(x_refresh_token: str = Header(None)):
    """Dispara el pipeline de scraping y análisis."""
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    try:
        pipeline_path = Path(__file__).parent / "pipeline.py"
        result = subprocess.run(
            [sys.executable, str(pipeline_path)],
            capture_output=True, text=True, timeout=600,
            cwd=str(Path(__file__).parent)
        )
        return {
            "status": "ok" if result.returncode == 0 else "error",
            "stdout": result.stdout[-3000:],
            "stderr": result.stderr[-1000:],
            "timestamp": datetime.now().isoformat()
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Pipeline timeout (>10min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="0.0.0.0", port=PORT, reload=False)
