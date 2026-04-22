"""
api_server.py
=============
API REST para el portal anticorrupción JGM.
Sirve los datos generados por pipeline.py como endpoints JSON.
También sirve el dashboard HTML estático.

Endpoints:
  GET /                     → index.html (portal principal)
  GET /jgm.html             → ficha JGM
  GET /sgp.html             → ficha SGP
  GET /presidencia.html     → ficha Presidencia
  GET /dashboard.html       → dashboard con datos incrustados
  GET /alertas.html         → alertas
  GET /grafos_nodos.html    → grafo de nodos
  GET /manual_usuario.html  → manual
  GET /documentacion.html   → documentación técnica
  GET /api/health           → health check JSON
  GET /api/inteligencia     → datos completos (alertas + grafo)
  GET /api/alertas          → solo alertas (filtros: nivel, tipo)
  GET /api/grafo            → solo grafo de nodos
  GET /api/contratos        → contratos/licitaciones JGM
  GET /api/bora             → publicaciones BORA relevantes
  GET /api/resumen          → KPIs ejecutivos
  GET /api/db-status        → estado de conexión PostgreSQL
  POST /api/refresh         → dispara pipeline (requiere X-Refresh-Token)

Variables de entorno:
  DATABASE_URL   → URL de PostgreSQL (Railway la inyecta automáticamente)
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
from fastapi.responses import HTMLResponse, RedirectResponse

DATA_DIR = Path(__file__).parent.parent / "data"
FRONTEND_DIR = Path(__file__).parent / "frontend"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "dev")
PORT = int(os.getenv("PORT", 8000))
DATABASE_URL = os.getenv("DATABASE_URL", "")

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


# ─── helpers ───────────────────────────────────────────────────────────────────

def _load(nombre: str) -> dict:
    """Carga un JSON del directorio data/. Si no existe, intenta desde PostgreSQL."""
    path = DATA_DIR / nombre
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    # Fallback: intentar desde PostgreSQL
    if DATABASE_URL and nombre == "inteligencia.json":
        return _load_from_db() or {}
    return {}


def _load_from_db() -> dict | None:
    """Carga el último snapshot de inteligencia.json desde PostgreSQL."""
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT payload FROM pipeline_snapshots ORDER BY created_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return json.loads(row[0])
    except Exception as e:
        print(f"[DB] Error leyendo desde PostgreSQL: {e}")
    return None


def _serve_html(filename: str) -> HTMLResponse:
    """Sirve un archivo HTML del directorio frontend."""
    path = FRONTEND_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} no encontrado")
    return HTMLResponse(content=path.read_text(encoding="utf-8"))


# ─── rutas frontend ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    """Página principal — sirve index.html."""
    return _serve_html("index.html")


@app.get("/index.html", response_class=HTMLResponse, include_in_schema=False)
def index_html():
    return _serve_html("index.html")


@app.get("/jgm.html", response_class=HTMLResponse, include_in_schema=False)
def jgm_html():
    return _serve_html("jgm.html")


@app.get("/sgp.html", response_class=HTMLResponse, include_in_schema=False)
def sgp_html():
    return _serve_html("sgp.html")


@app.get("/presidencia.html", response_class=HTMLResponse, include_in_schema=False)
def presidencia_html():
    return _serve_html("presidencia.html")


@app.get("/alertas.html", response_class=HTMLResponse, include_in_schema=False)
def alertas_html():
    return _serve_html("alertas.html")


@app.get("/grafos_nodos.html", response_class=HTMLResponse, include_in_schema=False)
def grafos_nodos_html():
    return _serve_html("grafos_nodos.html")


@app.get("/manual_usuario.html", response_class=HTMLResponse, include_in_schema=False)
def manual_usuario_html():
    return _serve_html("manual_usuario.html")


@app.get("/documentacion.html", response_class=HTMLResponse, include_in_schema=False)
def documentacion_html():
    return _serve_html("documentacion.html")


@app.get("/dashboard.html", response_class=HTMLResponse, include_in_schema=False)
def dashboard_html():
    """Dashboard HTML con datos incrustados como JS global."""
    path = FRONTEND_DIR / "dashboard.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dashboard no encontrado")
    intel = _load("inteligencia.json")
    html = path.read_text(encoding="utf-8")
    inject = f"\n<script>window.__JGM_DATA__ = {json.dumps(intel, ensure_ascii=False)};</script>\n"
    html = html.replace("</head>", inject + "</head>", 1)
    return HTMLResponse(content=html)


# ─── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Health check — estado del servicio y último pipeline."""
    intel = _load("inteligencia.json")
    meta = intel.get("meta", {})
    db_ok = False
    if DATABASE_URL:
        try:
            import psycopg2
            conn = psycopg2.connect(DATABASE_URL)
            conn.close()
            db_ok = True
        except Exception:
            db_ok = False
    return {
        "status": "ok",
        "servicio": "Monitor Anticorrupción JGM",
        "ultima_actualizacion": meta.get("ultima_actualizacion"),
        "total_alertas": meta.get("total_alertas", 0),
        "alertas_alta": meta.get("alertas_alta", 0),
        "db_conectada": db_ok,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/db-status")
def db_status():
    """Estado de la conexión a PostgreSQL."""
    if not DATABASE_URL:
        return {"conectada": False, "motivo": "DATABASE_URL no configurada"}
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pipeline_snapshots")
        total = cur.fetchone()[0]
        cur.execute("SELECT created_at FROM pipeline_snapshots ORDER BY created_at DESC LIMIT 1")
        ultimo = cur.fetchone()
        cur.close()
        conn.close()
        return {
            "conectada": True,
            "snapshots_guardados": total,
            "ultimo_snapshot": ultimo[0].isoformat() if ultimo else None
        }
    except psycopg2.errors.UndefinedTable:
        return {"conectada": True, "motivo": "Tabla pipeline_snapshots no creada aún — correr /api/init-db"}
    except Exception as e:
        return {"conectada": False, "motivo": str(e)}


@app.post("/api/init-db")
def init_db(x_refresh_token: str = Header(None)):
    """Crea las tablas en PostgreSQL si no existen."""
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido")
    if not DATABASE_URL:
        raise HTTPException(status_code=503, detail="DATABASE_URL no configurada")
    try:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_snapshots (
                id SERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                total_alertas INTEGER DEFAULT 0,
                alertas_alta INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_snapshots_created ON pipeline_snapshots (created_at DESC);
        """)
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "ok", "mensaje": "Tablas creadas correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    publicaciones = data if isinstance(data, list) else []
    if relevante_jgm:
        publicaciones = [p for p in publicaciones if p.get("relevante_jgm")]
    return {"total": len(publicaciones), "publicaciones": publicaciones[:200]}


@app.get("/api/resumen")
def get_resumen():
    """KPIs ejecutivos para el header del dashboard."""
    data = _load("resumen.json")
    if not data:
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
        # Si hay DATABASE_URL, guardar snapshot en PostgreSQL
        if result.returncode == 0 and DATABASE_URL:
            _save_snapshot_to_db()
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


def _save_snapshot_to_db():
    """Guarda el último inteligencia.json como snapshot en PostgreSQL."""
    try:
        import psycopg2
        intel = _load("inteligencia.json")
        if not intel:
            return
        meta = intel.get("meta", {})
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pipeline_snapshots (payload, total_alertas, alertas_alta)
            VALUES (%s, %s, %s)
            """,
            (
                json.dumps(intel, ensure_ascii=False),
                meta.get("total_alertas", 0),
                meta.get("alertas_alta", 0),
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        print("[DB] Snapshot guardado en PostgreSQL")
    except Exception as e:
        print(f"[DB] Error guardando snapshot: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_server:app", host="0.0.0.0", port=PORT, reload=False)
