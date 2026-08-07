"""
api_server.py
=============
API REST para el portal anticorrupciÃ³n JGM.
Sirve los datos generados por pipeline.py como endpoints JSON.
TambiÃ©n sirve el dashboard HTML estÃ¡tico.

Endpoints:
  GET /                     â†’ index.html (portal principal)
  GET /jgm.html             â†’ ficha JGM
  GET /sgp.html             â†’ ficha SGP
  GET /presidencia.html     â†’ ficha Presidencia
  GET /dashboard.html       â†’ dashboard con datos incrustados
  GET /alertas.html         â†’ alertas
  GET /grafos_nodos.html    â†’ grafo de nodos
  GET /manual_usuario.html  â†’ manual
  GET /documentacion.html   â†’ documentaciÃ³n tÃ©cnica
  GET /api/health           â†’ health check JSON
  GET /api/inteligencia     â†’ datos completos (alertas + grafo)
  GET /api/alertas          â†’ solo alertas (filtros: nivel, tipo)
  GET /api/grafo            â†’ solo grafo de nodos
  GET /api/contratos        â†’ contratos/licitaciones JGM
  GET /api/bora             â†’ publicaciones BORA relevantes
  GET /api/resumen          â†’ KPIs ejecutivos
  GET /api/db-status        â†’ estado de conexiÃ³n PostgreSQL
  POST /api/refresh         â†’ dispara pipeline (requiere X-Refresh-Token)
  POST /api/v1/chat/{area}  â†’ agente IA de solo lectura (area: jgm|sgp|presidencia)

Variables de entorno:
  DATABASE_URL          â†’ URL de PostgreSQL (Railway la inyecta automÃ¡ticamente)
  REFRESH_TOKEN         â†’ token secreto (default: "dev")
  PORT                  â†’ puerto (default: 8000)
  ANTHROPIC_API_KEY     â†’ clave de la API de Claude (si falta, /api/v1/chat/* devuelve 503)
  ANTHROPIC_MODEL       â†’ modelo a usar (default: "claude-sonnet-5")
  CHAT_RATE_LIMIT_DIARIO â†’ mensajes por IP por dÃ­a en el chat (default: 30)
"""

import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DATA_DIR = Path(__file__).parent.parent / "data"
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "dev")
PORT = int(os.getenv("PORT", 8000))
DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── Agente IA (chat) ──────────────────────────────────────────────────────────
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL     = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5")
CHAT_RATE_LIMIT_DIA = int(os.getenv("CHAT_RATE_LIMIT_DIARIO", "30"))  # mensajes por IP por día

app = FastAPI(
    title="Monitor AnticorrupciÃ³n JGM â€” API",
    description="Datos de alertas de parentesco, conflictos societarios y desvÃ­os de flujo de fondos",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"]
)

# Servir los JSON estáticos que consumen jgm.html, sgp.html y presidencia.html
# (fetch('data/contratos_jgm.json'), etc.). Sin este mount devuelven 404.
_FRONTEND_DATA = FRONTEND_DIR / "data"
if _FRONTEND_DATA.exists():
    app.mount("/data", StaticFiles(directory=str(_FRONTEND_DATA)), name="frontend-data")


# â”€â”€â”€ helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
    """Carga el Ãºltimo snapshot de inteligencia.json desde PostgreSQL."""
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


# â”€â”€â”€ rutas frontend â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    """PÃ¡gina principal â€” sirve index.html."""
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


# â”€â”€â”€ API endpoints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@app.get("/api/health")
def health():
    """Health check â€” estado del servicio y Ãºltimo pipeline."""
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
        "servicio": "Monitor AnticorrupciÃ³n JGM",
        "ultima_actualizacion": meta.get("ultima_actualizacion"),
        "total_alertas": meta.get("total_alertas", 0),
        "alertas_alta": meta.get("alertas_alta", 0),
        "db_conectada": db_ok,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/api/db-status")
def db_status():
    """Estado de la conexiÃ³n a PostgreSQL."""
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
        return {"conectada": True, "motivo": "Tabla pipeline_snapshots no creada aÃºn â€” correr /api/init-db"}
    except Exception as e:
        return {"conectada": False, "motivo": str(e)}


@app.post("/api/init-db")
def init_db(x_refresh_token: str = Header(None)):
    """Crea las tablas en PostgreSQL si no existen."""
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token invÃ¡lido")
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
    """Grafo de nodos para visualizaciÃ³n (funcionarios, empresas, vÃ­nculos)."""
    data = _load("inteligencia.json")
    grafo = data.get("grafo", {})
    if not grafo:
        raise HTTPException(status_code=404, detail="Grafo no disponible todavÃ­a")
    return grafo


@app.get("/api/contratos")
def get_contratos(
    tipo: str = Query(None, description="adjudicacion o convocatoria"),
    monto_min: float = Query(None, description="Monto mÃ­nimo en ARS"),
    limit: int = Query(100, description="MÃ¡ximo de resultados")
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



# ─── Agente IA — chat de solo lectura por área ────────────────────────────────
#
# Endpoint público POST /api/v1/chat/{area} (area = jgm | sgp | presidencia).
# El agente SOLO puede leer datos ya publicados en el portal (contratos,
# personal, cruces) a través de "tools" — nunca escribe en la base ni ejecuta
# código arbitrario. Cada respuesta debe basarse en los datos que devuelven
# las tools; si no encuentra nada, tiene que decirlo en vez de inventar.
#
# Requiere la variable de entorno ANTHROPIC_API_KEY. Si no está configurada,
# el endpoint devuelve 503 en vez de romper el resto de la API.

AREAS_VALIDAS = {"jgm", "sgp", "presidencia"}
_rate_limit_chat: dict[str, tuple[str, int]] = defaultdict(lambda: ("", 0))


def _chat_rate_limit_ok(ip: str) -> bool:
    """Límite simple por IP/día en memoria. No sobrevive a un restart ni se
    comparte entre instancias — suficiente para desalentar abuso en un único
    servicio de Railway; si se escala a más de una instancia, mover a Redis."""
    hoy = datetime.now().strftime("%Y-%m-%d")
    dia, cnt = _rate_limit_chat[ip]
    if dia != hoy:
        _rate_limit_chat[ip] = (hoy, 1)
        return True
    if cnt >= CHAT_RATE_LIMIT_DIA:
        return False
    _rate_limit_chat[ip] = (hoy, cnt + 1)
    return True


def _tool_buscar_contratos(area: str, proveedor: str = None, organismo: str = None,
                            monto_min: float = None, limit: int = 15) -> list[dict]:
    data = _load(f"contratos_{area}.json")
    if not isinstance(data, list):
        return []
    out = data
    if proveedor:
        p = proveedor.lower()
        out = [c for c in out if p in str(c.get("proveedor", "")).lower()]
    if organismo:
        o = organismo.lower()
        out = [c for c in out if o in str(c.get("organismo", "")).lower()]
    if monto_min:
        out = [c for c in out if (c.get("monto_adjudicado") or 0) >= monto_min]
    out = sorted(out, key=lambda c: c.get("monto_adjudicado") or 0, reverse=True)
    return out[:limit]


def _tool_buscar_personal(area: str, apellido: str = None, cargo: str = None,
                           limit: int = 15) -> list[dict]:
    data = _load(f"personal_{area}.json")
    if not isinstance(data, list):
        return []
    out = data
    if apellido:
        a = apellido.lower()
        out = [p for p in out if a in str(p.get("apellido", "")).lower()]
    if cargo:
        c = cargo.lower()
        out = [p for p in out if c in str(p.get("cargo", "")).lower()]
    return out[:limit]


def _tool_buscar_cruces(apellido: str = None, limit: int = 10) -> list[dict]:
    """Busca en cruces.json (generado por scripts/generar_cruces_pen.py).
    Es un cruce a nivel de todo el monitor, no solo del área actual —
    el agente debe aclararlo si el organismo del resultado no coincide."""
    data = _load("cruces.json")
    cruces = data.get("cruces", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    out = cruces
    if apellido:
        a = apellido.lower()
        out = [c for c in out if a in str(c.get("funcionario", c.get("apellido", ""))).lower()]
    return out[:limit]


TOOLS_SCHEMA = [
    {
        "name": "buscar_contratos",
        "description": "Busca contratos/adjudicaciones del área actual por proveedor, organismo o monto mínimo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "proveedor": {"type": "string", "description": "Nombre o parte del nombre del proveedor"},
                "organismo": {"type": "string", "description": "Nombre o parte del nombre del organismo"},
                "monto_min": {"type": "number", "description": "Monto adjudicado mínimo en ARS"},
            },
        },
    },
    {
        "name": "buscar_personal",
        "description": "Busca funcionarios/autoridades del área actual por apellido o cargo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "apellido": {"type": "string"},
                "cargo": {"type": "string"},
            },
        },
    },
    {
        "name": "buscar_cruces",
        "description": "Busca alertas de cruce (funcionario que también es proveedor, o apellido coincidente) en todo el monitor, filtrando opcionalmente por apellido.",
        "input_schema": {
            "type": "object",
            "properties": {
                "apellido": {"type": "string"},
            },
        },
    },
]

SYSTEM_PROMPT_TMPL = """Sos el asistente del Monitor de Transparencia del Poder Ejecutivo Nacional argentino, sección {area_label}.

Reglas estrictas:
- Respondé siempre en español, de forma breve y concreta.
- SOLO podés afirmar datos que hayan sido devueltos por las tools (buscar_contratos, buscar_personal, buscar_cruces). Nunca inventes montos, nombres, CUITs ni fechas.
- Si una búsqueda no devuelve resultados, decilo explícitamente ("no encontré coincidencias") en vez de sugerir que sí hay algo.
- Cuando menciones una alerta de cruce, aclará que es un indicador algorítmico de riesgo (coincidencia de CUIL/apellido), no una acusación ni una determinación de responsabilidad.
- Si te preguntan algo fuera de contratos/personal/cruces de este portal, decí que no es tu función y sugerí de qué se puede hablar.
- No des consejos legales ni políticos; ceñite a describir los datos públicos."""

AREA_LABELS = {
    "jgm": "Jefatura de Gabinete de Ministros",
    "sgp": "Secretaría General de la Presidencia",
    "presidencia": "Presidencia de la Nación",
}


async def _ejecutar_tool(nombre: str, area: str, entrada: dict):
    if nombre == "buscar_contratos":
        return _tool_buscar_contratos(area, **entrada)
    if nombre == "buscar_personal":
        return _tool_buscar_personal(area, **entrada)
    if nombre == "buscar_cruces":
        return _tool_buscar_cruces(**entrada)
    return {"error": f"tool desconocida: {nombre}"}


async def _llamar_claude(client: httpx.AsyncClient, messages: list, system: str) -> dict:
    r = await client.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": ANTHROPIC_MODEL,
            "max_tokens": 700,
            "system": system,
            "tools": TOOLS_SCHEMA,
            "messages": messages,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


class ChatIn(BaseModel):
    mensaje: str
    historial: list[dict] = []  # [{"role": "user"|"assistant", "texto": "..."}]


@app.post("/api/v1/chat/{area}", tags=["Agente IA"])
async def chat_agente(area: str, body: ChatIn, request: Request):
    if area not in AREAS_VALIDAS:
        raise HTTPException(status_code=404, detail=f"Área inválida: {area}")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Agente no configurado (falta ANTHROPIC_API_KEY)")
    if not body.mensaje or not body.mensaje.strip():
        raise HTTPException(status_code=400, detail="Mensaje vacío")
    if len(body.mensaje) > 800:
        raise HTTPException(status_code=400, detail="Mensaje demasiado largo (máx. 800 caracteres)")

    ip = request.client.host if request.client else "desconocida"
    if not _chat_rate_limit_ok(ip):
        raise HTTPException(
            status_code=429,
            detail=f"Límite de {CHAT_RATE_LIMIT_DIA} mensajes por día alcanzado. Volvé a intentar mañana.",
        )

    system = SYSTEM_PROMPT_TMPL.format(area_label=AREA_LABELS[area])

    # Historial acotado: últimos 6 turnos, para no disparar el costo por request
    messages = []
    for turno in body.historial[-6:]:
        rol = "assistant" if turno.get("role") == "assistant" else "user"
        texto = str(turno.get("texto", ""))[:800]
        if texto:
            messages.append({"role": rol, "content": texto})
    messages.append({"role": "user", "content": body.mensaje.strip()})

    try:
        async with httpx.AsyncClient() as client:
            for _ in range(4):  # máximo 4 idas y vueltas de tool-use por mensaje
                resp = await _llamar_claude(client, messages, system)
                stop = resp.get("stop_reason")
                bloques = resp.get("content", [])

                if stop != "tool_use":
                    texto = "".join(b.get("text", "") for b in bloques if b.get("type") == "text")
                    return {"respuesta": texto.strip() or "No tengo una respuesta para eso.", "area": area}

                messages.append({"role": "assistant", "content": bloques})
                tool_results = []
                for b in bloques:
                    if b.get("type") != "tool_use":
                        continue
                    resultado = await _ejecutar_tool(b["name"], area, b.get("input", {}) or {})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": b["id"],
                        "content": json.dumps(resultado, ensure_ascii=False, default=str)[:4000],
                    })
                messages.append({"role": "user", "content": tool_results})

            return {"respuesta": "No pude completar la búsqueda, probá con una consulta más simple.", "area": area}

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Error del proveedor de IA: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado del agente: {e}")


@app.post("/api/refresh")
def refresh(x_refresh_token: str = Header(None)):
    """Dispara el pipeline de scraping y anÃ¡lisis."""
    if x_refresh_token != REFRESH_TOKEN:
        raise HTTPException(status_code=401, detail="Token invÃ¡lido")
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
    """Guarda el Ãºltimo inteligencia.json como snapshot en PostgreSQL."""
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


# -- MEACI: CUITs sancionados internacionalmente ------------------------------
MEACI_CUITS = set()  # Se poblará con lista real en próxima sesión

@app.get("/api/cruce-cuits-bulk")
def cruce_cuits_bulk(cuits: str = Query(..., description="CUITs separados por coma")):
    """Verifica si algun CUIT esta en lista de sancionados internacionales."""
    lista = [c.strip().replace("-","").replace(".","") for c in cuits.split(",") if c.strip()]
    alertas = {c: True for c in lista if c in MEACI_CUITS}
    return {"alertas": alertas, "total": len(alertas)}


# -- MEACI: CUITs sancionados internacionalmente ------------------------------
