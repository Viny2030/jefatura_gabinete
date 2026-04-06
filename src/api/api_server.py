"""
api_server.py
FastAPI - Portal Anticorrupción JGM
Endpoints REST para el dashboard público
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import databases
import sqlalchemy
from pydantic import BaseModel
from datetime import date

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/jgm")

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

# ── Tablas reflejadas ──────────────────────────────────────────────────────────
alertas_tbl   = sqlalchemy.Table("alertas",   metadata, autoload_with=sqlalchemy.create_engine(DATABASE_URL))
contratos_tbl = sqlalchemy.Table("contratos", metadata, autoload_with=sqlalchemy.create_engine(DATABASE_URL))
nomina_tbl    = sqlalchemy.Table("nomina",    metadata, autoload_with=sqlalchemy.create_engine(DATABASE_URL))
vinculos_tbl  = sqlalchemy.Table("vinculos",  metadata, autoload_with=sqlalchemy.create_engine(DATABASE_URL))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()


app = FastAPI(
    title="Portal Anticorrupción - JGM",
    description="API pública de monitoreo de contratos y alertas de la Jefatura de Gabinete",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Servir frontend estático
app.mount("/static", StaticFiles(directory="src/frontend"), name="static")


# ── Schemas ───────────────────────────────────────────────────────────────────
class AlertaOut(BaseModel):
    id: str
    tipo: str
    nivel: str
    titulo: str
    descripcion: Optional[str]
    monto_involucrado: Optional[float]
    fecha_creacion: str

class ContratoOut(BaseModel):
    id: str
    organismo: Optional[str]
    proveedor: Optional[str]
    cuit_proveedor: Optional[str]
    monto_adjudicado: Optional[float]
    tipo_proceso: Optional[str]
    fecha_adjudicacion: Optional[str]

class KPIOut(BaseModel):
    total_contratos: int
    monto_total_ars: float
    alertas_activas: int
    alertas_alta: int
    proveedores_unicos: int
    funcionarios_monitoreados: int


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def root():
    return FileResponse("src/frontend/dashboard.html")

@app.get("/api/v1/kpis", response_model=KPIOut, tags=["Dashboard"])
async def get_kpis():
    """Indicadores principales para el dashboard."""
    q_contratos = "SELECT COUNT(*) as n, COALESCE(SUM(monto_adjudicado),0) as total FROM contratos"
    q_alertas   = "SELECT COUNT(*) as n FROM alertas WHERE resuelta = FALSE"
    q_alta      = "SELECT COUNT(*) as n FROM alertas WHERE nivel = 'alta' AND resuelta = FALSE"
    q_proveed   = "SELECT COUNT(DISTINCT cuit_proveedor) as n FROM contratos WHERE cuit_proveedor IS NOT NULL"
    q_func      = "SELECT COUNT(*) as n FROM nomina"

    c  = await database.fetch_one(q_contratos)
    a  = await database.fetch_one(q_alertas)
    al = await database.fetch_one(q_alta)
    p  = await database.fetch_one(q_proveed)
    f  = await database.fetch_one(q_func)

    return KPIOut(
        total_contratos=c["n"],
        monto_total_ars=float(c["total"]),
        alertas_activas=a["n"],
        alertas_alta=al["n"],
        proveedores_unicos=p["n"],
        funcionarios_monitoreados=f["n"],
    )


@app.get("/api/v1/alertas", tags=["Alertas"])
async def get_alertas(
    nivel: Optional[str] = Query(None, description="alta | media | baja"),
    tipo:  Optional[str] = Query(None, description="nepotismo | sobreprecio | concentracion"),
    limit: int = Query(50, le=500),
    offset: int = 0,
):
    """Lista de alertas activas con filtros opcionales."""
    where = "WHERE resuelta = FALSE"
    params = {}
    if nivel:
        where += " AND nivel = :nivel"
        params["nivel"] = nivel
    if tipo:
        where += " AND tipo = :tipo"
        params["tipo"] = tipo

    query = f"""
        SELECT id::text, tipo, nivel, titulo, descripcion,
               monto_involucrado, fecha_creacion::text
        FROM alertas {where}
        ORDER BY
            CASE nivel WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END,
            fecha_creacion DESC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = limit
    params["offset"] = offset
    rows = await database.fetch_all(query, params)
    return [dict(r) for r in rows]


@app.get("/api/v1/contratos", tags=["Contratos"])
async def get_contratos(
    organismo: Optional[str] = None,
    proveedor: Optional[str] = None,
    desde: Optional[date] = None,
    hasta: Optional[date] = None,
    limit: int = Query(100, le=1000),
    offset: int = 0,
):
    """Contratos con filtros."""
    where_parts = []
    params: dict = {"limit": limit, "offset": offset}

    if organismo:
        where_parts.append("organismo ILIKE :organismo")
        params["organismo"] = f"%{organismo}%"
    if proveedor:
        where_parts.append("proveedor ILIKE :proveedor")
        params["proveedor"] = f"%{proveedor}%"
    if desde:
        where_parts.append("fecha_adjudicacion >= :desde")
        params["desde"] = desde
    if hasta:
        where_parts.append("fecha_adjudicacion <= :hasta")
        params["hasta"] = hasta

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = f"""
        SELECT id::text, organismo, proveedor, cuit_proveedor,
               monto_adjudicado, tipo_proceso, fecha_adjudicacion::text
        FROM contratos {where}
        ORDER BY monto_adjudicado DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """
    rows = await database.fetch_all(query, params)
    return [dict(r) for r in rows]


@app.get("/api/v1/contratos/por-organismo", tags=["Contratos"])
async def contratos_por_organismo():
    """Gasto total por organismo para el gráfico de barras."""
    query = """
        SELECT organismo,
               COUNT(*) as cantidad,
               COALESCE(SUM(monto_adjudicado), 0) as monto_total
        FROM contratos
        WHERE organismo IS NOT NULL
        GROUP BY organismo
        ORDER BY monto_total DESC
        LIMIT 20
    """
    rows = await database.fetch_all(query)
    return [dict(r) for r in rows]


@app.get("/api/v1/contratos/top-proveedores", tags=["Contratos"])
async def top_proveedores(limit: int = Query(20, le=100)):
    """Top proveedores por monto total adjudicado."""
    query = """
        SELECT proveedor, cuit_proveedor,
               COUNT(*) as contratos,
               COALESCE(SUM(monto_adjudicado), 0) as monto_total
        FROM contratos
        WHERE proveedor IS NOT NULL
        GROUP BY proveedor, cuit_proveedor
        ORDER BY monto_total DESC
        LIMIT :limit
    """
    rows = await database.fetch_all(query, {"limit": limit})
    return [dict(r) for r in rows]


@app.get("/api/v1/nomina", tags=["Nómina"])
async def get_nomina(
    apellido: Optional[str] = None,
    organismo: Optional[str] = None,
    limit: int = Query(100, le=500),
    offset: int = 0,
):
    """Nómina de personal de la JGM."""
    where_parts = []
    params: dict = {"limit": limit, "offset": offset}

    if apellido:
        where_parts.append("apellido ILIKE :apellido")
        params["apellido"] = f"%{apellido}%"
    if organismo:
        where_parts.append("organismo ILIKE :organismo")
        params["organismo"] = f"%{organismo}%"

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    query = f"""
        SELECT id::text, nombre, apellido, cuil, cargo, tipo_contratacion,
               agrupamiento, nivel, organismo
        FROM nomina {where}
        ORDER BY apellido, nombre
        LIMIT :limit OFFSET :offset
    """
    rows = await database.fetch_all(query, params)
    return [dict(r) for r in rows]


@app.get("/api/v1/grafo/nodos", tags=["Grafo"])
async def grafo_nodos():
    """
    Devuelve nodos y aristas para el grafo de relaciones.
    Formato compatible con vis.js Network.
    """
    # Nodos: funcionarios
    func_query = """
        SELECT DISTINCT v.cuil_a as id,
               n.nombre || ' ' || n.apellido as label,
               n.cargo as title,
               'funcionario' as grupo
        FROM vinculos v
        JOIN nomina n ON n.cuil = v.cuil_a
        LIMIT 200
    """
    # Nodos: proveedores
    prov_query = """
        SELECT DISTINCT v.cuil_b as id,
               COALESCE(s.razon_social, v.cuil_b) as label,
               'Proveedor' as title,
               'proveedor' as grupo
        FROM vinculos v
        LEFT JOIN proveedores_sipro s ON s.cuit = v.cuil_b
        LIMIT 200
    """
    # Aristas
    edge_query = """
        SELECT cuil_a as from, cuil_b as to,
               tipo_vinculo as type,
               nivel_alerta as nivel
        FROM vinculos
        LIMIT 500
    """

    func_rows = await database.fetch_all(func_query)
    prov_rows = await database.fetch_all(prov_query)
    edge_rows = await database.fetch_all(edge_query)

    nodes = [dict(r) for r in func_rows] + [dict(r) for r in prov_rows]
    edges = [dict(r) for r in edge_rows]

    return {"nodes": nodes, "edges": edges}


@app.get("/api/v1/health", tags=["Sistema"])
async def health():
    try:
        await database.fetch_one("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))