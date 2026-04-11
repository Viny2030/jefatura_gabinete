"""
api_server.py
FastAPI - Portal Anticorrupción JGM
"""

import os
from contextlib import asynccontextmanager
from typing import Optional
from datetime import date

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv
import databases
import sqlalchemy
from pydantic import BaseModel

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL no configurada. Copiá .env.example a .env y completá los datos.")

database = databases.Database(DATABASE_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()


app = FastAPI(
    title="Portal Anticorrupción - JGM",
    description="API pública de monitoreo de contratos y alertas",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="src/frontend"), name="static")


class KPIOut(BaseModel):
    total_contratos: int
    monto_total_ars: float
    alertas_activas: int
    alertas_alta: int
    proveedores_unicos: int
    funcionarios_monitoreados: int


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse("src/frontend/dashboard.html")


@app.get("/api/v1/kpis", response_model=KPIOut, tags=["Dashboard"])
async def get_kpis():
    c  = await database.fetch_one("SELECT COUNT(*) as n, COALESCE(SUM(monto_adjudicado),0) as total FROM contratos")
    a  = await database.fetch_one("SELECT COUNT(*) as n FROM alertas WHERE resuelta = FALSE")
    al = await database.fetch_one("SELECT COUNT(*) as n FROM alertas WHERE nivel = 'alta' AND resuelta = FALSE")
    p  = await database.fetch_one("SELECT COUNT(DISTINCT cuit_proveedor) as n FROM contratos WHERE cuit_proveedor IS NOT NULL")
    f  = await database.fetch_one("SELECT COUNT(*) as n FROM nomina")
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
    nivel: Optional[str] = None,
    tipo:  Optional[str] = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
):
    where = "WHERE resuelta = FALSE"
    params = {}
    if nivel:
        where += " AND nivel = :nivel"
        params["nivel"] = nivel
    if tipo:
        where += " AND tipo = :tipo"
        params["tipo"] = tipo
    params["limit"]  = limit
    params["offset"] = offset
    query = f"""
        SELECT id::text, tipo, nivel, titulo, descripcion,
               monto_involucrado, fecha_creacion::text
        FROM alertas {where}
        ORDER BY CASE nivel WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END, fecha_creacion DESC
        LIMIT :limit OFFSET :offset
    """
    rows = await database.fetch_all(query, params)
    return [dict(r) for r in rows]


@app.get("/api/v1/contratos", tags=["Contratos"])
async def get_contratos(
    organismo: Optional[str] = None,
    proveedor: Optional[str] = None,
    limit: int = Query(100, le=1000),
    offset: int = 0,
):
    where_parts = []
    params: dict = {"limit": limit, "offset": offset}
    if organismo:
        where_parts.append("organismo ILIKE :organismo")
        params["organismo"] = f"%{organismo}%"
    if proveedor:
        where_parts.append("proveedor ILIKE :proveedor")
        params["proveedor"] = f"%{proveedor}%"
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
    query = """
        SELECT organismo, COUNT(*) as cantidad,
               COALESCE(SUM(monto_adjudicado), 0) as monto_total
        FROM contratos WHERE organismo IS NOT NULL
        GROUP BY organismo ORDER BY monto_total DESC LIMIT 20
    """
    rows = await database.fetch_all(query)
    return [dict(r) for r in rows]


@app.get("/api/v1/contratos/top-proveedores", tags=["Contratos"])
async def top_proveedores(limit: int = Query(20, le=100)):
    query = """
        SELECT proveedor, cuit_proveedor,
               COUNT(*) as contratos,
               COALESCE(SUM(monto_adjudicado), 0) as monto_total
        FROM contratos WHERE proveedor IS NOT NULL
        GROUP BY proveedor, cuit_proveedor
        ORDER BY monto_total DESC LIMIT :limit
    """
    rows = await database.fetch_all(query, {"limit": limit})
    return [dict(r) for r in rows]


# ── Contratos comprar.gob.ar ──────────────────────────────────────────────────

@app.get("/api/v1/contratos/comprar", tags=["Contratos"])
async def get_contratos_comprar(
    tipo:             Optional[str] = None,
    estado:           Optional[str] = None,
    unidad_ejecutora: Optional[str] = None,
    numero_proceso:   Optional[str] = None,
    limit:  int = Query(100, le=1000),
    offset: int = 0,
):
    """
    Lista de procesos de compra scrapeados de comprar.gob.ar.
    Filtros: tipo_proceso, estado, unidad_ejecutora, numero_proceso.
    """
    where_parts = []
    params: dict = {"limit": limit, "offset": offset}

    if tipo:
        where_parts.append("tipo_proceso ILIKE :tipo")
        params["tipo"] = f"%{tipo}%"
    if estado:
        where_parts.append("estado ILIKE :estado")
        params["estado"] = f"%{estado}%"
    if unidad_ejecutora:
        where_parts.append("unidad_ejecutora ILIKE :unidad_ejecutora")
        params["unidad_ejecutora"] = f"%{unidad_ejecutora}%"
    if numero_proceso:
        where_parts.append("numero_proceso ILIKE :numero_proceso")
        params["numero_proceso"] = f"%{numero_proceso}%"

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    count_query = f"SELECT COUNT(*) as n FROM contratos_comprar {where}"
    total = (await database.fetch_one(count_query, params))["n"]

    query = f"""
        SELECT
            numero_proceso,
            expediente,
            nombre_proceso,
            tipo_proceso,
            fecha_apertura,
            estado,
            unidad_ejecutora,
            saf,
            scraped_at::text
        FROM contratos_comprar
        {where}
        ORDER BY numero_proceso DESC
        LIMIT :limit OFFSET :offset
    """
    rows = await database.fetch_all(query, params)
    return {
        "total":  total,
        "limit":  limit,
        "offset": offset,
        "data":   [dict(r) for r in rows],
    }


@app.get("/api/v1/contratos/comprar/resumen", tags=["Contratos"])
async def resumen_contratos_comprar():
    """KPIs y agrupamientos de los contratos de comprar.gob.ar."""
    total = await database.fetch_one(
        "SELECT COUNT(*) as n, MAX(scraped_at)::text as ultima_actualizacion FROM contratos_comprar"
    )
    por_tipo = await database.fetch_all("""
        SELECT tipo_proceso, COUNT(*) as cantidad
        FROM contratos_comprar
        GROUP BY tipo_proceso ORDER BY cantidad DESC
    """)
    por_estado = await database.fetch_all("""
        SELECT estado, COUNT(*) as cantidad
        FROM contratos_comprar
        GROUP BY estado ORDER BY cantidad DESC
    """)
    por_unidad = await database.fetch_all("""
        SELECT unidad_ejecutora, COUNT(*) as cantidad
        FROM contratos_comprar
        GROUP BY unidad_ejecutora ORDER BY cantidad DESC LIMIT 10
    """)
    return {
        "total_procesos":       total["n"],
        "ultima_actualizacion": total["ultima_actualizacion"],
        "por_tipo_proceso":     [dict(r) for r in por_tipo],
        "por_estado":           [dict(r) for r in por_estado],
        "por_unidad_ejecutora": [dict(r) for r in por_unidad],
    }


# ── Nómina ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/nomina", tags=["Nómina"])
async def get_nomina(
    apellido:  Optional[str] = None,
    organismo: Optional[str] = None,
    limit:  int = Query(100, le=500),
    offset: int = 0,
):
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


# ── Grafo ─────────────────────────────────────────────────────────────────────

@app.get("/api/v1/grafo/nodos", tags=["Grafo"])
async def grafo_nodos():
    func_query = """
        SELECT DISTINCT v.cuil_a as id,
               n.nombre || ' ' || n.apellido as label,
               n.cargo as title, 'funcionario' as grupo
        FROM vinculos v JOIN nomina n ON n.cuil = v.cuil_a LIMIT 200
    """
    prov_query = """
        SELECT DISTINCT v.cuil_b as id,
               COALESCE(s.razon_social, v.cuil_b) as label,
               'Proveedor' as title, 'proveedor' as grupo
        FROM vinculos v LEFT JOIN proveedores_sipro s ON s.cuit = v.cuil_b LIMIT 200
    """
    edge_query = """
        SELECT cuil_a as from, cuil_b as to,
               tipo_vinculo as type, nivel_alerta as nivel
        FROM vinculos LIMIT 500
    """
    func_rows = await database.fetch_all(func_query)
    prov_rows = await database.fetch_all(prov_query)
    edge_rows = await database.fetch_all(edge_query)
    return {
        "nodes": [dict(r) for r in func_rows] + [dict(r) for r in prov_rows],
        "edges": [dict(r) for r in edge_rows],
    }


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/api/v1/health", tags=["Sistema"])
async def health():
    try:
        await database.fetch_one("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))