"""
test_api.py
===========
Tests de los endpoints FastAPI usando TestClient + mocks sobre la DB.

Cubre:
  GET  /api/v1/health              → 200, campos requeridos
  GET  /api/v1/kpis                → 200, estructura KPIOut
  GET  /api/v1/alertas             → 200, filtros nivel/tipo
  GET  /api/v1/contratos           → 200, filtros organismo/proveedor
  GET  /api/v1/contratos/comprar   → 200, estructura paginada
  GET  /api/v1/nomina              → 200, filtros apellido/organismo
  GET  /api/v1/grafo/nodos         → 200, nodos y aristas
  GET  /api/cruce-cuit             → 200, respuesta MEACI
"""
import sys
import os
from unittest.mock import patch, AsyncMock, MagicMock

import pytest

# Inyectar DATABASE_URL válida antes de importar app
os.environ["DATABASE_URL"] = "postgresql://fake:fake@localhost/fake"
os.environ["REFRESH_TOKEN"] = "test_token_secreto"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "api"))

from fastapi.testclient import TestClient
from api_server import app

# ── Helper para simular filas de DB ──────────────────────────────────────────

def make_row(**kwargs):
    """Simula una fila de resultado de `databases` (accesible por clave)."""
    m = MagicMock()
    m.__getitem__ = lambda self, key: kwargs[key]
    m.keys = lambda: kwargs.keys()
    # Para dict(row)
    m._mapping = kwargs
    return m


def dict_row(**kwargs):
    """Fila que se convierte correctamente con dict(row)."""
    class Row(dict):
        def __getitem__(self, key):
            return super().__getitem__(key)
    return Row(kwargs)


# ── Cliente ───────────────────────────────────────────────────────────────────

# Saltamos lifespan (connect/disconnect) para no necesitar DB real
client = TestClient(app, raise_server_exceptions=False)


# ─── /api/v1/health ──────────────────────────────────────────────────────────

class TestHealth:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(return_value=dict_row(n=1))
            resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_campos_requeridos(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(return_value=dict_row(n=1))
            data = client.get("/api/v1/health").json()
        assert "status" in data
        assert data["status"] == "ok"

    def test_db_error_retorna_503(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(side_effect=Exception("connection error"))
            resp = client.get("/api/v1/health")
        assert resp.status_code == 503


# ─── /api/v1/kpis ─────────────────────────────────────────────────────────────

class TestKPIs:

    def _mock_kpis(self, mock_db):
        mock_db.fetch_one = AsyncMock(side_effect=[
            dict_row(n=10, total=5_000_000.0),  # contratos
            dict_row(n=3),                        # alertas activas
            dict_row(n=1),                        # alertas alta
            dict_row(n=7),                        # proveedores únicos
            dict_row(n=25),                       # nómina
        ])

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            self._mock_kpis(mock_db)
            resp = client.get("/api/v1/kpis")
        assert resp.status_code == 200

    def test_estructura_correcta(self):
        with patch("api_server.database") as mock_db:
            self._mock_kpis(mock_db)
            data = client.get("/api/v1/kpis").json()
        for campo in ["total_contratos", "monto_total_ars", "alertas_activas",
                      "alertas_alta", "proveedores_unicos", "funcionarios_monitoreados"]:
            assert campo in data, f"Campo '{campo}' faltante en /api/v1/kpis"

    def test_valores_correctos(self):
        with patch("api_server.database") as mock_db:
            self._mock_kpis(mock_db)
            data = client.get("/api/v1/kpis").json()
        assert data["total_contratos"] == 10
        assert data["monto_total_ars"] == 5_000_000.0
        assert data["alertas_activas"] == 3
        assert data["alertas_alta"] == 1
        assert data["proveedores_unicos"] == 7
        assert data["funcionarios_monitoreados"] == 25


# ─── /api/v1/alertas ──────────────────────────────────────────────────────────

ALERTAS_ROWS = [
    dict_row(id="1", tipo="NEPOTISMO", nivel="alta", titulo="Test alta",
             descripcion="desc", monto_involucrado=100000, fecha_creacion="2026-05-01"),
    dict_row(id="2", tipo="CONFLICTO", nivel="media", titulo="Test media",
             descripcion="desc", monto_involucrado=50000, fecha_creacion="2026-04-01"),
]


class TestAlertas:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=ALERTAS_ROWS)
            resp = client.get("/api/v1/alertas")
        assert resp.status_code == 200

    def test_retorna_lista(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=ALERTAS_ROWS)
            data = client.get("/api/v1/alertas").json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_filtro_nivel(self):
        fila = [dict_row(id="1", tipo="NEPOTISMO", nivel="alta", titulo="T",
                         descripcion="d", monto_involucrado=0, fecha_creacion="2026-05-01")]
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=fila)
            data = client.get("/api/v1/alertas?nivel=alta").json()
        assert len(data) == 1
        assert data[0]["nivel"] == "alta"

    def test_filtro_tipo(self):
        fila = [dict_row(id="2", tipo="CONFLICTO", nivel="media", titulo="T",
                         descripcion="d", monto_involucrado=0, fecha_creacion="2026-04-01")]
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=fila)
            data = client.get("/api/v1/alertas?tipo=CONFLICTO").json()
        assert data[0]["tipo"] == "CONFLICTO"

    def test_lista_vacia(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[])
            data = client.get("/api/v1/alertas").json()
        assert data == []


# ─── /api/v1/contratos ────────────────────────────────────────────────────────

CONTRATOS_ROWS = [
    dict_row(id="C001", organismo="JGM", proveedor="EMPRESA A", cuit_proveedor="20123456789",
             monto_adjudicado=1_000_000.0, tipo_proceso="adjudicacion",
             fecha_adjudicacion="2026-05-01"),
    dict_row(id="C002", organismo="SGP", proveedor="EMPRESA B", cuit_proveedor="20987654321",
             monto_adjudicado=500_000.0, tipo_proceso="convocatoria",
             fecha_adjudicacion="2026-04-01"),
]


class TestContratos:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=CONTRATOS_ROWS)
            resp = client.get("/api/v1/contratos")
        assert resp.status_code == 200

    def test_retorna_lista(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=CONTRATOS_ROWS)
            data = client.get("/api/v1/contratos").json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_campos_contrato(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[CONTRATOS_ROWS[0]])
            data = client.get("/api/v1/contratos").json()
        for campo in ["id", "organismo", "proveedor", "monto_adjudicado", "tipo_proceso"]:
            assert campo in data[0], f"Campo '{campo}' faltante en contrato"

    def test_filtro_organismo(self):
        fila = [CONTRATOS_ROWS[0]]
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=fila)
            data = client.get("/api/v1/contratos?organismo=JGM").json()
        assert data[0]["organismo"] == "JGM"

    def test_filtro_proveedor(self):
        fila = [CONTRATOS_ROWS[1]]
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=fila)
            data = client.get("/api/v1/contratos?proveedor=EMPRESA+B").json()
        assert data[0]["proveedor"] == "EMPRESA B"

    def test_lista_vacia(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[])
            data = client.get("/api/v1/contratos").json()
        assert data == []


# ─── /api/v1/contratos/comprar ────────────────────────────────────────────────

COMPRAR_ROWS = [
    dict_row(numero_proceso="EX-2026-001", expediente="EXP-001",
             nombre_proceso="Licitación Test", tipo_proceso="licitacion_publica",
             fecha_apertura="2026-05-10", estado="publicado",
             unidad_ejecutora="JGM", saf="001", scraped_at="2026-05-01"),
]


class TestContratosComprar:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(return_value=dict_row(n=1))
            mock_db.fetch_all = AsyncMock(return_value=COMPRAR_ROWS)
            resp = client.get("/api/v1/contratos/comprar")
        assert resp.status_code == 200

    def test_estructura_paginada(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(return_value=dict_row(n=1))
            mock_db.fetch_all = AsyncMock(return_value=COMPRAR_ROWS)
            data = client.get("/api/v1/contratos/comprar").json()
        for campo in ["total", "limit", "offset", "data"]:
            assert campo in data, f"Campo '{campo}' faltante en respuesta paginada"
        assert isinstance(data["data"], list)

    def test_total_correcto(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_one = AsyncMock(return_value=dict_row(n=5))
            mock_db.fetch_all = AsyncMock(return_value=COMPRAR_ROWS)
            data = client.get("/api/v1/contratos/comprar").json()
        assert data["total"] == 5


# ─── /api/v1/nomina ───────────────────────────────────────────────────────────

NOMINA_ROWS = [
    dict_row(id="N001", nombre="Juan", apellido="Pérez", cuil="20123456789",
             cargo="Director", tipo_contratacion="planta", agrupamiento="profesional",
             nivel="A", organismo="JGM"),
    dict_row(id="N002", nombre="María", apellido="García", cuil="27987654321",
             cargo="Analista", tipo_contratacion="contrato", agrupamiento="técnico",
             nivel="B", organismo="SGP"),
]


class TestNomina:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=NOMINA_ROWS)
            resp = client.get("/api/v1/nomina")
        assert resp.status_code == 200

    def test_retorna_lista(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=NOMINA_ROWS)
            data = client.get("/api/v1/nomina").json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_campos_nomina(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[NOMINA_ROWS[0]])
            data = client.get("/api/v1/nomina").json()
        for campo in ["id", "nombre", "apellido", "cargo", "organismo"]:
            assert campo in data[0], f"Campo '{campo}' faltante en nómina"

    def test_filtro_apellido(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[NOMINA_ROWS[0]])
            data = client.get("/api/v1/nomina?apellido=Pérez").json()
        assert data[0]["apellido"] == "Pérez"

    def test_filtro_organismo(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[NOMINA_ROWS[1]])
            data = client.get("/api/v1/nomina?organismo=SGP").json()
        assert data[0]["organismo"] == "SGP"


# ─── /api/v1/grafo/nodos ──────────────────────────────────────────────────────

class TestGrafo:

    def test_retorna_200(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[])
            resp = client.get("/api/v1/grafo/nodos")
        assert resp.status_code == 200

    def test_estructura_nodos_aristas(self):
        with patch("api_server.database") as mock_db:
            mock_db.fetch_all = AsyncMock(return_value=[])
            data = client.get("/api/v1/grafo/nodos").json()
        assert "nodes" in data
        assert "edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)


# ─── /api/cruce-cuit ──────────────────────────────────────────────────────────

class TestCruceCuit:

    def test_retorna_200_con_cuit(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"cuit": "20123456789", "sancionado": False}

        with patch("api_server.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.get = AsyncMock(return_value=mock_response)
            resp = client.get("/api/cruce-cuit?cuit=20123456789")
        assert resp.status_code == 200

    def test_error_externo_retorna_igual_200(self):
        with patch("api_server.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.get = AsyncMock(side_effect=Exception("timeout"))
            resp = client.get("/api/cruce-cuit?cuit=20123456789")
        assert resp.status_code == 200
        data = resp.json()
        assert "error" in data or "sancionado" in data
