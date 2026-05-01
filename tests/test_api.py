"""
test_api.py
===========
Tests de los endpoints FastAPI usando TestClient (sin DB real).

Cubre:
  GET  /api/health        → 200, campos requeridos
  GET  /api/alertas       → 200, filtros nivel/tipo
  GET  /api/contratos     → 200, filtros tipo/monto_min
  GET  /api/resumen       → 200, fallback a inteligencia.json
  GET  /api/bora          → 200, filtro relevante_jgm
  GET  /api/grafo         → 200 o 404 según datos
  POST /api/refresh       → 401 sin token, 401 token incorrecto
  POST /api/init-db       → 401 sin token
"""
import sys
import os
from unittest.mock import patch

import pytest

# Asegurar que no haya DATABASE_URL real en tests
os.environ["DATABASE_URL"] = ""
os.environ["REFRESH_TOKEN"] = "test_token_secreto"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient
from api_server import app

client = TestClient(app, raise_server_exceptions=False)

# ── Datos de muestra ──────────────────────────────────────────────────────────

INTELIGENCIA = {
    "meta": {
        "ultima_actualizacion": "2026-05-01T00:00:00",
        "total_alertas": 3,
        "alertas_alta": 1,
        "alertas_media": 2,
    },
    "alertas": [
        {"id": "1", "tipo_alerta": "NEPOTISMO", "nivel": "ALTA",
         "titulo": "Test ALTA", "descripcion": "desc"},
        {"id": "2", "tipo_alerta": "CONFLICTO_SOCIETARIO", "nivel": "MEDIA",
         "titulo": "Test MEDIA", "descripcion": "desc"},
        {"id": "3", "tipo_alerta": "DESVIO_IAP_GLOBAL", "nivel": "MEDIA",
         "titulo": "Test MEDIA 2", "descripcion": "desc"},
    ],
    "grafo": {
        "nodos": [{"id": "f1", "tipo": "funcionario"}],
        "aristas": [],
        "stats": {
            "nodos_funcionarios": 2,
            "nodos_empresas": 1,
            "aristas_rojas": 1,
            "aristas_amarillas": 2,
        },
    },
}

CONTRATOS = {
    "contratos": [
        {"id": "C001", "tipo": "adjudicacion", "proveedor": "EMPRESA A",
         "monto_estimado": 1_000_000, "organismo": "JGM"},
        {"id": "C002", "tipo": "convocatoria", "proveedor": "EMPRESA B",
         "monto_estimado": 200_000, "organismo": "SGP"},
    ],
    "meta": {"total": 2},
}

BORA = [
    {"titulo": "Designación JGM", "fecha": "2026-05-01", "relevante_jgm": True},
    {"titulo": "Norma SGP", "fecha": "2026-05-01", "relevante_jgm": False},
]

RESUMEN = {
    "kpis": {"total_alertas": 3, "alertas_alta": 1, "alertas_media": 2},
    "ultima_actualizacion": "2026-05-01T00:00:00",
}


# ─── /api/health ─────────────────────────────────────────────────────────────

class TestHealth:

    def test_retorna_200(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_campo_status_ok(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/health").json()
        assert data["status"] == "ok"

    def test_campos_requeridos_presentes(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/health").json()
        for campo in ["status", "timestamp", "total_alertas", "alertas_alta", "db_conectada"]:
            assert campo in data, f"Campo '{campo}' faltante en /api/health"

    def test_db_conectada_false_sin_database_url(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/health").json()
        assert data["db_conectada"] is False

    def test_total_alertas_viene_del_meta(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/health").json()
        assert data["total_alertas"] == INTELIGENCIA["meta"]["total_alertas"]


# ─── /api/alertas ─────────────────────────────────────────────────────────────

class TestAlertas:

    def test_retorna_200(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            resp = client.get("/api/alertas")
        assert resp.status_code == 200

    def test_estructura_correcta(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas").json()
        assert "alertas" in data
        assert "total" in data
        assert isinstance(data["alertas"], list)

    def test_total_coincide_con_len(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas").json()
        assert data["total"] == len(data["alertas"])

    def test_filtro_nivel_alta(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas?nivel=ALTA").json()
        assert all(a["nivel"].upper() == "ALTA" for a in data["alertas"])
        assert data["total"] == 1

    def test_filtro_nivel_media(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas?nivel=MEDIA").json()
        assert all(a["nivel"].upper() == "MEDIA" for a in data["alertas"])
        assert data["total"] == 2

    def test_filtro_tipo_nepotismo(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas?tipo=NEPOTISMO").json()
        assert all("NEPOTISMO" in a["tipo_alerta"].upper() for a in data["alertas"])

    def test_filtro_tipo_inexistente_retorna_vacio(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            data = client.get("/api/alertas?tipo=TIPO_INEXISTENTE").json()
        assert data["total"] == 0
        assert data["alertas"] == []

    def test_sin_datos_retorna_503(self):
        with patch("api_server._load", return_value={}):
            resp = client.get("/api/inteligencia")
        assert resp.status_code == 503


# ─── /api/contratos ───────────────────────────────────────────────────────────

class TestContratos:

    def test_retorna_200(self):
        with patch("api_server._load", return_value=CONTRATOS):
            resp = client.get("/api/contratos")
        assert resp.status_code == 200

    def test_estructura_correcta(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos").json()
        assert "contratos" in data
        assert "total" in data
        assert isinstance(data["contratos"], list)

    def test_filtro_tipo_adjudicacion(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos?tipo=adjudicacion").json()
        assert all(c["tipo"] == "adjudicacion" for c in data["contratos"])

    def test_filtro_tipo_convocatoria(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos?tipo=convocatoria").json()
        assert all(c["tipo"] == "convocatoria" for c in data["contratos"])

    def test_filtro_monto_min(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos?monto_min=500000").json()
        for c in data["contratos"]:
            assert (c.get("monto_estimado") or 0) >= 500_000

    def test_contratos_ordenados_por_monto_desc(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos").json()
        montos = [c.get("monto_estimado") or 0 for c in data["contratos"]]
        assert montos == sorted(montos, reverse=True)

    def test_limit_parametro(self):
        with patch("api_server._load", return_value=CONTRATOS):
            data = client.get("/api/contratos?limit=1").json()
        assert len(data["contratos"]) <= 1


# ─── /api/resumen ─────────────────────────────────────────────────────────────

class TestResumen:

    def test_retorna_resumen_directo(self):
        with patch("api_server._load", return_value=RESUMEN):
            resp = client.get("/api/resumen")
        assert resp.status_code == 200

    def test_fallback_a_inteligencia(self):
        """Si resumen.json está vacío, usa inteligencia.json."""
        def mock_load(nombre):
            if nombre == "resumen.json":
                return {}
            return INTELIGENCIA

        with patch("api_server._load", side_effect=mock_load):
            resp = client.get("/api/resumen")
        assert resp.status_code == 200
        data = resp.json()
        assert "kpis" in data
        assert data["kpis"]["total_alertas"] == 3

    def test_sin_datos_retorna_503(self):
        with patch("api_server._load", return_value={}):
            resp = client.get("/api/resumen")
        assert resp.status_code == 503


# ─── /api/bora ────────────────────────────────────────────────────────────────

class TestBora:

    def test_retorna_200(self):
        with patch("api_server._load", return_value=BORA):
            resp = client.get("/api/bora")
        assert resp.status_code == 200

    def test_solo_relevantes_por_defecto(self):
        with patch("api_server._load", return_value=BORA):
            data = client.get("/api/bora").json()
        assert all(p["relevante_jgm"] for p in data["publicaciones"])

    def test_todas_cuando_relevante_false(self):
        with patch("api_server._load", return_value=BORA):
            data = client.get("/api/bora?relevante_jgm=false").json()
        # Deben aparecer las no relevantes también
        assert data["total"] == len(BORA)


# ─── /api/grafo ───────────────────────────────────────────────────────────────

class TestGrafo:

    def test_retorna_200_con_datos(self):
        with patch("api_server._load", return_value=INTELIGENCIA):
            resp = client.get("/api/grafo")
        assert resp.status_code == 200

    def test_retorna_404_sin_grafo(self):
        data_sin_grafo = {**INTELIGENCIA, "grafo": {}}
        with patch("api_server._load", return_value=data_sin_grafo):
            resp = client.get("/api/grafo")
        assert resp.status_code == 404


# ─── /api/refresh ─────────────────────────────────────────────────────────────

class TestRefresh:

    def test_sin_token_retorna_401(self):
        resp = client.post("/api/refresh")
        assert resp.status_code == 401

    def test_token_invalido_retorna_401(self):
        resp = client.post("/api/refresh", headers={"X-Refresh-Token": "token_incorrecto"})
        assert resp.status_code == 401

    def test_mensaje_error_token_invalido(self):
        data = client.post(
            "/api/refresh",
            headers={"X-Refresh-Token": "mal_token"}
        ).json()
        assert "detail" in data
        assert "inválido" in data["detail"].lower() or "invalid" in data["detail"].lower()


# ─── /api/init-db ─────────────────────────────────────────────────────────────

class TestInitDb:

    def test_sin_token_retorna_401(self):
        resp = client.post("/api/init-db")
        assert resp.status_code == 401

    def test_token_invalido_retorna_401(self):
        resp = client.post("/api/init-db", headers={"X-Refresh-Token": "bad"})
        assert resp.status_code == 401
