"""
test_live.py
============
Smoke tests contra el entorno productivo en Railway.
Se ejecutan diariamente para verificar que la app esté en pie.

Uso:
    pytest tests/test_live.py -v -m live

La variable de entorno LIVE_URL puede sobreescribir la URL default.
"""
import os
import pytest
import requests

BASE_URL = os.environ.get(
    "LIVE_URL",
    "https://jefaturagabinete-production.up.railway.app"
).rstrip("/")

TIMEOUT = 20  # segundos


# ─── helpers ─────────────────────────────────────────────────────────────────

def get(path: str, **kwargs) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


def post(path: str, **kwargs) -> requests.Response:
    return requests.post(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


# ─── tests ───────────────────────────────────────────────────────────────────

@pytest.mark.live
class TestLiveHealth:

    def test_health_responde_200(self):
        resp = get("/api/health")
        assert resp.status_code == 200, (
            f"Health check falló con {resp.status_code}. Body: {resp.text[:200]}"
        )

    def test_health_json_valido(self):
        resp = get("/api/health")
        data = resp.json()
        assert data.get("status") == "ok", f"status inesperado: {data}"

    def test_health_tiene_timestamp(self):
        data = get("/api/health").json()
        assert "timestamp" in data
        assert data["timestamp"]  # no vacío

    def test_health_tiene_total_alertas(self):
        data = get("/api/health").json()
        assert "total_alertas" in data
        assert isinstance(data["total_alertas"], int)

    def test_health_tiempo_respuesta_aceptable(self):
        import time
        inicio = time.time()
        get("/api/health")
        duracion = time.time() - inicio
        assert duracion < 10, f"Respuesta demasiado lenta: {duracion:.1f}s"


@pytest.mark.live
class TestLiveFrontend:

    def test_raiz_responde_200(self):
        resp = get("/")
        assert resp.status_code == 200

    def test_raiz_es_html(self):
        resp = get("/")
        assert "text/html" in resp.headers.get("content-type", "").lower()

    def test_docs_fastapi_accesibles(self):
        """FastAPI expone /docs por defecto."""
        resp = get("/docs")
        assert resp.status_code == 200

    def test_openapi_json_accesible(self):
        resp = get("/openapi.json")
        assert resp.status_code == 200
        data = resp.json()
        assert "openapi" in data
        assert "paths" in data


@pytest.mark.live
class TestLiveApiEndpoints:

    def test_alertas_accesible(self):
        resp = get("/api/alertas")
        assert resp.status_code in (200, 503), (
            f"Respuesta inesperada en /api/alertas: {resp.status_code}"
        )

    def test_alertas_json_si_200(self):
        resp = get("/api/alertas")
        if resp.status_code == 200:
            data = resp.json()
            assert "alertas" in data
            assert "total" in data

    def test_contratos_accesible(self):
        resp = get("/api/contratos")
        assert resp.status_code in (200, 503)

    def test_contratos_json_si_200(self):
        resp = get("/api/contratos")
        if resp.status_code == 200:
            data = resp.json()
            assert "contratos" in data

    def test_resumen_accesible(self):
        resp = get("/api/resumen")
        assert resp.status_code in (200, 503)

    def test_bora_accesible(self):
        resp = get("/api/bora")
        assert resp.status_code in (200, 503)

    def test_inteligencia_accesible(self):
        resp = get("/api/inteligencia")
        assert resp.status_code in (200, 503)

    def test_db_status_accesible(self):
        resp = get("/api/db-status")
        assert resp.status_code == 200
        data = resp.json()
        assert "conectada" in data


@pytest.mark.live
class TestLiveSecurity:

    def test_refresh_sin_token_retorna_401(self):
        resp = post("/api/refresh")
        assert resp.status_code == 401, (
            "El endpoint /api/refresh debe rechazar requests sin token"
        )

    def test_refresh_token_falso_retorna_401(self):
        resp = post("/api/refresh", headers={"X-Refresh-Token": "token_falso_12345"})
        assert resp.status_code == 401

    def test_init_db_sin_token_retorna_401(self):
        resp = post("/api/init-db")
        assert resp.status_code == 401

    def test_no_expone_info_sensible_en_health(self):
        data = get("/api/health").json()
        body_str = str(data).lower()
        # No debe filtrarse DATABASE_URL ni tokens
        assert "postgresql://" not in body_str
        assert "password" not in body_str
        assert "secret" not in body_str


@pytest.mark.live
class TestLiveCORS:

    def test_cors_headers_en_get(self):
        resp = get(
            "/api/health",
            headers={"Origin": "https://example.com"}
        )
        # La API tiene CORS abierto (allow_origins=["*"])
        acao = resp.headers.get("access-control-allow-origin", "")
        assert acao == "*" or acao == "https://example.com", (
            f"CORS header ausente o incorrecto: '{acao}'"
        )

    def test_preflight_options_no_falla(self):
        resp = requests.options(
            f"{BASE_URL}/api/health",
            headers={
                "Origin": "https://example.com",
                "Access-Control-Request-Method": "GET",
            },
            timeout=TIMEOUT,
        )
        assert resp.status_code < 500, (
            f"Preflight CORS respondió con error {resp.status_code}"
        )
