"""GET /api/frescura con los datos del repo."""
from fastapi.testclient import TestClient

from src.api.api_server import app


def test_frescura_jefatura():
    c = TestClient(app)
    d = c.get("/api/frescura").json()
    assert d["formato"] == "frescura/1" and d["sitio"] == "jefatura"
    assert {x["id"] for x in d["datos"]} == {"contratos", "nomina", "cruces", "presupuesto", "sigen"}
    assert all(x["dato"] and x["fuente"] for x in d["datos"])
    assert c.get("/data/frescura.js").status_code == 200
    assert "frescura-ficha" in c.get("/").text


def test_presupuesto_toma_la_fecha_de_la_fuente():
    fila = next(x for x in TestClient(app).get("/api/frescura").json()["datos"] if x["id"] == "presupuesto")
    assert fila["periodo_fin"], "no se pudo leer fecha_actualizacion_fuente"
