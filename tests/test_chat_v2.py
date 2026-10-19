"""
test_chat_v2.py
===============
Agente IA v2 (src/api/chat_v2.py + src/api/server_v2.py), sin red ni API key real.

Cada caso reproduce una falla observada en producción con el agente v1:
  - "EJES SA" no encontraba "EJES S.A."
  - "Microsoft" no aparecía en SGP (está en el objeto; el proveedor es MSLI Latam)
  - búsquedas con/sin tilde
  - rankings armados sobre una muestra cortada a 4000 caracteres
"""
import json
import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "api"))

import chat_v2  # noqa: E402

CONTRATOS_JGM = [
    {"numero_proceso": "79-0001-CDI24", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "Contratación de la firma EJES S.A. - servicio mensual de monitoreo de medios de ",
     "cuit": "", "monto_adjudicado": None, "moneda": "", "estado": "Adjudicado",
     "objeto": "Contratación de la firma EJES S.A. - servicio mensual de monitoreo de medios de comunicación",
     "fecha_adjudicacion": "2024-03-15", "ejercicio": 2024},
    {"numero_proceso": "79-0002-LPU25", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "LA MANTOVANA DE SERVICIOS GENERALES S.A.", "cuit": "30-69605181-6",
     "monto_adjudicado": 574107438.0, "moneda": "ARS", "estado": "Adjudicado",
     "objeto": "Limpieza integral", "fecha_adjudicacion": "2025-02-01", "ejercicio": 2025},
    {"numero_proceso": "79-0003-LPU25", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "GRUB S. A.", "cuit": "30-71464746-2", "monto_adjudicado": 690000000.0,
     "moneda": "ARS", "estado": "Adjudicado", "objeto": "Espacios verdes",
     "fecha_adjudicacion": "2026-01-10", "ejercicio": 2026},
    {"numero_proceso": "79-0004-LPU26", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "GRUB S. A.", "cuit": "30714647462", "monto_adjudicado": 38520000.0,
     "moneda": "ARS", "estado": "Adjudicado", "objeto": "Poda",
     "fecha_adjudicacion": "2026-03-10", "ejercicio": 2026},
    {"numero_proceso": "79-0005-CDI25", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "MSLI Latam, Inc.", "cuit": "", "monto_adjudicado": 768787.56,
     "moneda": "USD", "estado": "Adjudicado",
     "objeto": "Adquisición de licencias y suscripción de productos Windows MICROSOFT",
     "fecha_adjudicacion": "2025-01-23", "ejercicio": 2025},
    {"numero_proceso": "79-0006-CDI23", "organismo": "79/000 - DGTA - JGM",
     "proveedor": "Contratación Directa por Especialidad - \"Intervenciones\" (GONZÁLEZ DEL RÍO P",
     "cuit": "", "monto_adjudicado": None, "estado": "Adjudicado",
     "objeto": "Contratación Directa por Especialidad - \"Intervenciones\" (GONZÁLEZ DEL RÍO Pablo)",
     "fecha_adjudicacion": "2023-01-27", "ejercicio": 2023},
]
AMPLIADA = [
    {"numero_proceso": "91-0001-CDI23", "organismo": "91/00 - CONICET", "proveedor": "ACME SRL",
     "cuit": "30-11111111-1", "monto_adjudicado": 1000.0, "moneda": "ARS", "objeto": "Traslados",
     "estado": "Adjudicado"},
]
PERSONAL_JGM = [
    {"apellido": "SANTILLI", "nombre": "Diego César", "cargo": "Jefe de Gabinete de Ministros",
     "organismo": "Jefatura de Gabinete de Ministros", "cuil": 20123456789.0},
    {"apellido": "PÉREZ", "nombre": "Ana", "cargo": "Directora de Compras",
     "organismo": "Jefatura de Gabinete de Ministros", "cuil": None},
]
CRUCES = {"cruces": [
    {"nivel": "MEDIO", "area": "jgm", "tipo": "Apellido en razón social — posible vínculo",
     "funcionario": "SICILIA, Federico Esteban", "empresa": "EMBOTELLADORA SICILIA SRL",
     "cuit": "30717087549", "organismo": "15 - Compras"},
    {"nivel": "MEDIO", "area": "presidencia", "tipo": "Apellido en razón social — posible vínculo",
     "funcionario": "GALLI, Juan Manuel", "empresa": "Alejandro Luis Galli", "cuit": "20184140757"},
]}


@pytest.fixture(autouse=True)
def datos(tmp_path, monkeypatch):
    d = tmp_path / "data"
    (d / "por_area").mkdir(parents=True)
    archivos = {
        "contratos_jgm.json": CONTRATOS_JGM,
        "contratos_jgm_ampliada.json": AMPLIADA,
        "contratos_sgp.json": [],
        "contratos_presidencia.json": [],
        "personal_jgm.json": PERSONAL_JGM,
        "cruces.json": CRUCES,
        "por_area/contratos_salud.json": [
            {"numero_proceso": "80-0001-LPU25", "proveedor": "LABORATORIO SUR SA", "cuit": "30-22222222-2",
             "monto_adjudicado": 5.0, "objeto": "Vacunas"}],
    }
    for nombre, contenido in archivos.items():
        (d / nombre).write_text(json.dumps(contenido, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(chat_v2, "DATA_DIRS", [d])
    chat_v2._cache.clear()
    chat_v2._rate.clear()
    yield d


# ── Normalización ─────────────────────────────────────────────────────────────

class TestNormalizacion:
    def test_tildes_mayusculas_y_puntos(self):
        assert chat_v2.normalizar("GONZÁLEZ del Río S.A.") == "gonzalez del rio sa"

    def test_sufijo_con_espacio(self):
        assert chat_v2.normalizar("GRUB S. A.") == "grub s a"

    def test_sufijos_no_obligatorios(self):
        assert chat_v2.tokens_requeridos("EJES SA") == ["ejes"]
        assert chat_v2.tokens_requeridos("S.R.L.") == ["srl"]   # solo sufijo: se respeta

    def test_coincide_prefijo_de_palabra(self):
        assert chat_v2.coincide("EJES SA", "Contratación de la firma EJES S.A.")
        assert not chat_v2.coincide("jes", "Contratación de la firma EJES S.A.")

    def test_none_seguro(self):
        assert chat_v2.normalizar(None) == ""


# ── buscar_contratos ──────────────────────────────────────────────────────────

class TestBuscarContratos:
    def test_ejes_sa_encuentra_ejes_s_a(self):
        r = chat_v2.tool_buscar_contratos("jgm", texto="EJES SA")
        assert r["total_coincidencias"] == 1
        assert r["resultados"][0]["numero_proceso"] == "79-0001-CDI24"
        # el proveedor era el título del proceso → se informa como sin razón social
        assert r["resultados"][0]["proveedor"] is None

    def test_busca_en_objeto(self):
        r = chat_v2.tool_buscar_contratos("jgm", texto="Microsoft")
        assert [x["proveedor"] for x in r["resultados"]] == ["MSLI Latam, Inc."]

    def test_sin_tilde_encuentra_con_tilde(self):
        r = chat_v2.tool_buscar_contratos("jgm", texto="gonzalez del rio")
        assert r["total_coincidencias"] == 1

    @pytest.mark.parametrize("cuit", ["30-71464746-2", "30714647462", "30 71464746 2"])
    def test_cuit_con_y_sin_guiones(self, cuit):
        r = chat_v2.tool_buscar_contratos("jgm", texto=cuit)
        assert r["total_coincidencias"] == 2

    def test_numero_de_proceso(self):
        assert chat_v2.tool_buscar_contratos("jgm", texto="79-0002-LPU25")["total_coincidencias"] == 1

    def test_orden_por_monto_y_totales(self):
        r = chat_v2.tool_buscar_contratos("jgm")
        assert r["total_coincidencias"] == 6
        assert r["resultados"][0]["proveedor"] == "GRUB S. A."
        assert r["con_monto"] == 4

    def test_alcance_ampliada_y_todo_pen(self):
        assert chat_v2.tool_buscar_contratos("jgm", texto="acme")["total_coincidencias"] == 0
        assert chat_v2.tool_buscar_contratos("jgm", texto="acme", alcance="ampliada")["total_coincidencias"] == 1
        assert chat_v2.tool_buscar_contratos("jgm", texto="vacunas", alcance="todo_pen")["total_coincidencias"] == 1

    def test_alcance_invalido_cae_en_area(self):
        assert chat_v2.tool_buscar_contratos("jgm", alcance="marte")["alcance"] == "area"

    def test_sugerencias_por_error_de_tipeo(self):
        r = chat_v2.tool_buscar_contratos("jgm", texto="Mantovna")
        assert r["total_coincidencias"] == 0
        assert "LA MANTOVANA DE SERVICIOS GENERALES S.A." in r["sugerencias"]


# ── Empaquetado ───────────────────────────────────────────────────────────────

class TestEmpaquetar:
    def test_nunca_corta_json_y_respeta_tope(self):
        items = [{"objeto": "x" * 500, "n": i} for i in range(50)]
        r = chat_v2.empaquetar(items, 50, max_chars=3000)
        s = json.dumps(r, ensure_ascii=False)
        assert len(s) <= 3000
        assert json.loads(s)["truncado"] is True
        assert r["mostrando"] < 50 and r["total_coincidencias"] == 50


# ── Agregaciones ──────────────────────────────────────────────────────────────

class TestAgregaciones:
    def test_ranking_agrega_por_cuit_y_moneda(self):
        r = chat_v2.tool_ranking_proveedores("jgm", moneda="ARS")
        top = r["resultados"][0]
        assert top["proveedor"] == "GRUB S. A."
        assert top["contratos"] == 2           # CUIT con y sin guiones = mismo proveedor
        assert top["monto_total"] == 728520000.0
        assert all(x["proveedor"] != "MSLI Latam, Inc." for x in r["resultados"])   # es USD

    def test_ranking_usd(self):
        r = chat_v2.tool_ranking_proveedores("jgm", moneda="USD")
        assert [x["proveedor"] for x in r["resultados"]] == ["MSLI Latam, Inc."]

    def test_resumen_informa_cobertura(self):
        r = chat_v2.tool_resumen_area("jgm")
        assert r["contratos"] == 6 and r["con_monto"] == 4 and r["con_razon_social"] == 4
        assert r["cobertura_monto_pct"] == 66.7
        assert r["personal"] == 2

    def test_system_prompt_incluye_cobertura(self):
        p = chat_v2.system_prompt("jgm")
        assert "6 contratos" in p and "TEXTO PLANO" in p


# ── Personal y cruces ─────────────────────────────────────────────────────────

class TestPersonalYCruces:
    def test_personal_por_nombre_o_cargo_sin_tilde(self):
        assert chat_v2.tool_buscar_personal("jgm", texto="perez")["total_coincidencias"] == 1
        assert chat_v2.tool_buscar_personal("jgm", texto="diego")["total_coincidencias"] == 1
        assert chat_v2.tool_buscar_personal("jgm", texto="compras")["total_coincidencias"] == 1

    def test_personal_por_cuil_float(self):
        assert chat_v2.tool_buscar_personal("jgm", cuil="20-12345678-9")["total_coincidencias"] == 1

    def test_cruces_por_empresa_area_y_cuit(self):
        assert chat_v2.tool_buscar_cruces("jgm", texto="embotelladora")["total_coincidencias"] == 1
        assert chat_v2.tool_buscar_cruces("jgm", solo_esta_area=True)["total_coincidencias"] == 1
        assert chat_v2.tool_buscar_cruces("jgm", texto="20-18414075-7")["total_coincidencias"] == 1

    def test_tool_desconocida_y_parametro_invalido(self):
        assert "error" in chat_v2.ejecutar_tool("borrar_todo", "jgm", {})
        assert "error" in chat_v2.ejecutar_tool("buscar_contratos", "jgm", {"inventado": 1})


# ── Endpoint ──────────────────────────────────────────────────────────────────

from fastapi.testclient import TestClient  # noqa: E402
import server_v2  # noqa: E402

client = TestClient(server_v2.app, raise_server_exceptions=False)


def _resp_tool(nombre, entrada):
    return {"stop_reason": "tool_use",
            "content": [{"type": "tool_use", "id": "t1", "name": nombre, "input": entrada}]}


def _resp_texto(texto):
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": texto}]}


class TestEndpoint:
    def test_rutas_v1_siguen_existiendo(self):
        def _extraer_rutas(routes):
            paths = set()
            for r in routes:
                if hasattr(r, "path"):
                    paths.add(r.path)
                elif hasattr(r, "routes"):
                    paths.update(_extraer_rutas(r.routes))
            return paths

        rutas = _extraer_rutas(server_v2.app.routes)
        assert {"/api/v1/chat/{area}", "/api/v2/chat/{area}", "/api/health"} <= rutas

    def test_503_sin_api_key(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "")
        r = client.post("/api/v2/chat/jgm", json={"mensaje": "hola"})
        assert r.status_code == 503

    def test_area_invalida(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "x")
        assert client.post("/api/v2/chat/marte", json={"mensaje": "hola"}).status_code == 404

    def test_loop_de_herramientas(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "x")
        mock = AsyncMock(side_effect=[_resp_tool("buscar_contratos", {"texto": "EJES SA"}),
                                      _resp_texto("Encontré 1 contrato: 79-0001-CDI24.")])
        with patch.object(chat_v2, "llamar_claude", mock):
            r = client.post("/api/v2/chat/jgm", json={"mensaje": "¿Contratos de EJES SA?"})
        assert r.status_code == 200
        d = r.json()
        assert d["herramientas"] == ["buscar_contratos"] and d["version"] == 2
        # el resultado de la tool que vio el modelo es JSON válido y trae el proceso
        tool_msg = mock.call_args_list[1].args[1][-1]["content"][0]["content"]
        assert json.loads(tool_msg)["resultados"][0]["numero_proceso"] == "79-0001-CDI24"

    def test_historial_alternado(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "x")
        mock = AsyncMock(return_value=_resp_texto("ok"))
        hist = [{"role": "assistant", "texto": "hola"}, {"role": "user", "texto": "a"},
                {"role": "user", "texto": "b"}]
        with patch.object(chat_v2, "llamar_claude", mock):
            client.post("/api/v2/chat/jgm", json={"mensaje": "c", "historial": hist})
        msgs = mock.call_args.args[1]
        roles = [m["role"] for m in msgs]
        assert roles[0] == "user" and all(a != b for a, b in zip(roles, roles[1:]))

    def test_rate_limit_por_ip_real(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "x")
        monkeypatch.setattr(chat_v2, "CHAT_RATE_LIMIT_DIA", 1)
        with patch.object(chat_v2, "llamar_claude", AsyncMock(return_value=_resp_texto("ok"))):
            a1 = client.post("/api/v2/chat/jgm", json={"mensaje": "x"},
                             headers={"X-Forwarded-For": "1.1.1.1"})
            a2 = client.post("/api/v2/chat/jgm", json={"mensaje": "x"},
                             headers={"X-Forwarded-For": "1.1.1.1"})
            b1 = client.post("/api/v2/chat/jgm", json={"mensaje": "x"},
                             headers={"X-Forwarded-For": "2.2.2.2"})
            # una IP forjada a la izquierda no evade el límite
            a3 = client.post("/api/v2/chat/jgm", json={"mensaje": "x"},
                             headers={"X-Forwarded-For": "9.9.9.9, 1.1.1.1"})
        assert (a1.status_code, a2.status_code, b1.status_code, a3.status_code) == (200, 429, 200, 429)

    def test_diagnostico_sin_secretos(self, monkeypatch):
        monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "sk-ant-secreto")
        r = client.get("/api/v2/chat/diagnostico")
        assert r.status_code == 200
        assert "sk-ant" not in r.text
        assert r.json()["areas"]["jgm"]["con_monto"] == 4

    def test_reemplazo_v1_opt_in(self, monkeypatch):
        monkeypatch.delenv("CHAT_V2_REEMPLAZA_V1", raising=False)
        assert server_v2._reemplazar_chat_v1() is False
        monkeypatch.setenv("CHAT_V2_REEMPLAZA_V1", "1")
        try:
            assert server_v2._reemplazar_chat_v1() is True
            monkeypatch.setattr(chat_v2, "ANTHROPIC_API_KEY", "x")
            with patch.object(chat_v2, "llamar_claude", AsyncMock(return_value=_resp_texto("v2"))):
                r = client.post("/api/v1/chat/jgm", json={"mensaje": "x"})
            assert r.json().get("version") == 2
        finally:
            server_v2.app.router.routes.pop(0)
