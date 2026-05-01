"""
test_pipeline.py
================
Tests unitarios para src/pipeline.py

Cubre:
  - generar_resumen()  → KPIs y agrupaciones
  - run_step()         → manejo de éxito, error y args
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pipeline import generar_resumen, run_step


# ─── generar_resumen ─────────────────────────────────────────────────────────

class TestGenerarResumen:

    def _intel(self, alertas=None, grafo_stats=None):
        return {
            "meta": {
                "total_alertas": len(alertas or []),
                "alertas_alta": sum(1 for a in (alertas or []) if a.get("nivel") == "ALTA"),
                "alertas_media": sum(1 for a in (alertas or []) if a.get("nivel") == "MEDIA"),
                "ultima_actualizacion": "2026-05-01T00:00:00",
            },
            "alertas": alertas or [],
            "grafo": {
                "stats": grafo_stats or {
                    "nodos_funcionarios": 0,
                    "nodos_empresas": 0,
                    "aristas_rojas": 0,
                    "aristas_amarillas": 0,
                }
            },
        }

    def test_none_retorna_dict_vacio(self):
        assert generar_resumen(None) == {}

    def test_vacio_retorna_kpis_en_cero(self):
        resumen = generar_resumen(self._intel())
        assert resumen["kpis"]["total_alertas"] == 0
        assert resumen["kpis"]["alertas_alta"] == 0
        assert resumen["kpis"]["alertas_media"] == 0

    def test_kpis_calculados_correctamente(self):
        alertas = [
            {"tipo_alerta": "NEPOTISMO", "nivel": "ALTA"},
            {"tipo_alerta": "NEPOTISMO", "nivel": "MEDIA"},
            {"tipo_alerta": "CONFLICTO_SOCIETARIO", "nivel": "ALTA"},
            {"tipo_alerta": "DESVIO_IAP_GLOBAL", "nivel": "MEDIA"},
            {"tipo_alerta": "DESVIO_IAP_GLOBAL", "nivel": "MEDIA"},
        ]
        resumen = generar_resumen(self._intel(alertas))
        assert resumen["kpis"]["total_alertas"] == 5
        assert resumen["kpis"]["alertas_alta"] == 2
        assert resumen["kpis"]["alertas_media"] == 3

    def test_kpis_grafo_incluidos(self):
        stats = {
            "nodos_funcionarios": 10,
            "nodos_empresas": 5,
            "aristas_rojas": 3,
            "aristas_amarillas": 7,
        }
        resumen = generar_resumen(self._intel(grafo_stats=stats))
        assert resumen["kpis"]["funcionarios_en_alerta"] == 10
        assert resumen["kpis"]["empresas_en_alerta"] == 5
        assert resumen["kpis"]["vinculos_familiares"] == 3
        assert resumen["kpis"]["vinculos_comerciales"] == 7

    def test_agrupacion_por_tipo(self):
        alertas = [
            {"tipo_alerta": "NEPOTISMO", "nivel": "ALTA"},
            {"tipo_alerta": "NEPOTISMO", "nivel": "MEDIA"},
            {"tipo_alerta": "CONFLICTO_SOCIETARIO", "nivel": "ALTA"},
        ]
        resumen = generar_resumen(self._intel(alertas))
        assert "NEPOTISMO" in resumen["por_tipo"]
        assert resumen["por_tipo"]["NEPOTISMO"]["total"] == 2
        assert resumen["por_tipo"]["NEPOTISMO"]["alta"] == 1
        assert resumen["por_tipo"]["NEPOTISMO"]["media"] == 1
        assert "CONFLICTO_SOCIETARIO" in resumen["por_tipo"]

    def test_conteos_por_tipo_son_correctos(self):
        alertas = [{"tipo_alerta": "DESVIO_IAP_GLOBAL", "nivel": "MEDIA"}] * 3
        resumen = generar_resumen(self._intel(alertas))
        assert resumen["por_tipo"]["DESVIO_IAP_GLOBAL"]["total"] == 3
        assert resumen["por_tipo"]["DESVIO_IAP_GLOBAL"]["alta"] == 0
        assert resumen["por_tipo"]["DESVIO_IAP_GLOBAL"]["media"] == 3

    def test_ultima_actualizacion_presente(self):
        resumen = generar_resumen(self._intel())
        assert "ultima_actualizacion" in resumen
        assert resumen["ultima_actualizacion"] == "2026-05-01T00:00:00"

    def test_fuentes_presentes(self):
        resumen = generar_resumen(self._intel())
        assert "fuentes" in resumen
        assert isinstance(resumen["fuentes"], list)
        assert len(resumen["fuentes"]) >= 1

    def test_fuentes_incluyen_bora_y_comprar(self):
        resumen = generar_resumen(self._intel())
        fuentes_str = " ".join(resumen["fuentes"]).upper()
        assert "BORA" in fuentes_str
        assert "COMPRAR" in fuentes_str or "SIPRO" in fuentes_str

    def test_tipo_otro_se_agrupa(self):
        """Alertas sin tipo definido deben caer en 'OTRO'."""
        alertas = [{"tipo_alerta": None, "nivel": "MEDIA"}]
        resumen = generar_resumen(self._intel(alertas))
        assert "OTRO" in resumen["por_tipo"]


# ─── run_step ────────────────────────────────────────────────────────────────

class TestRunStep:

    def test_step_exitoso_retorna_resultado(self):
        result = run_step("Test OK", lambda: {"valor": 42})
        assert result == {"valor": 42}

    def test_step_con_args_posicionales(self):
        result = run_step("Suma", lambda a, b: a + b, 3, 7)
        assert result == 10

    def test_step_con_kwargs(self):
        result = run_step("Kwargs", lambda x=0, y=0: x * y, x=4, y=5)
        assert result == 20

    def test_step_con_error_retorna_none(self):
        def func_falla():
            raise RuntimeError("Fallo de prueba")
        result = run_step("Test Error", func_falla)
        assert result is None

    def test_step_retorna_none_no_propaga_excepcion(self):
        """run_step no debe relanzar la excepción."""
        def func_explode():
            raise ValueError("Explosión controlada")
        try:
            result = run_step("Explosión", func_explode)
            assert result is None
        except ValueError:
            pytest.fail("run_step propagó la excepción — no debería hacerlo")

    def test_step_retorna_none_literal(self):
        result = run_step("None func", lambda: None)
        assert result is None

    def test_step_retorna_lista(self):
        result = run_step("Lista", lambda: [1, 2, 3])
        assert result == [1, 2, 3]

    def test_step_retorna_string(self):
        result = run_step("String", lambda: "ok")
        assert result == "ok"
