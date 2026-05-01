"""
test_matrix_corporate.py
========================
Tests unitarios para src/engine/matrix_corporate.py

Cubre:
  - detectar_funcionario_proveedor() → cruce CUIL/CUIT, normalización, alertas
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "engine"))

import pandas as pd
import pytest

from matrix_corporate import detectar_funcionario_proveedor


# ─── helpers ─────────────────────────────────────────────────────────────────

def _nomina(cuil="20-12345678-9", nombre="Luis", apellido="Villarino",
            cargo="Secretario", organismo="JGM"):
    return pd.DataFrame([{
        "cuil": cuil,
        "nombre": nombre,
        "apellido": apellido,
        "cargo": cargo,
        "organismo": organismo,
    }])


def _contratos(cuit="20-12345678-9", proveedor="VILLARINO LUIS",
               monto=800_000, organismo="JGM"):
    return pd.DataFrame([{
        "id": "C001",
        "cuit_proveedor": cuit,
        "proveedor": proveedor,
        "monto_adjudicado": monto,
        "organismo": organismo,
    }])


# ─── tests ───────────────────────────────────────────────────────────────────

class TestDetectarFuncionarioProveedor:

    def test_match_directo_genera_alerta_alta(self):
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos())
        assert len(resultado) == 1
        assert resultado[0]["nivel_alerta"] == "alta"

    def test_subtipo_correcto(self):
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos())
        assert resultado[0]["subtipo"] == "funcionario_es_proveedor"
        assert resultado[0]["tipo_vinculo"] == "societario"

    def test_estructura_resultado(self):
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos())
        r = resultado[0]
        assert "cuil_a" in r
        assert "cuil_b" in r
        assert "tipo_vinculo" in r
        assert "subtipo" in r
        assert "nivel_alerta" in r
        assert "detalle" in r
        assert "fuente" in r
        assert "fecha_deteccion" in r

    def test_detalle_contiene_campos_clave(self):
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos())
        det = resultado[0]["detalle"]
        assert "funcionario" in det
        assert "cargo" in det
        assert "proveedor" in det
        assert "monto" in det

    def test_normaliza_cuil_con_guiones(self):
        """CUIL '20-12345678-9' debe coincidir con CUIT '20123456789'."""
        resultado = detectar_funcionario_proveedor(
            _nomina(cuil="20-12345678-9"),
            _contratos(cuit="20123456789"),
        )
        assert len(resultado) == 1

    def test_normaliza_cuil_sin_guiones(self):
        """CUIL '20123456789' debe coincidir con CUIT '20-12345678-9'."""
        resultado = detectar_funcionario_proveedor(
            _nomina(cuil="20123456789"),
            _contratos(cuit="20-12345678-9"),
        )
        assert len(resultado) == 1

    def test_sin_match_retorna_lista_vacia(self):
        resultado = detectar_funcionario_proveedor(
            _nomina(cuil="20-11111111-1"),
            _contratos(cuit="30-99999999-9"),
        )
        assert resultado == []

    def test_nomina_vacia_retorna_lista_vacia(self):
        assert detectar_funcionario_proveedor(pd.DataFrame(), _contratos()) == []

    def test_contratos_vacios_retorna_lista_vacia(self):
        assert detectar_funcionario_proveedor(_nomina(), pd.DataFrame()) == []

    def test_ambas_vacias_retorna_lista_vacia(self):
        assert detectar_funcionario_proveedor(pd.DataFrame(), pd.DataFrame()) == []

    def test_monto_es_float(self):
        """El monto en detalle debe ser un float (no NaN ni None)."""
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos(monto=500_000))
        assert isinstance(resultado[0]["detalle"]["monto"], float)
        assert resultado[0]["detalle"]["monto"] == 500_000.0

    def test_monto_none_no_explota(self):
        """Si monto_adjudicado es None, el resultado no debe lanzar excepción.
        pandas convierte None en NaN al hacer merge, así que el monto puede ser NaN.
        Lo importante es que no explote y que devuelva un resultado."""
        resultado = detectar_funcionario_proveedor(
            _nomina(),
            _contratos(monto=None),
        )
        assert len(resultado) == 1
        # NaN o 0 son aceptables — lo importante es que no lance excepción
        monto = resultado[0]["detalle"]["monto"]
        assert monto == 0 or monto != monto  # monto != monto es True sólo para NaN

    def test_multiples_contratos_mismo_cuil(self):
        """Un funcionario puede aparecer en varios contratos."""
        nomina = _nomina(cuil="20-12345678-9")
        contratos = pd.DataFrame([
            {"id": "C001", "cuit_proveedor": "20-12345678-9", "proveedor": "P1",
             "monto_adjudicado": 100_000, "organismo": "JGM"},
            {"id": "C002", "cuit_proveedor": "20-12345678-9", "proveedor": "P1",
             "monto_adjudicado": 200_000, "organismo": "SGP"},
        ])
        resultado = detectar_funcionario_proveedor(nomina, contratos)
        assert len(resultado) == 2

    def test_fuente_es_cruce_cuil_cuit(self):
        resultado = detectar_funcionario_proveedor(_nomina(), _contratos())
        assert resultado[0]["fuente"] == "cruce_cuil_cuit"
