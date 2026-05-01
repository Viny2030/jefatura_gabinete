"""
test_matrix_kinship.py
======================
Tests unitarios para src/engine/matrix_kinship.py

Cubre:
  - extraer_apellido_proveedor()  → tokenización y filtros
  - detectar_coincidencias_apellido() → cruces nómina/contratos, niveles de alerta
"""
import sys
import os

# Ajustar path para importar módulos del proyecto
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "engine"))

import pandas as pd
import pytest

from matrix_kinship import extraer_apellido_proveedor, detectar_coincidencias_apellido


# ─── extraer_apellido_proveedor ───────────────────────────────────────────────

class TestExtraerApellidoProveedor:

    def test_persona_fisica_simple(self):
        tokens = extraer_apellido_proveedor("RAMIREZ CARLOS")
        assert "ramirez" in tokens

    def test_empresa_sa_filtrada(self):
        """SA, SRL, SAS no deben aparecer como tokens."""
        tokens = extraer_apellido_proveedor("TECH SOLUCIONES SA")
        assert "sa" not in tokens
        assert "srl" not in tokens

    def test_empresa_srl_filtrada(self):
        tokens = extraer_apellido_proveedor("BERTOLDO SERVICIOS SRL")
        assert "srl" not in tokens
        assert "bertoldo" in tokens

    def test_apellido_compuesto(self):
        tokens = extraer_apellido_proveedor("FERNANDEZ GARCIA JOSE")
        assert "fernandez" in tokens

    def test_input_none_retorna_lista_vacia(self):
        assert extraer_apellido_proveedor(None) == []

    def test_input_vacio_retorna_lista_vacia(self):
        assert extraer_apellido_proveedor("") == []

    def test_input_numero_retorna_lista_vacia(self):
        assert extraer_apellido_proveedor(12345) == []  # type: ignore

    def test_tokens_cortos_excluidos(self):
        """Tokens de < 4 caracteres deben excluirse (MIN_APELLIDO_LEN = 4)."""
        tokens = extraer_apellido_proveedor("GIL ANA SA")
        assert "gil" not in tokens   # len = 3
        assert "ana" not in tokens   # len = 3

    def test_tokens_exactamente_4_chars_incluidos(self):
        """Tokens con exactamente 4 caracteres sí deben incluirse."""
        tokens = extraer_apellido_proveedor("VEGA LUNA")
        assert "vega" in tokens   # len = 4
        assert "luna" in tokens   # len = 4

    def test_puntuacion_eliminada(self):
        """Puntuación adherida al token debe eliminarse."""
        tokens = extraer_apellido_proveedor("MONTEIRO, CONSULTORES.")
        assert "monteiro" in tokens

    def test_palabras_conectoras_filtradas(self):
        for stop in ["y", "e", "de", "del", "la", "el"]:
            tokens = extraer_apellido_proveedor(f"EMPRESA {stop.upper()} SERVICIOS")
            assert stop not in tokens, f"Stop word '{stop}' no debería aparecer"


# ─── detectar_coincidencias_apellido ─────────────────────────────────────────

class TestDetectarCoincidenciasApellido:

    def _nomina(self, apellido="Ramirez", organismo="JGM"):
        return pd.DataFrame([{
            "cuil": "20-12345678-9",
            "nombre": "Carlos",
            "apellido": apellido,
            "cargo": "Director",
            "organismo": organismo,
        }])

    def _contratos(self, proveedor="RAMIREZ CONSULTORA SRL", organismo="JGM"):
        return pd.DataFrame([{
            "id": "C001",
            "cuit_proveedor": "30-99999999-1",
            "proveedor": proveedor,
            "monto_adjudicado": 500_000,
            "organismo": organismo,
            "fecha_adjudicacion": "2026-01-01",
        }])

    def test_detecta_coincidencia_mismo_organismo_nivel_alta(self):
        resultado = detectar_coincidencias_apellido(self._nomina(), self._contratos())
        assert len(resultado) > 0
        assert resultado[0]["nivel_alerta"] == "alta"

    def test_detecta_coincidencia_distinto_organismo_nivel_media(self):
        resultado = detectar_coincidencias_apellido(
            self._nomina(organismo="JGM"),
            self._contratos(organismo="SGP"),
        )
        assert len(resultado) > 0
        assert resultado[0]["nivel_alerta"] == "media"

    def test_apellido_comun_no_genera_alerta(self):
        """Apellidos como 'Garcia', 'Lopez' deben ignorarse."""
        resultado = detectar_coincidencias_apellido(
            self._nomina(apellido="Garcia"),
            self._contratos(proveedor="GARCIA IMPRESIONES SA"),
        )
        assert len(resultado) == 0

    def test_estructura_resultado(self):
        resultado = detectar_coincidencias_apellido(self._nomina(), self._contratos())
        assert len(resultado) > 0
        r = resultado[0]
        assert "cuil_a" in r
        assert "cuil_b" in r
        assert r["tipo_vinculo"] == "parentesco"
        assert r["subtipo"] == "apellido_coincidente"
        assert "detalle" in r
        assert "fuente" in r
        assert "fecha_deteccion" in r

    def test_detalle_contiene_campos_clave(self):
        resultado = detectar_coincidencias_apellido(self._nomina(), self._contratos())
        detalle = resultado[0]["detalle"]
        assert "funcionario" in detalle
        assert "proveedor" in detalle
        assert "monto" in detalle
        assert "mismo_organismo" in detalle
        assert "token_coincidente" in detalle

    def test_nomina_vacia_retorna_lista_vacia(self):
        resultado = detectar_coincidencias_apellido(pd.DataFrame(), self._contratos())
        assert resultado == []

    def test_contratos_vacios_retorna_lista_vacia(self):
        resultado = detectar_coincidencias_apellido(self._nomina(), pd.DataFrame())
        assert resultado == []

    def test_ambas_vacias_retorna_lista_vacia(self):
        assert detectar_coincidencias_apellido(pd.DataFrame(), pd.DataFrame()) == []

    def test_sin_coincidencia_no_genera_alerta(self):
        resultado = detectar_coincidencias_apellido(
            self._nomina(apellido="Villarreal"),
            self._contratos(proveedor="COMPUTACION CENTRAL SA"),
        )
        assert len(resultado) == 0

    def test_multiples_coincidencias(self):
        """Un apellido que aparece en varios contratos debe generar múltiples alertas."""
        nomina = self._nomina(apellido="Bertoldo")
        contratos = pd.DataFrame([
            {"id": "C1", "cuit_proveedor": "30-1", "proveedor": "BERTOLDO SERVICIOS SA",
             "monto_adjudicado": 100_000, "organismo": "JGM", "fecha_adjudicacion": "2026-01-01"},
            {"id": "C2", "cuit_proveedor": "30-2", "proveedor": "BERTOLDO Y ASOCIADOS SRL",
             "monto_adjudicado": 200_000, "organismo": "JGM", "fecha_adjudicacion": "2026-02-01"},
        ])
        resultado = detectar_coincidencias_apellido(nomina, contratos)
        assert len(resultado) == 2
