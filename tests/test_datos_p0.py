"""
test_datos_p0.py
================
scripts/verificar_cobertura.py (sin red).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import verificar_cobertura as vc  # noqa: E402

class TestVerificarCobertura:
    def test_medir_reconoce_proveedor_que_es_el_objeto(self):
        data = [
            {"proveedor": "Contratación de X", "objeto": "Contratación de X servicio", "cuit": ""},
            {"proveedor": "EJES SA", "objeto": "monitoreo", "cuit": "33-1", "monto_adjudicado": 10},
        ]
        assert vc.medir_contratos(data) == {"contratos": 2, "con_monto": 1, "con_razon": 1}

    def test_cero_montos_es_falla(self):
        act = {"contratos": 179, "con_monto": 0, "con_razon": 0}
        assert "0 contratos con monto" in vc.evaluar(act, None, 0.3, exigir_monto=True)

    def test_caida_mayor_al_umbral(self):
        act = {"contratos": 353, "con_monto": 8, "con_razon": 8}
        prev = {"contratos": 353, "con_monto": 20, "con_razon": 20}
        probs = vc.evaluar(act, prev, 0.3, exigir_monto=True)
        assert any("con_monto cayó 20 → 8" in p for p in probs)

    def test_caida_menor_al_umbral_ok(self):
        act = {"contratos": 263, "con_monto": 170, "con_razon": 172}
        prev = {"contratos": 263, "con_monto": 174, "con_razon": 176}
        assert vc.evaluar(act, prev, 0.3, exigir_monto=True) == []

    def test_medir_detalle_csv(self):
        txt = "﻿numero_proceso,monto_adjudicado,proveedor_razon\n1,10,A\n2,,\n"
        assert vc.medir_detalle(txt) == {"contratos": 2, "con_monto": 1, "con_razon": 1}
