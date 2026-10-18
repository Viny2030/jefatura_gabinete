"""
test_datos_p0.py
================
scripts/proteger_detalle.py y scripts/verificar_cobertura.py (sin git ni red).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import proteger_detalle as pd_  # noqa: E402
import verificar_cobertura as vc  # noqa: E402

COLS = ["numero_proceso", "monto_adjudicado", "proveedor_razon", "proveedor_cuit",
        "moneda", "fecha_perfeccionamiento", "detalle_fecha"]


def fila(nup, monto="", razon="", cuit="", moneda="", fecha="2026-10-14"):
    return {"numero_proceso": nup, "monto_adjudicado": monto, "proveedor_razon": razon,
            "proveedor_cuit": cuit, "moneda": moneda, "fecha_perfeccionamiento": "",
            "detalle_fecha": fecha}


class TestProtegerDetalle:
    def test_restaura_lo_borrado_desde_el_commit_mas_reciente(self):
        actuales = [fila("79-0001-CDI24")]
        snaps = [
            ("bbb", [fila("79-0001-CDI24")]),                                       # ya borrado
            ("aaa", [fila("79-0001-CDI24", "14400000.00", "EJES SA", "33-66341994-9")]),
            ("zzz", [fila("79-0001-CDI24", "1.00", "VIEJO SA")]),                   # más viejo
        ]
        rest = pd_.recuperar(actuales, snaps, COLS)
        assert actuales[0]["proveedor_razon"] == "EJES SA"
        assert actuales[0]["monto_adjudicado"] == "14400000.00"
        assert {r[2] for r in rest} == {"aaa"}

    def test_no_pisa_datos_actuales(self):
        actuales = [fila("23-0001", "5.00", "NUEVO SA", moneda="ARS")]
        rest = pd_.recuperar(actuales, [("aaa", [fila("23-0001", "1.00", "VIEJO SA")])], COLS)
        assert rest == [] and actuales[0]["proveedor_razon"] == "NUEVO SA"

    def test_no_mezcla_commits(self):
        """Razón social de un commit y monto de otro nunca se combinan."""
        actuales = [fila("79-1")]
        snaps = [("bbb", [fila("79-1", razon="SOLO RAZON SA")]),
                 ("aaa", [fila("79-1", "9.00", "OTRA SA")])]
        pd_.recuperar(actuales, snaps, COLS)
        assert actuales[0]["proveedor_razon"] == "SOLO RAZON SA"
        assert actuales[0]["monto_adjudicado"] == ""

    def test_sin_historial_no_cambia_nada(self):
        actuales = [fila("79-1")]
        assert pd_.recuperar(actuales, [], COLS) == []

    def test_vacios_equivalentes(self):
        for v in ("", " ", "0", "0.00", "nan", None):
            assert pd_._vacio(v)
        assert not pd_._vacio("EJES SA")


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
