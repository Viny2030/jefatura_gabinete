"""Cruce por apellido (scripts/generar_cruces_pen.py) y rótulo del grafo."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import generar_cruces_pen as g  # noqa: E402


def c(apellido, razon, cuit):
    return g.coincide_apellido(g.normalizar(apellido), g.normalizar(razon), cuit)


def test_coincidencias_que_se_mantienen():
    assert c("PONTORIERO", "PABLO PONTORIERO", "20174539724")
    assert c("GALLI", "Alejandro Luis Galli", "20184140757")
    assert c("SOUZA", "R.A.H. REFRIGERACION de SOUZA A y SOUZA R SOC DE HECHO", "30709261084")
    assert c("ZUBIRI", "ZUBIRI HNOS S.A.", "30700000001")


def test_homenajes_en_instituciones():
    assert not c("JAURETCHE", "Universidad Nacional Arturo Jauretche", "30711340161")
    assert not c("GARRAHAN", "HOSPITAL DE PEDIATRIA JUAN P GARRAHAN", "30000000002")


def test_palabra_comun_en_nombre_de_fantasia():
    assert not c("MEDICO", "SOCORRO MEDICO PRIVADO SA", "30612213417")


def test_apellidos_que_son_nombres_de_pila():
    assert not c("GERMAN", "German Gianattasio", "20200068263")
    assert not c("GERMAN", "VAN ZANDWEGHE MARCELO L Y VAN ZANDWEGHE GERMAN P SH", "30680288158")
    assert not c("GIULIANO", "Giuliano Woloszanowski", "20399145466")
    assert not c("ARTURO", "ARTURO PEREZ SRL", "30700000003")


def test_nombres_de_pila_de_la_nomina(monkeypatch):
    monkeypatch.setattr(g, "NOMBRES_PILA", set(g.NOMBRES_PILA_BASE) | {"ZOILO"})
    assert not c("ZOILO", "ZOILO BARRIENTOS", "20100000004")


def test_grafo_no_rotula_parentesco(monkeypatch):
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from src.api import api_server
    monkeypatch.setattr(api_server, "_cruces_lista", lambda: [{
        "tipo": "Apellido en razón social — posible vínculo", "nivel": "MEDIO",
        "funcionario": "PONTORIERO, Hugo", "cargo": "Director", "empresa": "PABLO PONTORIERO",
        "cuit": "20174539724", "organismo": "JGM",
    }])
    d = api_server.get_grafo_v1()
    assert [e["type"] for e in d["edges"]] == ["apellido"]
    assert {n["label"] for n in d["nodes"]} == {"PONTORIERO, Hugo", "PABLO PONTORIERO"}
