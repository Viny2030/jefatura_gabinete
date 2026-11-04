"""
test_nomina_bime.py
===================
El export BIME "estructura_autoridades_APN" (oct 2026) renombró las columnas
de Mapa del Estado (uo_jurisdiccion, aut_apellido, ...). Sin mapearlas, el
workflow nomina_manual publicó personal_*.json vacíos y el portal mostró
0 autoridades. Estos tests fijan el mapeo y el corte por formato desconocido.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from scraper_nomina import normalizar_columnas, validar_nomina  # noqa: E402

COLS_BIME = [
    "uo_jurisdiccion", "uo_subjurisdiccion", "uo_unidad_superior", "unidad",
    "uo_reporta_a", "uo_nombre_corto", "uo_tipo_administracion", "uo_jerarquia",
    "uo_clase", "uo_normativa", "car_orden", "cargo", "car_nivel",
    "car_rango_jerarquia", "car_categoria", "car_escalafon", "car_suplemento",
    "aut_tratamiento", "aut_nombre", "aut_apellido", "aut_dni", "aut_cuil",
    "aut_sexo", "aut_norma_designacion", "uo_web",
]


def _df_bime():
    fila = {c: None for c in COLS_BIME}
    fila.update({
        "uo_jurisdiccion": "Jefatura de Gabinete de Ministros",
        "cargo": "Jefe de Gabinete de Ministros",
        "car_rango_jerarquia": "Ministro",
        "aut_nombre": "Diego César",
        "aut_apellido": "SANTILLI",
        "aut_cuil": 20177354490,
        "aut_sexo": "M",
        "aut_norma_designacion": "[Decreto 548/2026](https://www.boletinoficial.gob.ar/detalleAviso/primera/343705/20260630);",
    })
    return pd.DataFrame([fila])


def test_columnas_bime_se_mapean():
    df = normalizar_columnas(_df_bime())
    for col in ("organismo", "sub_organismo", "apellido", "nombre", "cuil",
                "genero", "norma_designacion", "jerarquia", "escalafon"):
        assert col in df.columns, col
    assert df.loc[0, "organismo"] == "Jefatura de Gabinete de Ministros"
    assert df.loc[0, "apellido"] == "SANTILLI"


def test_bime_pasa_validacion():
    validar_nomina(normalizar_columnas(_df_bime()))


def test_formato_desconocido_corta():
    df = normalizar_columnas(pd.DataFrame([{"xx_jurisdiccion": "JGM", "cargo": "x"}]))
    with pytest.raises(SystemExit):
        validar_nomina(df)


def test_sin_filas_del_monitor_corta():
    df = _df_bime()
    df["uo_jurisdiccion"] = "Ministerio de Economía"
    with pytest.raises(SystemExit):
        validar_nomina(normalizar_columnas(df))
