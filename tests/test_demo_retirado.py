"""El prototipo con datos de demostración no vuelve al portal público."""
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent / "src" / "frontend"


def test_el_prototipo_no_esta_en_el_repo():
    assert not (RAIZ / "dashboard.html").exists()


def test_ninguna_pagina_enlaza_al_prototipo():
    for html in RAIZ.rglob("*.html"):
        texto = html.read_text(encoding="utf-8")
        assert 'href="dashboard.html"' not in texto, html
        assert 'href="/dashboard.html"' not in texto, html
