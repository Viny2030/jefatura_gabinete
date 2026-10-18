"""
test_scraper_detalle.py
=======================
scripts/scraper_detalle_contratos.py sin red: parser de la vista del pliego,
payload de búsqueda, merge de contratos_detalle.csv y motivos de fallo.
"""
import csv
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import scraper_detalle_contratos as sc  # noqa: E402

# ── HTML de ejemplo (estructura real de VistaPreviaPliegoCiudadano.aspx) ──────

HTML_CONTRACTUAL = """
<html><body>
<h4>Documento contractual por proveedor</h4>
<table class="table table-hover" id="ctl00_CPH1_UCVistaPreviaPliego_UCDetalleImputacionAdjudicacion_gvDetalleImputacion">
<thead><tr class="tr-header">
<th scope="col">Número</th><th scope="col">Nombre proveedor</th><th scope="col">Número CUIT</th>
<th scope="col">Tipo</th><th scope="col">Estado</th><th scope="col">Fecha perfeccionamiento</th>
<th scope="col">Monto</th><th scope="col">Moneda</th>
</tr></thead><tbody>
<tr><td><a class="btn btn-link">79-0005-OC24</a></td><td>EJES SA</td><td>33663419949</td>
<td><span>Original</span></td><td>Cumplido</td><td>11/4/2024</td><td>14.400.000,00</td><td>Peso Argentino</td></tr>
<tr><td><a>79-0009-OC24</a></td><td>EJES SA</td><td>33663419949</td>
<td><span>Ampliación</span></td><td>Cumplido</td><td>01/9/2024</td><td>2.880.000,00</td><td>Peso Argentino</td></tr>
</tbody></table>
</body></html>
"""

HTML_VARIOS_SIN_ORIGINAL = """
<h4>Documento contractual por proveedor</h4>
<table><tr><th>Número</th><th>Nombre proveedor</th><th>Número CUIT</th><th>Tipo</th>
<th>Estado</th><th>Fecha perfeccionamiento</th><th>Monto</th><th>Moneda</th></tr>
<tr><td>23-1</td><td>CHICO SRL</td><td>30-71504200-9</td><td>Ampliación</td><td>x</td><td>1/1/2026</td><td>1.000,50</td><td>Peso Argentino</td></tr>
<tr><td>23-2</td><td>GRUB S. A.</td><td>30714647462</td><td>Ampliación</td><td>x</td><td>06/8/2026</td><td>690.000.000,00</td><td>Peso Argentino</td></tr>
</table>
"""

HTML_SOLO_PROVEEDOR = """
<h4>Proveedores</h4>
<table><tr><th>Razón social</th><th>Número CUIT</th></tr>
<tr><td>MSLI Latam, Inc.</td><td>30-12345678-9</td></tr></table>
"""

HTML_CONTRACTUAL_SIN_MONTO = """
<h4>Documento contractual por proveedor</h4>
<table><tr><th>Número</th><th>Nombre proveedor</th><th>Número CUIT</th><th>Tipo</th>
<th>Estado</th><th>Fecha perfeccionamiento</th><th>Monto</th><th>Moneda</th></tr>
<tr><td>79-1</td><td>ACME SA</td><td>30-11111111-1</td><td>Original</td><td>x</td><td></td><td></td><td></td></tr>
</table>
"""

HTML_SIN_TABLA = "<html><body><h4>Cronograma</h4><p>Sin datos</p></body></html>"

# Respuesta delta de ASP.NET AJAX con un resultado en la grilla.
DELTA_GRILLA = (
    "1|#||4|60|updatePanel|ctl00_CPH1_UpdatePanel1|"
    '<table id="ctl00_CPH1_GridListaPliegos">'
    '<tr><td><a id="ctl00_CPH1_GridListaPliegos_ctl02_lnkNumeroProceso">79-0001-CDI23</a></td></tr>'
    '<tr><td><a id="ctl00_CPH1_GridListaPliegos_ctl03_lnkNumeroProceso">79-0001-CDI24</a></td></tr>'
    "</table>|0|hiddenField|__VIEWSTATE|NUEVOVS|0|asyncPostBackControlIDs||"
)


# ── Parser ───────────────────────────────────────────────────────────────────

class TestParser:
    def test_documento_contractual_toma_el_original(self):
        r = sc.parsear_detalle(HTML_CONTRACTUAL)
        assert r["monto_adjudicado"] == 14400000.0
        assert r["proveedor_razon"] == "EJES SA"
        assert r["proveedor_cuit"] == "33-66341994-9"
        assert r["moneda"] == "Peso Argentino"
        assert r["fecha_perfeccionamiento"] == "11/4/2024"
        assert r["tabla"] == "contractual"
        assert sc.motivo_detalle(r) is None

    def test_sin_original_toma_el_mayor_monto(self):
        r = sc.parsear_detalle(HTML_VARIOS_SIN_ORIGINAL)
        assert r["monto_adjudicado"] == 690000000.0
        assert r["proveedor_razon"] == "GRUB S. A."
        assert r["proveedor_cuit"] == "30-71464746-2"

    def test_solo_tabla_de_proveedor_es_sin_monto(self):
        r = sc.parsear_detalle(HTML_SOLO_PROVEEDOR)
        assert r["monto_adjudicado"] is None
        assert r["proveedor_razon"] == "MSLI Latam, Inc."
        assert r["proveedor_cuit"] == "30-12345678-9"
        assert sc.motivo_detalle(r) == sc.SIN_MONTO

    def test_contractual_sin_monto(self):
        r = sc.parsear_detalle(HTML_CONTRACTUAL_SIN_MONTO)
        assert r["monto_adjudicado"] is None
        assert sc.motivo_detalle(r) == sc.SIN_MONTO

    def test_pagina_sin_tabla(self):
        r = sc.parsear_detalle(HTML_SIN_TABLA)
        assert r["tabla"] is None
        assert sc.motivo_detalle(r) == sc.SIN_TABLA

    def test_detalle_no_descargado_es_error_servidor(self):
        assert sc.motivo_detalle(None) == sc.ERROR_SERVIDOR

    @pytest.mark.parametrize("texto,esperado", [
        ("14.400.000,00", 14400000.0),
        ("$ 1.234,56", 1234.56),
        ("768787.56", 768787.56),
        ("1.000.000", 1000000.0),
        ("0,50", None),
        ("", None),
    ])
    def test_limpiar_monto(self, texto, esperado):
        assert sc.limpiar_monto(texto) == esperado

    def test_limpiar_cuit(self):
        assert sc.limpiar_cuit("33663419949") == "33-66341994-9"
        assert sc.limpiar_cuit("30-71464746-2") == "30-71464746-2"
        assert sc.limpiar_cuit("123") is None


# ── Búsqueda por número ──────────────────────────────────────────────────────

class TestBusqueda:
    VS = {"__VIEWSTATE": "vs", "__VIEWSTATEGENERATOR": "AD3B4564",
          "__EVENTVALIDATION": "", "DXScript": "1_103"}

    def test_payload_sin_fechas_y_sin_eventvalidation_vacio(self):
        p = sc.payload_base(dict(self.VS), "79-0001-CDI24", fechas=False)
        assert p["ctl00$CPH1$txtNumeroProceso"] == "79-0001-CDI24"
        assert p["ctl00_CPH1_devDteEdtFechaAperturaDesde_Raw"] == "N"
        assert p["ctl00$CPH1$devDteEdtFechaAperturaDesde"] == ""
        assert p["ctl00$CPH1$devDteEdtFechaAperturaHasta$DDD$C"] == ""
        assert "__EVENTVALIDATION" not in p

    def test_payload_con_eventvalidation_si_vino(self):
        vs = dict(self.VS, __EVENTVALIDATION="ev")
        assert sc.payload_base(vs, "23-0005-LPU26")["__EVENTVALIDATION"] == "ev"

    def test_saf_por_prefijo(self):
        assert sc.saf_para_nup("79-0001-CDI24") == "591"
        assert sc.saf_para_nup("23-0005-LPU26") == "588"
        assert sc.saf_para_nup("999-1") == sc.SAF_ID

    def test_error_async(self):
        assert sc.es_error_async("0|error|500||")
        assert not sc.es_error_async(DELTA_GRILLA)

    def test_encuentra_ctl_del_nup_en_la_grilla(self):
        panel = sc.extraer_panel(DELTA_GRILLA, "ctl00_CPH1_UpdatePanel1")
        assert sc.encontrar_ctl_para_nup(panel, "79-0001-CDI24") == "ctl03"
        assert sc.encontrar_ctl_para_nup(panel, "79-9999-XXX24") is None
        vs = {}
        sc.actualizar_viewstate(DELTA_GRILLA, vs)
        assert vs["__VIEWSTATE"] == "NUEVOVS"

    def test_extraer_url_vista_previa(self):
        txt = 'x <a href="/PLIEGO/VistaPreviaPliegoCiudadano.aspx?qs=AbC%7Cd==">y</a>'
        assert sc.extraer_url_vista_previa(txt) == (
            "https://comprar.gob.ar/PLIEGO/VistaPreviaPliegoCiudadano.aspx?qs=AbC%7Cd==")
        assert sc.extraer_url_vista_previa("nada") is None

    def test_obtener_url_detalle_con_red_simulada(self, monkeypatch):
        monkeypatch.setattr(sc.time, "sleep", lambda s: None)

        class Resp:
            def __init__(self, text, url=sc.BASE_URL, status=200):
                self.text, self.url, self.status_code = text, url, status

        class Sesion:
            def __init__(self):
                self.posts = []

            def post(self, url, data=None, **kw):
                self.posts.append(data)
                if data["__EVENTTARGET"].endswith("btnListarPliegoNumero"):
                    return Resp(DELTA_GRILLA)
                return Resp("", url="https://comprar.gob.ar/PLIEGO/VistaPreviaPliegoCiudadano.aspx?qs=X")

        s = Sesion()
        url, motivo = sc.obtener_url_detalle(s, dict(self.VS), "79-0001-CDI24")
        assert motivo is None and url.endswith("qs=X")
        assert s.posts[1]["__EVENTTARGET"] == "ctl00$CPH1$GridListaPliegos$ctl03$lnkNumeroProceso"
        assert all(p["ctl00$CPH1$devDteEdtFechaAperturaDesde"] == "" for p in s.posts)

    def test_obtener_url_detalle_error_servidor(self, monkeypatch):
        monkeypatch.setattr(sc.time, "sleep", lambda s: None)

        class Sesion:
            def post(self, *a, **kw):
                return type("R", (), {"text": "0|error|500||", "status_code": 200, "url": sc.BASE_URL})()

        assert sc.obtener_url_detalle(Sesion(), dict(self.VS), "79-0001-CDI24") == (None, sc.ERROR_SERVIDOR)


# ── Merge: nunca borrar datos buenos ─────────────────────────────────────────

def _fila(nup, monto="", razon="", cuit="", moneda="", fecha="2026-10-01", **extra):
    f = {"numero_proceso": nup, "nombre_proceso": "Servicio X", "estado": "Adjudicado",
         "monto_adjudicado": monto, "proveedor_razon": razon, "proveedor_cuit": cuit,
         "moneda": moneda, "fecha_perfeccionamiento": "", "detalle_scrapeado": "True",
         "detalle_fecha": fecha}
    f.update(extra)
    return f


class TestFusionarFila:
    def test_fallo_conserva_proveedor_y_monto(self):
        previa = _fila("79-0001-CDI24", "14400000.00", "EJES SA", "33-66341994-9")
        nueva = _fila("79-0001-CDI24", fecha="2026-10-18")
        out = sc.fusionar_fila(previa, nueva)
        assert out["proveedor_razon"] == "EJES SA"
        assert out["monto_adjudicado"] == "14400000.00"
        assert out["proveedor_cuit"] == "33-66341994-9"
        assert out["detalle_fecha"] == "2026-10-18"

    def test_fallo_conserva_proveedor_sin_monto(self):
        previa = _fila("23-0001-CDI25", razon="MSLI Latam, Inc.")
        out = sc.fusionar_fila(previa, _fila("23-0001-CDI25", razon="OTRO SA", fecha="2026-10-18"))
        assert out["proveedor_razon"] == "MSLI Latam, Inc."
        assert out["detalle_fecha"] == "2026-10-18"

    def test_exito_reemplaza(self):
        previa = _fila("79-0001-CDI24", "14400000.00", "EJES SA")
        nueva = _fila("79-0001-CDI24", "14400000.00", "EJES SA", "33-66341994-9",
                      moneda="Peso Argentino", fecha="2026-10-18")
        out = sc.fusionar_fila(previa, nueva)
        assert out["moneda"] == "Peso Argentino" and out["detalle_fecha"] == "2026-10-18"

    def test_previa_vacia_toma_lo_nuevo(self):
        previa = _fila("79-0002-CDI25")
        out = sc.fusionar_fila(previa, _fila("79-0002-CDI25", razon="ACME SA", fecha="2026-10-18"))
        assert out["proveedor_razon"] == "ACME SA"

    def test_sin_previa(self):
        nueva = _fila("79-0003-CDI25")
        assert sc.fusionar_fila(None, nueva) == nueva


class TestProcesarDesdeCsv:
    """End-to-end del modo CSV con la red simulada."""

    @pytest.fixture
    def entorno(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sc.time, "sleep", lambda s: None)
        monkeypatch.setattr(sc, "obtener_viewstate", lambda s: {
            "__VIEWSTATE": "vs", "__VIEWSTATEGENERATOR": "g", "__EVENTVALIDATION": "", "DXScript": ""})

        area = tmp_path / "contratos_jgm.csv"
        with open(area, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["numero_proceso", "nombre_proceso", "estado", "unidad_ejecutora"])
            w.writeheader()
            for nup in ["79-0001-CDI24", "79-0002-CDI25", "79-0003-CDI25", "79-0004-CDI25", "79-0005-CDI25"]:
                w.writerow({"numero_proceso": nup, "nombre_proceso": "Servicio", "estado": "Adjudicado",
                            "unidad_ejecutora": "79/000"})

        out = tmp_path / "contratos_detalle.csv"
        previas = [
            # Con monto pero sin moneda → se re-scrapea; el nuevo intento falla.
            _fila("79-0001-CDI24", "14400000.00", "EJES SA", "33-66341994-9", fecha="2026-09-01"),
            # Sólo proveedor, intento viejo → se reintenta; la página no tiene tabla.
            _fila("79-0002-CDI25", razon="MSLI Latam, Inc.", fecha="2026-09-01"),
        ]
        cols = ["numero_proceso", "nombre_proceso", "tipo_proceso", "estado", "unidad_ejecutora",
                "organismo", "monto_adjudicado", "proveedor_razon", "proveedor_cuit", "moneda",
                "fecha_perfeccionamiento", "detalle_scrapeado", "detalle_fecha"]
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for p in previas:
                w.writerow(p)

        urls = {
            "79-0001-CDI24": (None, sc.ERROR_SERVIDOR),
            "79-0002-CDI25": ("u2", None),
            "79-0003-CDI25": ("u3", None),
            "79-0004-CDI25": (None, sc.SIN_URL),
            "79-0005-CDI25": ("u5", None),
        }
        paginas = {"u2": HTML_SIN_TABLA, "u3": HTML_CONTRACTUAL, "u5": HTML_SOLO_PROVEEDOR}
        monkeypatch.setattr(sc, "obtener_url_detalle", lambda s, vs, nup: urls[nup])
        monkeypatch.setattr(sc, "obtener_detalle", lambda s, url: sc.parsear_detalle(paginas[url]))
        return area, out

    def _leer(self, path):
        with open(path, encoding="utf-8-sig") as f:
            return {r["numero_proceso"]: r for r in csv.DictReader(f)}

    def test_fallo_no_borra_datos_previos(self, entorno):
        area, out = entorno
        sc.procesar_desde_csv([str(area)], out_csv=str(out))
        filas = self._leer(out)
        hoy = sc.datetime.now().strftime("%Y-%m-%d")

        f1 = filas["79-0001-CDI24"]
        assert (f1["proveedor_razon"], f1["monto_adjudicado"], f1["proveedor_cuit"]) == (
            "EJES SA", "14400000.00", "33-66341994-9")
        assert f1["detalle_fecha"] == hoy

        f2 = filas["79-0002-CDI25"]
        assert f2["proveedor_razon"] == "MSLI Latam, Inc." and f2["detalle_fecha"] == hoy

    def test_nuevos_datos_se_guardan(self, entorno):
        area, out = entorno
        sc.procesar_desde_csv([str(area)], out_csv=str(out))
        filas = self._leer(out)
        assert filas["79-0003-CDI25"]["monto_adjudicado"] == "14400000.00"
        assert filas["79-0003-CDI25"]["moneda"] == "Peso Argentino"
        assert filas["79-0005-CDI25"]["proveedor_razon"] == "MSLI Latam, Inc."
        assert filas["79-0004-CDI25"]["proveedor_razon"] == ""

    def test_motivos_en_el_log(self, entorno, caplog):
        area, out = entorno
        with caplog.at_level(logging.INFO, logger=sc.log.name):
            res = sc.procesar_desde_csv([str(area)], out_csv=str(out))
        assert res == {"ok": 1, "sin": 4, "motivos": {
            sc.ERROR_SERVIDOR: 1, sc.SIN_TABLA: 1, sc.SIN_URL: 1, sc.SIN_MONTO: 1}}
        texto = caplog.text
        assert "79-0001-CDI24: error_servidor (se conservan datos previos)" in texto
        assert "79-0002-CDI25: sin_tabla" in texto
        assert "79-0004-CDI25: sin_url" in texto
        assert "79-0005-CDI25: sin_monto" in texto

    def test_dias_reintento(self, entorno, monkeypatch):
        area, out = entorno
        hoy = sc.datetime.now().strftime("%Y-%m-%d")
        sc.procesar_desde_csv([str(area)], out_csv=str(out))  # todos quedan con fecha de hoy
        llamados = []
        monkeypatch.setattr(sc, "obtener_url_detalle",
                            lambda s, vs, nup: llamados.append(nup) or (None, sc.SIN_URL))
        sc.procesar_desde_csv([str(area)], out_csv=str(out))
        assert llamados == []  # 14 días por defecto: nada que reintentar
        sc.procesar_desde_csv([str(area)], out_csv=str(out), dias_reintento=0)
        assert "79-0004-CDI25" in llamados and "79-0003-CDI25" not in llamados  # el de monto no
        assert self._leer(out)["79-0001-CDI24"]["detalle_fecha"] == hoy


def test_sin_conexion_sale_con_error(tmp_path, monkeypatch):
    """Sin ViewState (ConnectTimeout) el modo CSV sale con código != 0 para
    que el bucle de reintentos del workflow funcione."""
    area = tmp_path / "contratos_jgm.csv"
    area.write_text("numero_proceso,estado\n79-0001-CDI24,Adjudicado\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sc, "obtener_viewstate", lambda s: None)
    monkeypatch.setattr(sys, "argv", ["scraper", "--csv", str(area)])
    with pytest.raises(SystemExit) as e:
        sc.main()
    assert e.value.code == 1
