"""
tools/generar_manual.py
=======================
Genera el manual en .docx y en HTML a partir de una sola fuente por idioma:

    docs/manual/manual_es.md  ─┬─►  src/frontend/data/manual/Manual_Monitor_PEN_ES.docx
    docs/manual/manual_en.md  ─┼─►  src/frontend/data/manual/Manual_Monitor_PEN_EN.docx
                               └─►  src/frontend/manual_usuario.html  (pestañas ES/EN)

Así la web y los Word no vuelven a desfasarse: se edita el .md y se regenera.
Adaptado de tools/generar_manual.py de Viny2030/gob_bo_comprar_tgn.

Requiere python-docx:
    pip install python-docx
    python tools/generar_manual.py

Admite el subconjunto de Markdown que usan los manuales: # ## ###, párrafos,
listas (- y 1.), tablas, bloques ``` y citas (>), con **negrita**, `código` y
enlaces [texto](url).
"""
import html
import os
import re
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FUENTES = {"es": os.path.join(RAIZ, "docs", "manual", "manual_es.md"),
           "en": os.path.join(RAIZ, "docs", "manual", "manual_en.md")}
DIR_DESCARGAS = os.path.join(RAIZ, "src", "frontend", "data", "manual")
DOCX = {"es": os.path.join(DIR_DESCARGAS, "Manual_Monitor_PEN_ES.docx"),
        "en": os.path.join(DIR_DESCARGAS, "Manual_Monitor_PEN_EN.docx")}
HTML_SALIDA = os.path.join(RAIZ, "src", "frontend", "manual_usuario.html")
# Foto del autor: se sirve desde /data (mount de api_server.py)
FOTO_AUTOR = "data/manual/autor.jpg"


# ── Parser ────────────────────────────────────────────────────────────────────
def parsear(texto: str) -> list:
    """Devuelve bloques: (tipo, contenido). Tipos: h1 h2 h3 p ul ol table code quote."""
    bloques, lineas, i = [], texto.splitlines(), 0
    while i < len(lineas):
        l = lineas[i]
        if not l.strip():
            i += 1
            continue
        if l.startswith("```"):
            j = i + 1
            while j < len(lineas) and not lineas[j].startswith("```"):
                j += 1
            bloques.append(("code", "\n".join(lineas[i + 1:j])))
            i = j + 1
            continue
        m = re.match(r"^(#{1,3}) (.*)", l)
        if m:
            bloques.append((f"h{len(m.group(1))}", m.group(2).strip()))
            i += 1
            continue
        if l.startswith("|"):
            filas = []
            while i < len(lineas) and lineas[i].startswith("|"):
                celdas = [c.strip() for c in lineas[i].strip().strip("|").split("|")]
                if not all(re.fullmatch(r":?-{3,}:?", c) for c in celdas):
                    filas.append(celdas)
                i += 1
            bloques.append(("table", filas))
            continue
        for tipo, patron in (("ul", r"^- (.*)"), ("ol", r"^\d+\. (.*)"), ("quote", r"^> ?(.*)")):
            if re.match(patron, l):
                items = []
                while i < len(lineas) and re.match(patron, lineas[i]):
                    items.append(re.match(patron, lineas[i]).group(1))
                    i += 1
                bloques.append((tipo, items if tipo != "quote" else " ".join(items)))
                break
        else:
            parrafo = []
            while i < len(lineas) and lineas[i].strip() and not re.match(r"^(#|\||- |\d+\. |> |```)", lineas[i]):
                parrafo.append(lineas[i].strip())
                i += 1
            bloques.append(("p", " ".join(parrafo)))
    return bloques


_INLINE = re.compile(r"(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\([^)]+\))")


def tokens(texto: str) -> list:
    """Divide en (tipo, texto[, url]) con tipo: txt, b, code, link."""
    out = []
    for parte in _INLINE.split(texto):
        if not parte:
            continue
        if parte.startswith("**"):
            out.append(("b", parte[2:-2]))
        elif parte.startswith("`"):
            out.append(("code", parte[1:-1]))
        elif parte.startswith("["):
            m = re.match(r"\[([^\]]+)\]\(([^)]+)\)", parte)
            out.append(("link", m.group(1), m.group(2)))
        else:
            out.append(("txt", parte))
    return out


# ── HTML ──────────────────────────────────────────────────────────────────────
def inline_html(texto: str) -> str:
    out = []
    for t in tokens(texto):
        if t[0] == "b":
            out.append(f"<strong>{html.escape(t[1])}</strong>")
        elif t[0] == "code":
            out.append(f'<code class="inline">{html.escape(t[1])}</code>')
        elif t[0] == "link":
            out.append(f'<a href="{html.escape(t[2])}" target="_blank">{html.escape(t[1])}</a>')
        else:
            esc = html.escape(t[1])
            out.append(re.sub(r"(https?://[^\s<]+)", r'<a href="\1" target="_blank">\1</a>', esc))
    return "".join(out)


def bloques_html(bloques: list, lang: str) -> tuple:
    """Devuelve (título, toc_html, cuerpo_html)."""
    titulo, toc, cuerpo, n, abierta = "", [], [], 0, False
    for tipo, c in bloques:
        if tipo == "h1":
            titulo = c
            continue
        if tipo == "h2":
            if abierta:
                cuerpo.append("</section>")
            n += 1
            ancla = f"{lang}-{n}"
            corto = re.sub(r"^\d+\.\s*", "", c).split(" — ")[0].split(":")[0]
            toc.append(f'<a href="#{ancla}">{n}. {html.escape(corto)}</a>')
            cuerpo.append(f'<section class="doc-section" id="{ancla}"><h2 class="manual-h2">{inline_html(c)}</h2>')
            if re.search(r"Autor|Author", c):
                cuerpo.append(f'<img class="autor-foto" src="{FOTO_AUTOR}" alt="Vicente Humberto Monteverde">')
            abierta = True
        elif not abierta:
            continue  # lo previo a la primera sección va en la portada (generar_html)
        elif tipo == "h3":
            cuerpo.append(f'<h3 class="sec-h3">{inline_html(c)}</h3>')
        elif tipo == "p":
            cuerpo.append(f'<p class="manual-p">{inline_html(c)}</p>')
        elif tipo in ("ul", "ol"):
            items = "".join(f"<li>{inline_html(x)}</li>" for x in c)
            cuerpo.append(f'<{tipo} class="manual-list">{items}</{tipo}>')
        elif tipo == "quote":
            # Citas: la de la sección 1 es informativa, el aviso legal no lleva negrita inicial, el resto advierte
            clase = "legal" if not c.startswith("**") else ("info" if n == 1 else "warn")
            cuerpo.append(f'<div class="callout {clase}"><p>{inline_html(c)}</p></div>')
        elif tipo == "code":
            cuerpo.append(f'<pre class="codeblock">{html.escape(c)}</pre>')
        elif tipo == "table":
            cab = "".join(f"<th>{inline_html(x)}</th>" for x in c[0])
            filas = "".join("<tr>" + "".join(f"<td>{inline_html(x)}</td>" for x in f) + "</tr>" for f in c[1:])
            cuerpo.append(f'<div class="tabla-wrap"><table class="manual-table"><thead><tr>{cab}</tr></thead>'
                          f"<tbody>{filas}</tbody></table></div>")
    if abierta:
        cuerpo.append("</section>")
    return titulo, "".join(toc), "\n".join(cuerpo)


PLANTILLA = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Manual de Usuario / User Manual · Monitor de Transparencia PEN</title>
<!-- Generado por tools/generar_manual.py a partir de docs/manual/*.md: no editar a mano. -->
<style>
:root {{
  --bg: #f4f6fb; --surface: #ffffff; --border: #e2e8f0; --text: #1e293b;
  --muted: #64748b; --accent: #2563eb; --alta: #dc2626; --media: #d97706; --baja: #059669;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: var(--bg); color: var(--text); font-family: system-ui, -apple-system, sans-serif; font-size: 14px; }}
header {{ background: #1e40af; padding: 16px 40px; }}
header h1 {{ font-size: 15px; font-weight: 800; color: #fff; }}
header .sub {{ font-size: 11px; color: #bfdbfe; margin-top: 2px; }}
nav {{ background: var(--surface); border-bottom: 1px solid var(--border); padding: 0 40px;
  display: flex; gap: 2px; align-items: center; flex-wrap: wrap; }}
nav a {{ color: var(--muted); text-decoration: none; padding: 11px 14px; font-size: 13px;
  border-bottom: 2px solid transparent; font-weight: 500; }}
nav a:hover {{ color: var(--text); }}
nav a.active {{ color: var(--accent); border-bottom-color: var(--accent); }}
.barra {{ max-width: 1100px; margin: 24px auto 0; padding: 0 28px; display: flex;
  justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }}
.lang-tabs {{ display: flex; gap: 4px; }}
.lang-tab-btn {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
  padding: 8px 16px; font-size: 13px; font-weight: 600; color: var(--muted); cursor: pointer; }}
.lang-tab-btn.active {{ color: #fff; background: var(--accent); border-color: var(--accent); }}
.descargas a {{ font-size: 12px; color: var(--accent); text-decoration: none; border: 1px solid var(--border);
  background: var(--surface); border-radius: 8px; padding: 7px 12px; margin-left: 6px; }}
.lang-content {{ display: none; }}
.lang-content.active {{ display: grid; }}
.layout {{ grid-template-columns: 240px minmax(0, 1fr); max-width: 1100px; margin: 20px auto 40px; gap: 28px; padding: 0 28px; }}
.toc {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 18px;
  height: fit-content; position: sticky; top: 20px; }}
.toc h2 {{ font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); margin-bottom: 12px; }}
.toc a {{ display: block; color: var(--text); text-decoration: none; font-size: 13px; padding: 6px 8px; border-radius: 6px; }}
.toc a:hover {{ background: #eff6ff; color: var(--accent); }}
.contenido {{ min-width: 0; }}
.portada {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 22px 26px; margin-bottom: 18px; }}
.portada h1 {{ font-size: 20px; margin-bottom: 8px; }}
.portada p {{ color: var(--muted); line-height: 1.6; margin-top: 6px; }}
.doc-section {{ background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
  padding: 22px 26px; margin-bottom: 18px; scroll-margin-top: 16px; }}
.manual-h2 {{ font-size: 17px; margin-bottom: 12px; }}
.sec-h3 {{ font-size: 14px; margin: 18px 0 8px; }}
.manual-p {{ line-height: 1.7; margin-bottom: 10px; }}
.manual-list {{ padding-left: 22px; line-height: 1.75; margin-bottom: 10px; }}
.tabla-wrap {{ overflow-x: auto; margin: 10px 0 14px; }}
.manual-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
.manual-table th {{ text-align: left; padding: 8px; background: #f8fafc; border-bottom: 2px solid var(--border); }}
.manual-table td {{ padding: 8px; border-bottom: 1px solid var(--border); vertical-align: top; line-height: 1.5; }}
.callout {{ border-radius: 10px; padding: 14px 16px; margin: 14px 0; line-height: 1.65; }}
.callout.info {{ background: #eff6ff; border-left: 3px solid var(--accent); }}
.callout.warn {{ background: #fffbeb; border-left: 3px solid var(--media); }}
.callout.legal {{ background: #fef2f2; border-left: 3px solid var(--alta); }}
code.inline {{ font-family: ui-monospace, Consolas, monospace; background: #f1f5f9; padding: 1px 5px; border-radius: 4px; font-size: .9em; overflow-wrap: anywhere; }}
pre.codeblock {{ background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 12px 14px; overflow-x: auto;
  font-family: ui-monospace, Consolas, monospace; font-size: 12.5px; margin: 10px 0 14px; line-height: 1.6; }}
.doc-section a {{ color: var(--accent); word-break: break-word; }}
.autor-foto {{ width: 96px; height: 96px; border-radius: 50%; object-fit: cover; float: right; margin: 0 0 10px 16px; }}
footer {{ text-align: center; color: var(--muted); font-size: 12px; padding: 0 16px 32px; }}
footer a {{ color: var(--muted); }}
@media (max-width: 768px) {{
  .layout {{ grid-template-columns: minmax(0, 1fr); padding: 0 16px; }}
  header, nav, .barra {{ padding-left: 16px; padding-right: 16px; }}
  .toc {{ position: static; }}
  .doc-section, .portada {{ padding: 18px 16px; }}
}}
</style>
</head>
<body>
<header>
  <h1>Monitor de Transparencia · Poder Ejecutivo Nacional</h1>
  <div class="sub">Manual de usuario y de operación · User and operations manual</div>
</header>
<nav>
  <a href="index.html">🏠 Inicio</a>
  <a href="jgm.html">JGM</a>
  <a href="sgp.html">SGP</a>
  <a href="presidencia.html">Presidencia</a>
  <a href="alertas.html">🔔 Alertas</a>
  <a href="grafos_nodos.html">🕸 Grafo</a>
  <a href="documentacion.html">Documentación</a>
  <a href="manual_usuario.html" class="active">📖 Manual</a>
</nav>
<div class="barra">
  <div class="lang-tabs">
    <button class="lang-tab-btn active" data-lang="es" onclick="switchLang('es')">🇦🇷 Español</button>
    <button class="lang-tab-btn" data-lang="en" onclick="switchLang('en')">🇬🇧 English</button>
  </div>
  <div class="descargas">
    <a href="data/manual/Manual_Monitor_PEN_ES.docx">📥 Word ES</a>
    <a href="data/manual/Manual_Monitor_PEN_EN.docx">📥 Word EN</a>
  </div>
</div>
{secciones}
<footer>Monitor de Transparencia PEN · <a href="https://github.com/Viny2030/jefatura_gabinete">github.com/Viny2030/jefatura_gabinete</a> · No afiliado al gobierno argentino</footer>
<script>
function switchLang(lang) {{
  document.querySelectorAll('.lang-tab-btn').forEach(b => b.classList.toggle('active', b.dataset.lang === lang));
  document.querySelectorAll('.lang-content').forEach(c => c.classList.toggle('active', c.id === 'lang-' + lang));
  document.documentElement.lang = lang;
}}
// #en, #en-3, etc. abren la versión en inglés (y van a la sección)
if (location.hash.startsWith('#en')) {{
  switchLang('en');
  window.addEventListener('load', () => {{
    const destino = document.getElementById(location.hash.slice(1));
    if (destino) destino.scrollIntoView();
  }});
}}
</script>
</body>
</html>
"""

TITULO_TOC = {"es": "Contenido", "en": "Contents"}


def generar_html(por_idioma: dict) -> str:
    partes = []
    for lang, bloques in por_idioma.items():
        portada = []
        for tipo, c in bloques:
            if tipo == "h2":
                break
            if tipo == "p":
                portada.append(f"<p>{inline_html(c)}</p>")
        titulo, toc, cuerpo = bloques_html(bloques, lang)
        activo = " active" if lang == "es" else ""
        partes.append(f'<div id="lang-{lang}" class="lang-content layout{activo}" lang="{lang}">\n'
                      f'<aside class="toc"><h2>{TITULO_TOC[lang]}</h2>{toc}</aside>\n'
                      f'<div class="contenido">\n'
                      f'<div class="portada"><h1>{html.escape(titulo)}</h1>{"".join(portada)}</div>\n'
                      f'{cuerpo}\n</div>\n</div>')
    return PLANTILLA.format(secciones="\n".join(partes))


# ── DOCX ──────────────────────────────────────────────────────────────────────
def generar_docx(bloques: list, ruta: str):
    from docx import Document
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Pt, RGBColor

    doc = Document()
    estilo = doc.styles["Normal"]
    estilo.font.name = "Calibri"
    estilo.font.size = Pt(10.5)

    def runs(parrafo, texto):
        for t in tokens(texto):
            if t[0] == "link":
                r = parrafo.add_run(f"{t[1]} ({t[2]})")
            else:
                r = parrafo.add_run(t[1])
            if t[0] == "b":
                r.bold = True
            if t[0] == "code":
                r.font.name = "Consolas"
                r.font.size = Pt(9.5)

    def sombrear(celda, color):
        tc = celda._tc.get_or_add_tcPr()
        shd = OxmlElement("w:shd")
        shd.set(qn("w:val"), "clear")
        shd.set(qn("w:color"), "auto")
        shd.set(qn("w:fill"), color)
        tc.append(shd)

    primero_h2 = False
    for tipo, c in bloques:
        if tipo == "h1":
            p = doc.add_heading(c, level=0)
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        elif tipo == "h2":
            primero_h2 = True
            doc.add_heading(c, level=1)
        elif tipo == "h3":
            doc.add_heading(c, level=2)
        elif tipo == "p":
            p = doc.add_paragraph()
            runs(p, c)
            if not primero_h2:
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        elif tipo in ("ul", "ol"):
            for x in c:
                runs(doc.add_paragraph(style="List Bullet" if tipo == "ul" else "List Number"), x)
        elif tipo == "quote":
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Pt(18)
            runs(p, c)
            for r in p.runs:
                r.font.color.rgb = RGBColor(0x33, 0x41, 0x55)
        elif tipo == "code":
            for linea in c.splitlines():
                r = doc.add_paragraph().add_run(linea)
                r.font.name = "Consolas"
                r.font.size = Pt(9)
        elif tipo == "table":
            t = doc.add_table(rows=len(c), cols=len(c[0]))
            t.style = "Table Grid"
            for i, fila in enumerate(c):
                for j, valor in enumerate(fila):
                    celda = t.cell(i, j)
                    celda.text = ""
                    runs(celda.paragraphs[0], valor)
                    if i == 0:
                        for r in celda.paragraphs[0].runs:
                            r.bold = True
                        sombrear(celda, "D9E2F3")
            doc.add_paragraph()
    doc.save(ruta)


def main():
    por_idioma = {}
    for lang, ruta in FUENTES.items():
        with open(ruta, encoding="utf-8") as f:
            por_idioma[lang] = parsear(f.read())
        generar_docx(por_idioma[lang], DOCX[lang])
        print(f"✅ {os.path.relpath(DOCX[lang], RAIZ)}")
    with open(HTML_SALIDA, "w", encoding="utf-8", newline="\n") as f:
        f.write(generar_html(por_idioma))
    print(f"✅ {os.path.relpath(HTML_SALIDA, RAIZ)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
