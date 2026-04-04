"""
inject_json.py
==============
Inyecta data/inteligencia.json en el dashboard HTML para uso local file://.
Genera src/frontend/dashboard_local.html sin modificar el original.

Uso:
    python inject_json.py
    # Abrir: src/frontend/dashboard_local.html
"""
import json, os, re
from datetime import datetime

DATA = "data/inteligencia.json"
SRC  = "src/frontend/dashboard.html"
OUT  = "src/frontend/dashboard_local.html"

if not os.path.exists(DATA):
    print(f"[ERROR] {DATA} no existe. Correr pipeline.py primero.")
    raise SystemExit(1)

with open(DATA, "r", encoding="utf-8") as f:
    data = json.load(f)

with open(SRC, "r", encoding="utf-8") as f:
    html = f.read()

inject = f"\n<script>window.__JGM_DATA__ = {json.dumps(data, ensure_ascii=False, separators=(',',':'))};</script>\n"
html = html.replace("</head>", inject + "</head>", 1)

with open(OUT, "w", encoding="utf-8") as f:
    f.write(html)

n = len(data.get("alertas", []))
print(f"[OK] {OUT} generado ({n} alertas)")
print(f"     Abrir: file:///{os.path.abspath(OUT).replace(os.sep, '/')}")
