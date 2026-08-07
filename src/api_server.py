"""
api_server.py (DEPRECATED — no usar)
=====================================
Este archivo es una versión vieja de la API (consultas directas a PostgreSQL
vía /api/v1/...) que quedó con un error de sintaxis y desincronizada del
resto del proyecto. Además, `httpx` y `sqlalchemy` no siempre están
disponibles en el mismo entorno donde se ejecuta el server real.

La API que realmente se despliega en producción (Railway/Docker) es
`src/api/api_server.py` — ver `Dockerfile` (`uvicorn src.api.api_server:app`)
y el `README.md`.

Este módulo se deja como un shim que reexporta la app real, para no romper
imports o paths viejos que pudieran referenciar `src.api_server` (por
ejemplo, algún paso de debug en `.github/workflows/tests_diarios.yml`).
No duplica lógica: no lo edites, editá `src/api/api_server.py`.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.api_server import app  # noqa: F401,E402
