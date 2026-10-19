"""
server_v2.py — punto de entrada que extiende api_server sin modificarlo
=======================================================================

Importa la app existente (todas sus rutas siguen igual) y le suma el agente
IA v2 (src/api/chat_v2.py).

Arranque (Railway → Settings → Deploy → Custom Start Command):

    uvicorn src.api.server_v2:app --host 0.0.0.0 --port $PORT

  (Railway expande $PORT solo si el comando corre en un shell; si no, usar
   el puerto fijo 8000 como en el Dockerfile:   ... --port 8000)

Variables de entorno nuevas (todas opcionales):

  CHAT_V2_REEMPLAZA_V1=1   → /api/v1/chat/{area} pasa a responder con v2.
                             Así jgm.html / sgp.html / presidencia.html usan
                             el agente nuevo sin tocar el HTML. Con 0 (default)
                             v1 queda intacto y v2 se prueba en /api/v2/chat/*.
  CHAT_V2_MAX_TOKENS       → tope de tokens de respuesta (default 1200).

Volver atrás = quitar el Custom Start Command (o poner la variable en 0).
"""

import os

from fastapi.routing import APIRoute

if __package__:                      # uvicorn src.api.server_v2:app
    from .api_server import app
    from . import chat_v2
else:                                # tests (pythonpath = src/api)
    from api_server import app
    import chat_v2

app.include_router(chat_v2.router)


def _reemplazar_chat_v1() -> bool:
    """Pone una ruta v2 en /api/v1/chat/{area} por delante de la de v1.

    Starlette resuelve la primera ruta que coincide, así que insertar al
    principio alcanza; la función v1 sigue existiendo en api_server.py."""
    if os.getenv("CHAT_V2_REEMPLAZA_V1", "0") != "1":
        return False
    ruta = APIRoute("/api/v1/chat/{area}", chat_v2.chat_v2_endpoint, methods=["POST"],
                    tags=["Agente IA v2"], name="chat_v1_servido_por_v2")
    app.router.routes.insert(0, ruta)
    return True


CHAT_V1_SERVIDO_POR_V2 = _reemplazar_chat_v1()
