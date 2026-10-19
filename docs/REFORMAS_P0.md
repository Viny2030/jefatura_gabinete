# Reformas P0 — Agente IA y cobertura de datos (2026-10-18)

Todo es **aditivo**: no se modificó ningún archivo existente. Lo nuevo convive
con lo actual y se activa por configuración.

## Diagnóstico (medido el 2026-10-18 sobre el sitio en vivo)

"La IA no encuentra empresas" tiene dos causas, una en los datos y otra en el buscador.

### 1. Datos: faltan razón social, CUIT y monto

| Área | Contratos | Con razón social y monto |
|---|---|---|
| JGM | 179 | **0** (había 10 hasta el 2026-10-13) |
| Presidencia | 353 | 8 (había 20) |
| SGP | 263 | 174 |
| JGM ampliada | 1.481 | 235 |

En los contratos sin detalle, `generar_json.py` pone el **título del proceso** en
el campo `proveedor`.

- **Pérdida de datos.** `scraper_detalle_contratos.py --csv` vuelve a scrapear las
  filas sin `moneda`, o sea todas las anteriores al lote 3. Si COMPR.AR no devuelve
  el proceso, guarda una fila vacía encima de la buena (`ya[nup] = fila`). La corrida
  del **2026-10-14** borró así 30 proveedores (EJES SA, MSLI Latam, CHAER SRL,
  C P S Comunicaciones, La Mantovana…).
- **COMPR.AR rechaza la búsqueda.** El postback de `btnListarPliegoNumero`
  responde hoy `0|error|500||`, también para procesos de SGP, así que el scraper
  no encuentra ningún proceso nuevo. Por prefijo: 79 (JGM) 0/178, 83 0/146,
  39 0/221, 28 0/129.

### 2. Buscador del agente v1 (`api_server.py`, `_tool_buscar_*`)

| Falla | Ejemplo en vivo |
|---|---|
| Subcadena literal: "EJES SA" ≠ "EJES S.A." | JGM y Presidencia: "no encontré coincidencias" para EJES SA |
| Solo mira `proveedor` | SGP: "Microsoft" está en el objeto; el proveedor es "MSLI Latam, Inc." |
| Tildes y mayúsculas | "gonzalez" ≠ "GONZÁLEZ" |
| Resultado cortado a 4.000 caracteres | 15 registros ocupan unos 8.200 caracteres: llegan ~7 con el JSON roto, y los "top 5" salen de esa muestra |
| Sin herramientas de agregación | Los rankings y totales no son confiables |
| Ve solo `contratos_{area}.json` | No consulta la jurisdicción ampliada ni el resto del PEN |
| `request.client.host` detrás del proxy de Railway | Probablemente todos los visitantes comparten el mismo límite de 30 mensajes por día |
| Responde en markdown | El chat pinta con `textContent`: las tablas se ven crudas, con `|` y `**` |

## Qué se agregó

| Archivo | Para qué |
|---|---|
| `src/api/chat_v2.py` | Agente v2. Búsqueda normalizada (sin tildes, `S.A.` = `SA`, sufijos opcionales) en proveedor, objeto, CUIT, número de proceso y organismo. Devuelve `total_coincidencias` y nunca corta el JSON. Herramientas nuevas `ranking_proveedores` y `resumen_area`, que informan la cobertura. Alcance `area`, `ampliada` o `todo_pen`. Sugiere nombres ante errores de tipeo. Toma la IP real de `X-Forwarded-For`. Responde en texto plano y cachea el prompt. |
| `src/api/server_v2.py` | Punto de entrada que importa la app actual y le suma `/api/v2/chat/{area}` y `/api/v2/chat/diagnostico`. Con `CHAT_V2_REEMPLAZA_V1=1`, `/api/v1/chat/*` responde con v2 sin tocar el HTML. |
| ~~`scripts/proteger_detalle.py` + `.github/workflows/proteger_detalle.yml`~~ | Restauraba desde el historial de git lo que el scraper borraba. **Retirado el 2026-10-19**: el scraper ya no borra datos (PR #2) y la última pasada restauró 0 procesos. La auditoría de lo recuperado queda en `contratos_detalle_recuperados.csv`. |
| `scripts/verificar_cobertura.py` + `.github/workflows/cobertura_datos.yml` | Pone el job en rojo (y GitHub manda el mail) si un área queda sin montos o si la cobertura cae más de 30 % en 24 h. Contra el estado del 2026-10-11 detecta JGM 10 → 0 y Presidencia 20 → 8. |
| `tests/test_chat_v2.py`, `tests/test_datos_p0.py` | 43 tests nuevos (129 en total, todos pasan). |

## Activación (en orden)

1. Mergear la rama `reformas-p0`.
2. ~~Actions → **Proteger detalle de contratos** → *Run workflow*.~~ Hecho el
   2026-10-18; el workflow se retiró el 2026-10-19.
3. Railway → servicio web → Settings → Deploy → **Custom Start Command**:
   `uvicorn src.api.server_v2:app --host 0.0.0.0 --port 8000`
4. Probar sin afectar a los usuarios:
   - `GET /api/v2/chat/diagnostico`
   - `POST /api/v2/chat/jgm` con `{"mensaje":"contratos de EJES SA"}`
5. Si responde bien: variable `CHAT_V2_REEMPLAZA_V1=1` en Railway. Las tres
   fichas pasan a usar v2.

**Volver atrás:** `CHAT_V2_REEMPLAZA_V1=0`, o borrar el Custom Start Command.

## Pendiente (requiere modificar código existente, con tu OK)

- ~~**Scraper (P0 real):** reparar el postback a COMPR.AR y no reemplazar una fila
  con datos por una vacía.~~ Hecho en los PR #2 y #3 (2026-10-18). Con el reintento
  completo, los contratos con monto pasaron de 459 a 1464 de 1615.
- Rate limit persistente en Postgres (la base ya está conectada).
- Crear la tabla `pipeline_snapshots`: `/api/db-status` informa que no existe.
- Limpieza: `src/api_server.py` duplicado, archivos vacíos (`railway.yml`,
  `analizar_paso1.py`, `.env.example.py`, `patch_jgm_contratos.py`…) y `debug_*.html`
  en la raíz.
