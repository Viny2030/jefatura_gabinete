# Monitor de Transparencia PEN — Manual de Usuario y de Operación

Contratos, nómina de autoridades y alertas de posible conflicto de interés en la Jefatura de Gabinete de Ministros, la Secretaría General de la Presidencia y la Presidencia de la Nación.

Versión del manual: noviembre de 2026 · jefatura.ecosistematransparencia.com.ar

## 1. Qué es el Monitor

El Monitor de Transparencia PEN es un portal web que reúne, en un solo lugar y con actualización automática, los procesos de compra, las autoridades designadas y los cruces entre funcionarios y proveedores de tres organismos del Poder Ejecutivo Nacional:

- **JGM** — Jefatura de Gabinete de Ministros, y los organismos de su jurisdicción (CONICET, CONAE, ENACOM y otros).
- **SGP** — Secretaría General de la Presidencia.
- **Presidencia** — Presidencia de la Nación (Secretaría de Cultura, Legal y Técnica, SIGEN y otros).

Todos los datos provienen de fuentes públicas oficiales. El portal no requiere registro.

> **Importante:** las alertas son indicadores algorítmicos de riesgo, no acusaciones. Una coincidencia de apellido entre un funcionario y un proveedor puede ser casual. El Monitor señala patrones que merecen revisión; no determina responsabilidades.

## 2. Páginas del portal

| Página | Qué muestra |
|---|---|
| Inicio (`index.html`) | Totales generales (cargos monitoreados, procesos de compra, montos adjudicados, alertas) y una tarjeta por organismo. Arriba, el indicador de actualización: fecha de procesamiento, fecha de los contratos y fecha de la nómina. |
| JGM (`jgm.html`) | Ficha de la Jefatura de Gabinete: jefes de gabinete por gestión, contratos y procesos, personal por jerarquía y escalafón, alertas, ejecución presupuestaria (crédito vigente, programas e incisos) y procesos de los organismos de la jurisdicción. |
| SGP (`sgp.html`) | Ficha de la Secretaría General: contratos y adjudicaciones, personal, alertas y decretos de designación. |
| Presidencia (`presidencia.html`) | Ficha de Presidencia: contratos y adjudicaciones, personal, alertas e informes de auditoría interna de la SIGEN (SAF 109). |
| Alertas (`alertas.html`) | Todas las alertas de cruce, con filtros por nivel (alta, media, baja) y por tipo. |
| Grafo (`grafos_nodos.html`) | Red de vínculos entre funcionarios y proveedores, con filtros, leyenda y estadísticas. |
| Documentación (`documentacion.html`) | Documentación técnica del sistema. |
| Manual (`manual_usuario.html`) | Este manual, en español y en inglés. |

Cada ficha de organismo tiene además un **Agente IA**: un chat que responde preguntas sobre los datos de ese organismo (sección 5).

## 3. Cómo usar el portal

1. En **Inicio**, mirá el indicador de actualización. Verde significa que todo está al día; amarillo significa que alguna fuente quedó atrasada (por ejemplo, "Nómina al 2026-07-06").
2. Elegí un organismo y hacé clic en **Ver JGM**, **Ver SGP** o **Ver Presidencia**.
3. En la ficha, recorré los contratos: cada proceso muestra el número de COMPR.AR, el tipo de procedimiento, el estado y, cuando está publicado, el proveedor adjudicado, su CUIT y el monto. Los montos en dólares se informan aparte y no se suman a los montos en pesos.
4. Revisá las **alertas** de la ficha o la página **Alertas**, que reúne las de los tres organismos.
5. Usá el **Grafo** para ver qué funcionarios y proveedores aparecen vinculados.
6. Para una pregunta puntual, consultá al **Agente IA** de la ficha.

## 4. Cómo leer las alertas

Las alertas las genera `scripts/generar_cruces_pen.py` todas las noches, cruzando el personal de cada organismo con los proveedores que tienen CUIT conocido.

| Nivel | Cruce | Cuándo se activa |
|---|---|---|
| Alto | CUIL igual a CUIT | El CUIL de un funcionario es idéntico al CUIT de un proveedor adjudicado: la misma persona es funcionario y proveedor. |
| Medio | Apellido en razón social | El apellido del funcionario aparece en el nombre del proveedor. |
| Medio | Proveedor multi-organismo | Un mismo CUIT tiene contratos en la JGM y en al menos otro organismo del PEN. |

Para reducir falsos positivos, el cruce por apellido:

- Ignora apellidos de menos de 4 letras y una lista de apellidos muy frecuentes en Argentina (González, Rodríguez, Pérez, etc.) o que también son nombres de pila (Martín, Daniel, Lorenzo, etc.).
- Si el proveedor es una persona humana (CUIT 20, 23, 24 o 27), exige que el apellido esté al principio o al final del nombre, no en el medio, donde suele ser un segundo nombre.
- Si el proveedor es una empresa, descarta topónimos y nombres de fantasía ("TORRES DEL PARANÁ S.R.L.").

Cada alerta muestra el funcionario y su cargo, la gestión, el proveedor y su CUIT, el organismo, la cantidad de contratos, el monto y el enlace al aviso. Una alerta media pide revisar el caso, no presume un vínculo.

## 5. Agente IA

Cada ficha tiene un chat que responde con los datos publicados de ese organismo (contratos, montos, estados, personal). Usa un modelo Claude de Anthropic, configurado en el servidor con la variable `ANTHROPIC_MODEL`.

- Hay un límite de mensajes por visitante y por día (variable `CHAT_RATE_LIMIT_DIARIO`, 30 por defecto). Al alcanzarlo, el chat lo avisa y vuelve a funcionar al día siguiente.
- El agente sólo conoce lo que el portal publica. Si un contrato no tiene monto informado, el agente tampoco lo sabe.
- `GET /api/v2/chat/diagnostico` informa qué modelo está activo y la cobertura de datos de cada área.

## 6. Procedimiento de actualización

El portal se actualiza solo con workflows de GitHub Actions. Cada workflow guarda sus resultados en la rama `main` del repositorio `Viny2030/jefatura_gabinete`, y Railway vuelve a desplegar el sitio con cada cambio en `main`. No hay un paso manual de publicación.

### 6.1 Calendario de los procesos automáticos

| Hora UTC (ART) | Workflow | Qué hace | Qué guarda |
|---|---|---|---|
| 04:00 (01:00) todos los días | Scraper Detalle Contratos | Busca en COMPR.AR el monto, el CUIT y la razón social de los procesos adjudicados. Procesa hasta 450 por noche y reintenta a los 14 días los que no tenían monto publicado. | `contratos_detalle.csv` |
| 05:00 (02:00) todos los días | Generador de Cruces PEN | Cruza el personal con los proveedores de los reportes de `monitor_contratos` y `gob_bo_comprar_tgn` y con `contratos_detalle.csv`. | `src/frontend/data/cruces.json` |
| 06:00 (03:00) lunes y jueves | Scraper Contratos JGM | Descarga de COMPR.AR los procesos de JGM, SGP, Presidencia y los organismos de la jurisdicción de la JGM. | `contratos_*.csv` |
| 07:00 (04:00) todos los días | Tests Diarios | Corre los tests unitarios y los smoke tests contra el sitio publicado; manda un mail con el resultado. | — |
| 10:30 (07:30) lunes | Presupuesto y SIGEN | Actualiza el crédito y la ejecución de la JGM (Presupuesto Abierto) y los informes de la SIGEN. | `presupuesto_jgm.json`, `sigen_auditorias.json` |
| Al subir un CSV a `nomina/` | Nómina APN — desde CSV subido | Procesa la nómina bajada a mano (sección 6.3) y regenera los JSON del portal. | `src/frontend/data/*.json` |
| 12:00 (09:00) lunes a viernes | Monitor Diario PEN | Reclasifica los reportes de `monitor_contratos`, intenta bajar la nómina, genera los JSON del portal y recalcula los campos derivados del personal. Manda un mail con el resultado. | `src/frontend/data/*.json` |
| 13:00 (10:00) del día 1 de cada mes | Recordatorio — nómina desactualizada | Si la nómina tiene más de 30 días, manda un mail con los pasos de la sección 6.3. | — |
| Después de Monitor Diario y 13:00 (10:00) | Cobertura de datos | Falla, y GitHub avisa por mail, si un área se queda sin montos o si la cobertura cae más de un 30 % respecto de 24 horas antes. | — |

### 6.2 Repositorios de los que depende

| Repositorio | Qué aporta | Dónde se genera |
|---|---|---|
| `Viny2030/monitor_contratos` (`main`) | Reporte diario BORA + COMPR.AR + TGN (`data/AAAA-MM/reporte_AAAA-MM-DD.xlsx`). Lo usan la reclasificación y los cruces. | Runner propio en una PC en Argentina, porque el BORA no responde a servidores del exterior. Llega alrededor de las 12:10 UTC. |
| `Viny2030/gob_bo_comprar_tgn` | Reporte integrado BORA + COMPR.AR + TGN. Lo usan los cruces. | Runner propio en la misma PC. |

> **Atención:** hasta noviembre de 2026 el portal leía `monitor_contratos_v2`, que corre en GitHub Actions. Desde el 22/10/2026 sus reportes salen vacíos ("Sin Datos"), aunque el workflow termine en verde. No volver a usarlo.

El reporte de `monitor_contratos` llega después de que corre el Monitor Diario PEN (12:00 UTC), así que el reporte de cada día entra en la corrida del día hábil siguiente.

### 6.3 Procedimientos manuales

**Nómina de autoridades (Mapa del Estado).** Mapa del Estado está detrás de un desafío de Cloudflare que exige un navegador, así que ningún script puede descargar la nómina, ni desde GitHub ni desde una PC en Argentina. El paso "Scraper nómina APN" del Monitor Diario falla todos los días por esa razón, y el portal conserva la última nómina publicada. La portada muestra la fecha ("Nómina al ..."). Para actualizarla:

1. Abrir en el navegador https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv y guardar el archivo CSV.
2. En GitHub, entrar a la carpeta `nomina/` del repositorio → **Add file → Upload files** → subir el CSV (por ejemplo `nomina_2026-11-03.csv`) → **Commit changes** en `main`.
3. El workflow **Nómina APN — desde CSV subido** se dispara solo: procesa el CSV más reciente (`scraper_nomina.py --archivo`), regenera los JSON (`generar_json.py` y `corregir_personal.py`) y publica el resultado. En unos minutos la portada muestra la fecha nueva.

Alternativa sin pasar por la web de GitHub: desde la copia local, en la rama `main` y sin cambios pendientes, ejecutar en PowerShell:

```
.\scripts\actualizar_nomina.ps1 -Csv C:\ruta\al\archivo.csv
```

Conviene actualizar la nómina una vez por mes o cuando haya cambios de autoridades. El día 1 de cada mes, si tiene más de 30 días, llega un mail recordándolo.

**Informes SIGEN.** La SIGEN corta las conexiones que no vienen de Argentina, así que el paso SIGEN del workflow semanal puede fallar. Desde una PC en Argentina:

```
.\scripts\actualizar_sigen.ps1
```

**Volver a correr un workflow.** En GitHub: Actions → elegir el workflow → **Run workflow**. Desde la terminal:

```
gh workflow run "Generador de Cruces PEN" -R Viny2030/jefatura_gabinete
```

El Scraper Detalle acepta el parámetro `dias`: con `0` reintenta todos los procesos sin monto, sin esperar los 14 días.

### 6.4 Cómo verificar que el portal está al día

1. **Portada:** el indicador debe mostrar la fecha de procesamiento del último día hábil.
2. **API:** `GET /api/health` debe responder `"status": "ok"`, con la fecha de la última actualización.
3. **GitHub Actions:** los workflows de la sección 6.1 deben terminar en verde. El paso de nómina en rojo es esperable (sección 6.3).
4. **Fuentes:** que los workflows terminen en verde no prueba que haya datos. Si las cifras no cambian durante varios días, abrir el último `reporte_*.xlsx` de `monitor_contratos` y confirmar que tenga las hojas "🚨 Flujo Completo" y "🏆 Adjudicaciones BORA". Desde noviembre de 2026, el log de la reclasificación muestra `⚠️ ... reporte vacío` cuando un reporte no trae adjudicaciones.
5. **Nómina:** `data/nomina_estado.json` indica la fecha de la última descarga real.

### 6.5 Problemas conocidos y solución

| Síntoma | Causa | Solución |
|---|---|---|
| "Nómina al ..." con una fecha vieja | Mapa del Estado exige navegador (Cloudflare). | Procedimiento manual de la sección 6.3. |
| Paso SIGEN en rojo en el workflow semanal | La SIGEN bloquea conexiones desde el exterior. | `scripts/actualizar_sigen.ps1` desde una PC en Argentina. |
| Los cruces no suman adjudicaciones nuevas | El reporte de `monitor_contratos` del día no se generó o salió vacío. | Revisar el runner de la PC (debe figurar "online" en GitHub → Settings → Actions → Runners de ese repositorio) y regenerar el reporte. |
| Mail "Cobertura de datos" fallido | Un área perdió montos o razón social. | Ver el log del workflow: indica qué área y cuánto cayó. Suele ser un 503 de COMPR.AR; se corrige solo en la corrida siguiente del Scraper Detalle. |
| Mail "Railway sin respuesta" | El sitio no respondió a los smoke tests. | Revisar el último despliegue en Railway y las variables de entorno. |
| El chat responde "Límite de mensajes" | Se alcanzó el límite diario de esa IP. | Esperar al día siguiente, o subir `CHAT_RATE_LIMIT_DIARIO` en Railway. |

## 7. Desarrollo y despliegue

### 7.1 Instalación local

```
git clone https://github.com/Viny2030/jefatura_gabinete.git
cd jefatura_gabinete
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.api.api_server:app --reload --port 8000
```

Después abrir http://localhost:8000/. El servidor sirve las páginas de `src/frontend/` y los datos de `src/frontend/data/` en `/data`.

### 7.2 Tests y cambios

- `pytest` corre todos los tests. `tests/test_live.py` necesita conexión, porque prueba el sitio publicado.
- Cada pull request contra `main` corre los tests unitarios en GitHub Actions (workflow "Tests en Pull Requests"). No mergear con los tests en rojo.
- Los datos de `src/frontend/data/` los escriben los workflows: no editarlos a mano, salvo con los procedimientos de la sección 6.3.

### 7.3 Variables de entorno (Railway)

| Variable | Uso |
|---|---|
| `DATABASE_URL` | Base PostgreSQL (la inyecta Railway). |
| `REFRESH_TOKEN` | Token de `/api/refresh` y `/api/init-db`. Sin él, esos endpoints rechazan todo pedido. |
| `ANTHROPIC_API_KEY` | Clave del Agente IA. Sin ella el chat responde 503. |
| `ANTHROPIC_MODEL` | Modelo del Agente IA. |
| `CHAT_RATE_LIMIT_DIARIO` | Mensajes por visitante y por día (30 por defecto). |
| `MEACI_API_URL` | API de sanciones internacionales (MEACI). Por defecto, https://meaci.ecosistematransparencia.com.ar |

### 7.4 Este manual

El manual se escribe en `docs/manual/manual_es.md` y `docs/manual/manual_en.md`. Después de editarlos, ejecutar:

```
python tools/generar_manual.py
```

El script regenera `src/frontend/manual_usuario.html` (con pestañas Español / English) y los Word `src/frontend/data/manual/Manual_Monitor_PEN_ES.docx` y `_EN.docx`. No editar esos archivos a mano.

## 8. API REST

La API es de acceso libre para consultas (GET). La documentación interactiva está en `/docs`.

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/api/health` | Estado del sistema, fecha de la última actualización y cantidad de alertas. |
| GET | `/api/alertas`, `/api/v1/alertas` | Alertas activas. |
| GET | `/api/contratos` | Contratos. |
| GET | `/api/bora` | Avisos del Boletín Oficial. |
| GET | `/api/resumen`, `/api/inteligencia` | Resúmenes agregados. |
| GET | `/api/grafo`, `/api/v1/grafo/nodos` | Nodos y vínculos del grafo. |
| GET | `/api/cruce-cuits-bulk?cuits=...` | Consulta hasta 200 CUIT en el registro de sanciones internacionales MEACI. |
| POST | `/api/v1/chat/{area}` | Agente IA (`area`: jgm, sgp o presidencia). |
| GET | `/api/v2/chat/diagnostico` | Modelo activo y cobertura de datos por área. |

Los archivos de datos también se pueden descargar directamente, por ejemplo `/data/meta.json`, `/data/cruces.json` o `/data/contratos_jgm.json`.

## 9. 🕸️ Relación con los otros sitios del ecosistema

Este sitio es parte del **Ecosistema de Transparencia**: once monitores que se consultan entre sí. Abajo, con qué sitios intercambia datos ("usa →" = toma datos de ese sitio; "← lo usa" = ese sitio toma datos de este) y de qué fuentes oficiales se alimenta. El grafo es interactivo: hacé clic en cualquier sitio para ver su ficha.

[Abrir el grafo completo ↗](https://mapa.ecosistematransparencia.com.ar/grafo?nodo=jefatura)

**Conexiones**

- **usa → MEACI**: Consulta en vivo la lista de CUIT sancionadas de MEACI (caché 6 h) para marcar proveedores.
- **← lo usa Mapa**: Enlace y estado en vivo del sitio.

**Fuentes oficiales**

- Mapa del Estado (nómina)
- COMPR.AR / datos.gob.ar
- Boletín Oficial

Detalle de todas las conexiones y de cómo se verifican las cifras entre sitios: [mapa.ecosistematransparencia.com.ar/manual#conexiones](https://mapa.ecosistematransparencia.com.ar/manual#conexiones)

## 10. Autor y contacto

Ph.D. Vicente Humberto Monteverde — Doctor en Ciencias Económicas, investigador en transparencia pública. Autor de la teoría de la Transferencia Regresiva de Ingresos y desarrollador del enfoque XAI aplicado a las contrataciones públicas. Publicaciones en Journal of Financial Crime (Emerald Publishing).

- vhmonte@retina.ar · viny01958@gmail.com
- Código fuente: https://github.com/Viny2030/jefatura_gabinete

> Herramienta experimental y académica. Los datos provienen de fuentes públicas oficiales del Estado argentino. Los resultados son indicadores algorítmicos de riesgo: no implican juicio de valor, acusación ni determinación de responsabilidad sobre ninguna empresa, organismo o persona. No afiliado al gobierno argentino.
