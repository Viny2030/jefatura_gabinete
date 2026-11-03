# PEN Transparency Monitor — User and Operations Manual

Contracts, appointed officials and possible conflict-of-interest alerts for the Chief of Cabinet's Office, the General Secretariat of the Presidency and the Office of the President of Argentina.

Manual version: November 2026 · jefatura.ecosistematransparencia.com.ar

## 1. What the Monitor is

The PEN Transparency Monitor is a website that brings together, in one place and with automatic updates, the procurement processes, the appointed officials and the cross-checks between officials and suppliers of three bodies of Argentina's national executive branch (Poder Ejecutivo Nacional, PEN):

- **JGM** — Chief of Cabinet's Office (Jefatura de Gabinete de Ministros), and the bodies under its jurisdiction (CONICET, CONAE, ENACOM and others).
- **SGP** — General Secretariat of the Presidency (Secretaría General de la Presidencia).
- **Presidency** — Office of the President (Secretariat of Culture, Legal and Technical Secretariat, SIGEN and others).

All data comes from official public sources. The site requires no registration.

> **Important:** alerts are algorithmic risk indicators, not accusations. A surname shared by an official and a supplier may be a coincidence. The Monitor points out patterns worth reviewing; it does not establish responsibility.

## 2. Site pages

| Page | What it shows |
|---|---|
| Home (`index.html`) | Overall totals (positions monitored, procurement processes, awarded amounts, alerts) and one card per body. At the top, the update indicator: processing date, contract date and payroll date. |
| JGM (`jgm.html`) | Chief of Cabinet's Office: chiefs of cabinet by administration, contracts and processes, staff by rank and pay scale, alerts, budget execution (current appropriation, programmes and spending categories) and processes of the bodies under its jurisdiction. |
| SGP (`sgp.html`) | General Secretariat: contracts and awards, staff, alerts and appointment decrees. |
| Presidency (`presidencia.html`) | Office of the President: contracts and awards, staff, alerts and SIGEN internal audit reports (SAF 109). |
| Alerts (`alertas.html`) | Every cross-check alert, filterable by level (high, medium, low) and by type. |
| Graph (`grafos_nodos.html`) | Network of links between officials and suppliers, with filters, legend and statistics. |
| Documentation (`documentacion.html`) | Technical documentation of the system. |
| Manual (`manual_usuario.html`) | This manual, in Spanish and English. |

Each body's page also has an **AI Agent**: a chat that answers questions about that body's data (section 5).

## 3. How to use the site

1. On **Home**, check the update indicator. Green means everything is current; yellow means a source is behind (for example, "Nómina al 2026-07-06", payroll as of that date).
2. Pick a body and click **Ver JGM**, **Ver SGP** or **Ver Presidencia**.
3. On its page, browse the contracts: each process shows its COMPR.AR number, procedure type, status and, when published, the awarded supplier, its CUIT (tax ID) and the amount. Amounts in US dollars are reported separately and are not added to amounts in pesos.
4. Review the **alerts** on the body's page, or the **Alerts** page, which gathers those of all three bodies.
5. Use the **Graph** to see which officials and suppliers appear linked.
6. For a specific question, ask the page's **AI Agent**.

## 4. How to read the alerts

Alerts are generated every night by `scripts/generar_cruces_pen.py`, which cross-checks each body's staff against the suppliers whose CUIT is known.

| Level | Check | When it fires |
|---|---|---|
| High | CUIL equals CUIT | An official's CUIL (personal tax ID) is identical to an awarded supplier's CUIT: the same person is both official and supplier. |
| Medium | Surname in company name | The official's surname appears in the supplier's name. |
| Medium | Multi-body supplier | The same CUIT has contracts with the JGM and with at least one other PEN body. |

To reduce false positives, the surname check:

- Ignores surnames shorter than 4 letters and a list of surnames that are very common in Argentina (González, Rodríguez, Pérez, etc.) or are also first names (Martín, Daniel, Lorenzo, etc.).
- When the supplier is an individual (CUIT starting with 20, 23, 24 or 27), requires the surname to be at the beginning or end of the name, not in the middle, where it is usually a middle name.
- When the supplier is a company, discards place names and trade names ("TORRES DEL PARANÁ S.R.L.").

Each alert shows the official and their position, the administration, the supplier and its CUIT, the body, the number of contracts, the amount and the link to the notice. A medium alert calls for a review; it does not presume a link.

## 5. AI Agent

Each body's page has a chat that answers using that body's published data (contracts, amounts, statuses, staff). It uses an Anthropic Claude model, set on the server with the `ANTHROPIC_MODEL` variable.

- There is a daily message limit per visitor (variable `CHAT_RATE_LIMIT_DIARIO`, 30 by default). Once reached, the chat says so and works again the next day.
- The agent only knows what the site publishes. If a contract has no published amount, the agent does not know it either.
- `GET /api/v2/chat/diagnostico` reports the active model and the data coverage of each area.

## 6. Update procedure

The site updates itself through GitHub Actions workflows. Each workflow saves its results to the `main` branch of the `Viny2030/jefatura_gabinete` repository, and Railway redeploys the site on every change to `main`. There is no manual publishing step.

### 6.1 Schedule of automatic processes

| Time UTC (Argentina) | Workflow | What it does | What it saves |
|---|---|---|---|
| 04:00 (01:00) daily | Scraper Detalle Contratos | Looks up in COMPR.AR the amount, CUIT and company name of awarded processes. Handles up to 450 per night and retries after 14 days those that had no published amount. | `contratos_detalle.csv` |
| 05:00 (02:00) daily | Generador de Cruces PEN | Cross-checks staff against the suppliers in the `monitor_contratos` and `gob_bo_comprar_tgn` reports and in `contratos_detalle.csv`. | `src/frontend/data/cruces.json` |
| 06:00 (03:00) Monday and Thursday | Scraper Contratos JGM | Downloads from COMPR.AR the processes of JGM, SGP, the Presidency and the bodies under the JGM's jurisdiction. | `contratos_*.csv` |
| 07:00 (04:00) daily | Tests Diarios | Runs the unit tests and the smoke tests against the live site; emails the result. | — |
| 10:30 (07:30) Monday | Presupuesto y SIGEN | Updates the JGM's appropriation and execution (Presupuesto Abierto) and the SIGEN reports. | `presupuesto_jgm.json`, `sigen_auditorias.json` |
| When a CSV is uploaded to `nomina/` | Nómina APN — desde CSV subido | Processes the manually downloaded payroll (section 6.3) and regenerates the site's JSON files. | `src/frontend/data/*.json` |
| 12:00 (09:00) Monday to Friday | Monitor Diario PEN | Reclassifies the `monitor_contratos` reports, tries to download the payroll, generates the site's JSON files and recomputes the staff's derived fields. Emails the result. | `src/frontend/data/*.json` |
| 13:00 (10:00) on the 1st of each month | Recordatorio — nómina desactualizada | If the payroll is more than 30 days old, sends an email with the steps in section 6.3. | — |
| After Monitor Diario and at 13:00 (10:00) | Cobertura de datos | Fails, and GitHub sends an email, if an area loses its amounts or coverage drops by more than 30 % compared with 24 hours earlier. | — |

### 6.2 Upstream repositories

| Repository | What it provides | Where it is generated |
|---|---|---|
| `Viny2030/monitor_contratos` (`main`) | Daily BORA + COMPR.AR + TGN report (`data/YYYY-MM/reporte_YYYY-MM-DD.xlsx`). Used by the reclassification and the cross-checks. | Self-hosted runner on a PC in Argentina, because the Official Gazette (BORA) does not answer servers abroad. Arrives around 12:10 UTC. |
| `Viny2030/gob_bo_comprar_tgn` | Integrated BORA + COMPR.AR + TGN report. Used by the cross-checks. | Self-hosted runner on the same PC. |

> **Warning:** until November 2026 the site read `monitor_contratos_v2`, which runs on GitHub Actions. Since 22 October 2026 its reports have been empty ("Sin Datos"), even though its workflow finishes green. Do not use it again.

The `monitor_contratos` report arrives after the Monitor Diario PEN has run (12:00 UTC), so each day's report is picked up by the next business day's run.

### 6.3 Manual procedures

**Payroll of officials (Mapa del Estado).** Mapa del Estado sits behind a Cloudflare challenge that requires a browser, so no script can download the payroll, neither from GitHub nor from a PC in Argentina. The "Scraper nómina APN" step of the Monitor Diario fails every day for that reason, and the site keeps the last published payroll. The home page shows its date ("Nómina al ..."). To update it:

1. Open https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv in a browser and save the CSV file.
2. On GitHub, open the repository's `nomina/` folder → **Add file → Upload files** → upload the CSV (for example `nomina_2026-11-03.csv`) → **Commit changes** to `main`.
3. The **Nómina APN — desde CSV subido** workflow starts on its own: it processes the most recent CSV (`scraper_nomina.py --archivo`), regenerates the JSON files (`generar_json.py` and `corregir_personal.py`) and publishes the result. Within a few minutes the home page shows the new date.

Alternative without the GitHub website: from the local copy, on the `main` branch with no pending changes, run in PowerShell:

```
.\scripts\actualizar_nomina.ps1 -Csv C:\path\to\file.csv
```

Updating the payroll once a month, or whenever officials change, is enough. On the 1st of each month, if it is more than 30 days old, a reminder email arrives.

**SIGEN reports.** SIGEN drops connections that do not come from Argentina, so the SIGEN step of the weekly workflow may fail. From a PC in Argentina:

```
.\scripts\actualizar_sigen.ps1
```

**Re-running a workflow.** On GitHub: Actions → choose the workflow → **Run workflow**. From the terminal:

```
gh workflow run "Generador de Cruces PEN" -R Viny2030/jefatura_gabinete
```

The Scraper Detalle accepts a `dias` parameter: `0` retries every process without an amount, without waiting the 14 days.

### 6.4 How to check that the site is current

1. **Home page:** the indicator should show the processing date of the last business day.
2. **API:** `GET /api/health` should answer `"status": "ok"`, with the date of the last update.
3. **GitHub Actions:** the workflows in section 6.1 should finish green. A red payroll step is expected (section 6.3).
4. **Sources:** green workflows do not prove there is data. If the figures do not change for several days, open the latest `reporte_*.xlsx` of `monitor_contratos` and check that it has the "🚨 Flujo Completo" and "🏆 Adjudicaciones BORA" sheets. Since November 2026, the reclassification log shows `⚠️ ... reporte vacío` when a report has no awards.
5. **Payroll:** `data/nomina_estado.json` gives the date of the last real download.

### 6.5 Known problems and solutions

| Symptom | Cause | Solution |
|---|---|---|
| "Nómina al ..." with an old date | Mapa del Estado requires a browser (Cloudflare). | Manual procedure in section 6.3. |
| SIGEN step red in the weekly workflow | SIGEN blocks connections from abroad. | `scripts/actualizar_sigen.ps1` from a PC in Argentina. |
| Cross-checks add no new awards | That day's `monitor_contratos` report was not generated or came out empty. | Check the PC runner (it must show as "online" on GitHub → Settings → Actions → Runners of that repository) and regenerate the report. |
| Failed "Cobertura de datos" email | An area lost amounts or company names. | Read the workflow log: it says which area and by how much. It is usually a COMPR.AR 503 error and fixes itself on the next Scraper Detalle run. |
| "Railway sin respuesta" email | The site did not answer the smoke tests. | Check the latest Railway deployment and the environment variables. |
| Chat answers with a message limit | The daily limit for that IP was reached. | Wait until the next day, or raise `CHAT_RATE_LIMIT_DIARIO` on Railway. |

## 7. Development and deployment

### 7.1 Local installation

```
git clone https://github.com/Viny2030/jefatura_gabinete.git
cd jefatura_gabinete
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.api.api_server:app --reload --port 8000
```

Then open http://localhost:8000/. The server serves the pages in `src/frontend/` and the data in `src/frontend/data/` under `/data`.

### 7.2 Tests and changes

- `pytest` runs every test. `tests/test_live.py` needs a connection, because it tests the live site.
- Every pull request against `main` runs the unit tests on GitHub Actions ("Tests en Pull Requests" workflow). Do not merge with failing tests.
- The data in `src/frontend/data/` is written by the workflows: do not edit it by hand, except through the procedures in section 6.3.

### 7.3 Environment variables (Railway)

| Variable | Use |
|---|---|
| `DATABASE_URL` | PostgreSQL database (injected by Railway). |
| `REFRESH_TOKEN` | Token for `/api/refresh` and `/api/init-db`. Without it, those endpoints reject every request. |
| `ANTHROPIC_API_KEY` | Key for the AI Agent. Without it the chat answers 503. |
| `ANTHROPIC_MODEL` | Model used by the AI Agent. |
| `CHAT_RATE_LIMIT_DIARIO` | Messages per visitor per day (30 by default). |
| `MEACI_API_URL` | International sanctions API (MEACI). Defaults to https://meaci.ecosistematransparencia.com.ar |

### 7.4 This manual

The manual is written in `docs/manual/manual_es.md` and `docs/manual/manual_en.md`. After editing them, run:

```
python tools/generar_manual.py
```

The script regenerates `src/frontend/manual_usuario.html` (with Español / English tabs) and the Word files `src/frontend/data/manual/Manual_Monitor_PEN_ES.docx` and `_EN.docx`. Do not edit those files by hand.

## 8. REST API

The API is open for queries (GET). Interactive documentation is at `/docs`.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | System status, date of the last update and number of alerts. |
| GET | `/api/alertas`, `/api/v1/alertas` | Active alerts. |
| GET | `/api/contratos` | Contracts. |
| GET | `/api/bora` | Official Gazette notices. |
| GET | `/api/resumen`, `/api/inteligencia` | Aggregated summaries. |
| GET | `/api/grafo`, `/api/v1/grafo/nodos` | Graph nodes and links. |
| GET | `/api/cruce-cuits-bulk?cuits=...` | Checks up to 200 CUITs against the MEACI international sanctions registry. |
| POST | `/api/v1/chat/{area}` | AI Agent (`area`: jgm, sgp or presidencia). |
| GET | `/api/v2/chat/diagnostico` | Active model and data coverage by area. |

The data files can also be downloaded directly, for example `/data/meta.json`, `/data/cruces.json` or `/data/contratos_jgm.json`.

## 9. Author and contact

Vicente Humberto Monteverde, Ph.D. — Doctor of Economic Sciences, researcher in public transparency. Author of the Regressive Income Transfer theory and developer of the XAI approach applied to public procurement. Published in the Journal of Financial Crime (Emerald Publishing).

- vhmonte@retina.ar · viny01958@gmail.com
- Source code: https://github.com/Viny2030/jefatura_gabinete

> Experimental and academic tool. Data comes from official public sources of the Argentine State. Results are algorithmic risk indicators: they imply no value judgement, accusation or finding of responsibility about any company, body or person. Not affiliated with the Argentine government.
