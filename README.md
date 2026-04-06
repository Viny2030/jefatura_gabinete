# Portal Anticorrupción · Jefatura de Gabinete de Ministros

Sistema de monitoreo automático de contratos, nómina y alertas de conflicto de interés
para la Jefatura de Gabinete de Ministros de la República Argentina.

## Arquitectura

```
Fuentes públicas         Ingesta (Python)       Base de datos
─────────────────        ─────────────────      ────────────
datos.gob.ar      ──▶   scraper_comprar.py ──▶
BORA              ──▶   scraper_bora.py    ──▶  PostgreSQL
mapadelestado     ──▶   scraper_nomina.py  ──▶

Motor de cruce           FastAPI                Frontend público
──────────────           ───────                ────────────────
matrix_kinship.py  ──▶
matrix_corporate.py──▶  /api/v1/...     ──▶   dashboard.html
matrix_cashflow.py ──▶                         grafo_nodos.html
                                               alertas.html
```

## Stack

| Componente | Tecnología |
|---|---|
| Ingesta | Python + requests + pandas |
| Base de datos | PostgreSQL (Railway) |
| API | FastAPI + asyncpg |
| Frontend | HTML/JS vanilla + Chart.js + vis.js |
| Automatización | GitHub Actions (cron diario 6 AM) |
| Deploy | Railway |

## Fuentes de datos

| Fuente | URL | Frecuencia |
|---|---|---|
| COMPR.AR (adjudicaciones) | datos.gob.ar | Semestral (CSV oficial) |
| BORA (resoluciones) | boletinoficial.gob.ar | Diaria |
| Nómina JGM | mapadelestado.dyte.gob.ar | Diaria |
| SIPRO (proveedores) | datos.gob.ar | Semestral |

## Setup local

### Requisitos
- Python 3.11+
- PostgreSQL 15+ (o Docker)
- Git

### Pasos

```bash
# 1. Clonar
git clone https://github.com/Viny2030/jefatura_gabinete.git
cd jefatura_gabinete

# 2. Entorno virtual
python -m venv .venv
.venv\Scripts\activate     # Windows PowerShell
# source .venv/bin/activate  # Linux/Mac

# 3. Dependencias
pip install -r requirements.txt

# 4. Configurar DB
cp .env.example .env
# Editar .env con tu DATABASE_URL

# 5. Crear schema
psql $DATABASE_URL -f src/database/schema.sql

# 6. Correr scrapers (primera carga)
python src/ingestion/scraper_comprar.py
python src/ingestion/scraper_nomina.py
python src/ingestion/scraper_bora.py

# 7. Correr motor de cruce
python src/engine/matrix_kinship.py
python src/engine/matrix_corporate.py
python src/engine/matrix_cashflow.py

# 8. Levantar API
uvicorn src.api.api_server:app --reload --port 8000

# 9. Abrir dashboard
# http://localhost:8000
```

## Fix crítico: GitHub Actions

El directorio anterior era `.github/workflkows/` (typo). El correcto es `.github/workflows/`.
Este repo ya tiene la carpeta corregida.

## Matrices de detección

### 1. Parentesco (matrix_kinship.py)
Cruza apellidos de la nómina contra tokens de razón social de proveedores.
Genera alerta ALTA cuando el proveedor trabaja para el mismo organismo que el funcionario.

### 2. Societaria (matrix_corporate.py)
Detecta cuando el CUIL de un funcionario coincide exactamente con el CUIT de un proveedor
(persona física contratando con el Estado mientras trabaja en él).

### 3. Flujo de fondos (matrix_cashflow.py)
- **Sobreprecio**: contratos >40% sobre la mediana del rubro/organismo → alerta ALTA si >80%
- **Concentración**: proveedor con >30% del gasto total de un organismo → alerta MEDIA/ALTA

## Deploy en Railway

1. Crear proyecto en Railway
2. Agregar servicio PostgreSQL
3. Copiar `DATABASE_URL` de Railway a las variables de entorno del servicio web
4. Conectar el repositorio GitHub → Railway detecta el `Dockerfile` automáticamente
5. Agregar `DATABASE_URL` como secreto en GitHub Actions (`Settings → Secrets`)

## Licencia

MIT — datos públicos, código abierto.