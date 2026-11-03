﻿# actualizar_nomina.ps1
# ---------------------------------------------------------------------------
# Mapa del Estado (mapadelestado.dyte.gob.ar) está detrás de un desafío de
# Cloudflare que exige un navegador: ningún script puede descargar la nómina,
# ni desde GitHub Actions ni desde una PC en Argentina. El paso "Scraper
# nómina APN" del workflow diario falla por eso, y el portal conserva los
# personal_*.json que haya en main.
#
# Procedimiento (ver docs/manual/manual_es.md, sección 6.3):
#   1. Abrir en el navegador
#      https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv
#      y guardar el CSV.
#   2. .\scripts\actualizar_nomina.ps1 -Csv C:\ruta\al\archivo.csv
# ---------------------------------------------------------------------------
param(
    [Parameter(Mandatory = $true)][string]$Csv
)
$ErrorActionPreference = "Stop"

$Csv = (Resolve-Path $Csv).Path
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }

git pull --ff-only origin main
if ($LASTEXITCODE -ne 0) { throw "git pull falló: dejá la copia local en main y sin cambios." }

& $py scripts/scraper_nomina.py --archivo $Csv
if ($LASTEXITCODE -ne 0) { throw "Falló scraper_nomina.py (¿el CSV es el de Mapa del Estado?)" }

& $py scripts/generar_json.py
if ($LASTEXITCODE -ne 0) { throw "Falló generar_json.py" }

& $py scripts/corregir_personal.py
if ($LASTEXITCODE -ne 0) { throw "Falló corregir_personal.py" }

git add src/frontend/data/
git diff --staged --quiet
if ($LASTEXITCODE -eq 0) { Write-Host "Sin cambios en la nómina."; exit 0 }
git commit -m "Nómina APN actualizada a mano $(Get-Date -Format yyyy-MM-dd)"
git push origin main
