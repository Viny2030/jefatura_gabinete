# actualizar_nomina.ps1
# ---------------------------------------------------------------------------
# Mapa del Estado está detrás de un desafío de Cloudflare: GitHub Actions y
# los scripts no pueden descargar la nómina. Este script procesa el CSV que
# bajás a mano desde el navegador y sube el resultado.
#
# 1. Abrí en el navegador (pasa el desafío solo):
#    https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv
#    y guardá el archivo (por ejemplo en Descargas\mapa_estado.csv).
# 2. Desde la carpeta del repo:
#    .\scripts\actualizar_nomina.ps1 -Csv "$HOME\Downloads\mapa_estado.csv"
# ---------------------------------------------------------------------------
param(
    [Parameter(Mandatory = $true)][string]$Csv,
    [switch]$SinPush
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

if (-not (Test-Path $Csv)) { throw "No existe el archivo $Csv" }

git pull --ff-only origin main

python scripts/scraper_nomina.py --archivo "$Csv"
if ($LASTEXITCODE -ne 0) { throw "Falló scraper_nomina.py" }

python scripts/generar_json.py
if ($LASTEXITCODE -ne 0) { throw "Falló generar_json.py" }

python scripts/corregir_personal.py
if ($LASTEXITCODE -ne 0) { throw "Falló corregir_personal.py" }

git add src/frontend/data/personal_jgm.json src/frontend/data/personal_sgp.json `
        src/frontend/data/personal_presidencia.json src/frontend/data/meta.json `
        src/frontend/data/nomina_estado.json
git commit -m "Nómina APN actualizada a mano $(Get-Date -Format yyyy-MM-dd)"

if (-not $SinPush) {
    git push origin main
    Write-Host "Listo. Los cruces se recalculan solos esta noche (workflow Generador de Cruces PEN)."
}
