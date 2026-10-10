# actualizar_sigen.ps1
# ---------------------------------------------------------------------------
# SIGEN (sigen.gob.ar) corta las conexiones que no vienen de Argentina, así
# que el workflow semanal puede fallar en ese paso. Esto lo hace desde tu PC.
#
#   .\scripts\actualizar_sigen.ps1
# ---------------------------------------------------------------------------
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

git pull --ff-only origin main

python scripts/generar_sigen_auditorias.py
if ($LASTEXITCODE -ne 0) { throw "Falló generar_sigen_auditorias.py (¿sitio caído?)" }

git add src/frontend/data/sigen_auditorias.json
git diff --staged --quiet
if ($LASTEXITCODE -eq 0) { Write-Host "Sin cambios en SIGEN."; exit 0 }
git commit -m "SIGEN actualizado a mano $(Get-Date -Format yyyy-MM-dd)"
git push origin main
