# Nómina APN — CSV bajado a mano

Mapa del Estado exige un navegador para descargar la nómina, así que no se puede
bajar con un script. Para actualizarla:

1. Abrir en el navegador
   https://mapadelestado.dyte.gob.ar/back/api/datos.php?db=m&id=9&fi=csv
   y guardar el CSV.
2. En GitHub, entrar a esta carpeta → **Add file → Upload files** → subir el CSV
   (por ejemplo `nomina_2026-11-03.csv`) → **Commit changes** en `main`.

El workflow "Nómina APN — desde CSV subido" (`.github/workflows/nomina_manual.yml`)
procesa el archivo más reciente, regenera los JSON del portal y los publica.
Detalle completo: `docs/manual/manual_es.md`, sección 6.3.
