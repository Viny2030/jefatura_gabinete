"""
matrix_cashflow.py
Detecta desvíos en el flujo de fondos comparando contratos del mismo tipo/objeto
para identificar sobreprecios, adjudicaciones directas anómalas y concentración
de contratos en un mismo proveedor.
"""

import os
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CASHFLOW] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Umbrales de alerta
UMBRAL_SOBREPRECIO_PCT  = 0.40   # >40% sobre la mediana del rubro → alerta alta
UMBRAL_CONCENTRACION    = 0.30   # proveedor con >30% del total del organismo → alerta media
MONTO_MINIMO_ANALISIS   = 1_000_000  # ARS, ignorar contratos pequeños


def cargar_contratos(engine=None) -> pd.DataFrame:
    if engine:
        return pd.read_sql(
            """
            SELECT id, organismo, tipo_proceso, objeto, proveedor,
                   cuit_proveedor, monto_adjudicado, fecha_adjudicacion
            FROM contratos
            WHERE monto_adjudicado >= %(min)s
            """,
            engine,
            params={"min": MONTO_MINIMO_ANALISIS},
        )
    import glob
    files = sorted(glob.glob("data/adjudicaciones_*.csv"))
    if not files:
        return pd.DataFrame()
    df = pd.read_csv(files[-1])
    df["monto_adjudicado"] = pd.to_numeric(df.get("monto_adjudicado", 0), errors="coerce")
    return df[df["monto_adjudicado"] >= MONTO_MINIMO_ANALISIS]


def detectar_sobreprecios(df: pd.DataFrame) -> list[dict]:
    """
    Para cada combinación organismo+tipo_proceso, calcula la mediana de montos
    y marca como sobreprecio los contratos que superen el umbral.
    """
    alertas = []
    if df.empty or "monto_adjudicado" not in df.columns:
        return alertas

    # Agrupar por organismo y tipo de proceso
    grupos = df.groupby(["organismo", "tipo_proceso"], dropna=True)

    for (organismo, tipo), grupo in grupos:
        if len(grupo) < 3:  # Necesitamos mínimo 3 para que la mediana sea representativa
            continue

        mediana = grupo["monto_adjudicado"].median()
        umbral = mediana * (1 + UMBRAL_SOBREPRECIO_PCT)

        sobreprecios = grupo[grupo["monto_adjudicado"] > umbral]
        for _, contrato in sobreprecios.iterrows():
            desvio_pct = (contrato["monto_adjudicado"] - mediana) / mediana * 100
            alertas.append({
                "tipo": "sobreprecio",
                "nivel": "alta" if desvio_pct > 80 else "media",
                "contrato_id": str(contrato.get("id", "")),
                "cuit_proveedor": contrato.get("cuit_proveedor", ""),
                "proveedor": contrato.get("proveedor", ""),
                "organismo": organismo,
                "monto": float(contrato["monto_adjudicado"]),
                "mediana_rubro": float(mediana),
                "desvio_pct": round(desvio_pct, 1),
                "tipo_proceso": tipo,
            })

    log.info("Sobreprecios detectados: %d", len(alertas))
    return alertas


def detectar_concentracion(df: pd.DataFrame) -> list[dict]:
    """
    Detecta proveedores que concentran más del UMBRAL_CONCENTRACION
    del gasto total de un organismo.
    """
    alertas = []
    if df.empty:
        return alertas

    total_por_organismo = df.groupby("organismo")["monto_adjudicado"].sum()

    for organismo, total in total_por_organismo.items():
        if total <= 0:
            continue
        por_proveedor = df[df["organismo"] == organismo].groupby(
            ["cuit_proveedor", "proveedor"]
        )["monto_adjudicado"].sum()

        for (cuit, nombre), subtotal in por_proveedor.items():
            pct = subtotal / total
            if pct > UMBRAL_CONCENTRACION:
                alertas.append({
                    "tipo": "concentracion",
                    "nivel": "alta" if pct > 0.50 else "media",
                    "cuit_proveedor": cuit,
                    "proveedor": nombre,
                    "organismo": organismo,
                    "monto": float(subtotal),
                    "pct_del_total": round(pct * 100, 1),
                    "total_organismo": float(total),
                })

    log.info("Concentraciones detectadas: %d", len(alertas))
    return alertas


def guardar_alertas(alertas_sp: list[dict], alertas_conc: list[dict], engine=None) -> None:
    todos = alertas_sp + alertas_conc
    if not todos:
        return

    if engine:
        with engine.begin() as conn:
            for a in todos:
                titulo = (
                    f"Sobreprecio {a.get('desvio_pct', '')}% en {a.get('organismo', '')}"
                    if a["tipo"] == "sobreprecio"
                    else f"Concentración {a.get('pct_del_total', '')}% — {a.get('proveedor', '')}"
                )
                conn.execute(
                    text("""
                        INSERT INTO alertas
                            (tipo, nivel, titulo, descripcion, proveedor_cuit, monto_involucrado)
                        VALUES
                            (:tipo, :nivel, :titulo, :desc, :cuit, :monto)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "tipo": a["tipo"],
                        "nivel": a["nivel"],
                        "titulo": titulo,
                        "desc": str(a),
                        "cuit": a.get("cuit_proveedor", ""),
                        "monto": a.get("monto", 0),
                    },
                )
        log.info("→ %d alertas de flujo de fondos guardadas", len(todos))
    else:
        import json
        os.makedirs("data", exist_ok=True)
        ruta = f"data/alertas_fondos_{datetime.now().strftime('%Y%m%d')}.json"
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(todos, f, ensure_ascii=False, indent=2)
        log.info("→ guardado en %s", ruta)


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None
    df = cargar_contratos(engine)

    if df.empty:
        log.warning("Sin datos de contratos para analizar.")
        return

    log.info("Contratos a analizar: %d (monto mínimo: $%s)", len(df), f"{MONTO_MINIMO_ANALISIS:,.0f}")

    alertas_sp   = detectar_sobreprecios(df)
    alertas_conc = detectar_concentracion(df)
    guardar_alertas(alertas_sp, alertas_conc, engine)

    log.info("Matriz de flujo de fondos finalizada.")


if __name__ == "__main__":
    main()