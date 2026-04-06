"""
matrix_corporate.py
Detecta funcionarios que aparecen como proveedores (mismo CUIL/CUIT)
o cuyo CUIT aparece en contratos. Cruza nómina vs contratos por CUIL/CUIT.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SOCIETARIO] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def cargar_datos(engine):
    nomina = pd.read_sql("SELECT cuil, nombre, apellido, cargo, organismo FROM nomina", engine)
    contratos = pd.read_sql(
        "SELECT id, cuit_proveedor, proveedor, monto_adjudicado, organismo FROM contratos",
        engine,
    )
    sipro = pd.read_sql("SELECT cuit, razon_social FROM proveedores_sipro", engine)
    return nomina, contratos, sipro


def cargar_desde_csv():
    import glob
    nomina_files = sorted(glob.glob("data/nomina_*.csv"))
    contrato_files = sorted(glob.glob("data/adjudicaciones_*.csv"))
    nomina = pd.read_csv(nomina_files[-1]) if nomina_files else pd.DataFrame()
    contratos = pd.read_csv(contrato_files[-1]) if contrato_files else pd.DataFrame()
    return nomina, contratos, pd.DataFrame()


def detectar_funcionario_proveedor(
    nomina: pd.DataFrame, contratos: pd.DataFrame
) -> list[dict]:
    """
    Caso más directo: un funcionario tiene el mismo CUIL que un proveedor
    (persona física que contrata con el Estado mientras trabaja en él).
    """
    if nomina.empty or contratos.empty:
        return []

    # CUIL funcionario sin guiones → comparar con CUIT proveedor
    nomina_cuil = nomina[nomina["cuil"].notna()].copy()
    nomina_cuil["cuil_norm"] = nomina_cuil["cuil"].str.replace(r"[^0-9]", "", regex=True)

    contratos_cuit = contratos[contratos["cuit_proveedor"].notna()].copy()
    contratos_cuit["cuit_norm"] = contratos_cuit["cuit_proveedor"].str.replace(r"[^0-9]", "", regex=True)

    merged = nomina_cuil.merge(contratos_cuit, left_on="cuil_norm", right_on="cuit_norm", how="inner")

    vinculos = []
    for _, row in merged.iterrows():
        vinculos.append({
            "cuil_a": row["cuil"],
            "cuil_b": row["cuit_proveedor"],
            "tipo_vinculo": "societario",
            "subtipo": "funcionario_es_proveedor",
            "nivel_alerta": "alta",
            "detalle": {
                "funcionario": f"{row.get('nombre', '')} {row.get('apellido', '')}",
                "cargo": row.get("cargo", ""),
                "proveedor": row.get("proveedor", ""),
                "monto": float(row.get("monto_adjudicado", 0) or 0),
                "organismo": row.get("organismo_x", ""),
            },
            "fuente": "cruce_cuil_cuit",
            "fecha_deteccion": datetime.now().isoformat(),
        })

    log.info("Funcionarios que son proveedores: %d", len(vinculos))
    return vinculos


def guardar_vinculos(vinculos: list[dict], engine=None) -> None:
    if not vinculos:
        return
    import json
    df = pd.DataFrame(vinculos)
    df["detalle"] = df["detalle"].apply(json.dumps)

    if engine:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(
                    text("""
                        INSERT INTO vinculos
                            (cuil_a, cuil_b, tipo_vinculo, subtipo, nivel_alerta, detalle, fuente)
                        VALUES
                            (:cuil_a, :cuil_b, :tipo_vinculo, :subtipo, :nivel_alerta, :detalle::jsonb, :fuente)
                        ON CONFLICT (cuil_a, cuil_b, tipo_vinculo, subtipo) DO UPDATE
                            SET nivel_alerta = EXCLUDED.nivel_alerta,
                                detalle      = EXCLUDED.detalle,
                                fecha_deteccion = NOW()
                    """),
                    row.to_dict(),
                )
        log.info("→ %d vínculos societarios guardados", len(df))
    else:
        os.makedirs("data", exist_ok=True)
        ruta = f"data/vinculos_societario_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(ruta, index=False, encoding="utf-8-sig")
        log.info("→ guardado en %s", ruta)


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None
    if engine:
        nomina, contratos, sipro = cargar_datos(engine)
    else:
        log.warning("Sin DB — cargando desde CSV")
        nomina, contratos, sipro = cargar_desde_csv()

    vinculos = detectar_funcionario_proveedor(nomina, contratos)
    guardar_vinculos(vinculos, engine)
    log.info("Matriz societaria finalizada.")


if __name__ == "__main__":
    main()