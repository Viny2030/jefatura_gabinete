"""
matrix_kinship.py
Detecta posibles vínculos de parentesco entre funcionarios y proveedores
cruzando apellidos, normas de designación y CUITs de la nómina vs contratos.

Sin acceso a ANSES/Renaper (requiere convenio), usa heurísticas de apellido
y cruza con la nómina y el SIPRO. Marca para revisión manual los casos
con alta probabilidad.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PARENTESCO] %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Umbral de similitud de apellido (Levenshtein aproximado por pandas)
MIN_APELLIDO_LEN = 4  # Ignorar apellidos muy cortos (García, López, etc.)
APELLIDOS_COMUNES = {
    "garcia", "gonzalez", "rodriguez", "fernandez", "lopez", "martinez",
    "perez", "sanchez", "romero", "sosa", "alvarez", "gomez", "diaz",
    "torres", "ruiz", "flores", "benitez", "herrera", "medina", "rojas",
}


def cargar_datos(engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    nomina = pd.read_sql("SELECT cuil, nombre, apellido, cargo, organismo FROM nomina", engine)
    contratos = pd.read_sql(
        "SELECT id, cuit_proveedor, proveedor, monto_adjudicado, organismo, fecha_adjudicacion FROM contratos",
        engine,
    )
    return nomina, contratos


def cargar_desde_csv() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fallback cuando no hay DB."""
    import glob

    nomina_files = sorted(glob.glob("data/nomina_*.csv"))
    contrato_files = sorted(glob.glob("data/adjudicaciones_*.csv"))

    nomina = pd.read_csv(nomina_files[-1]) if nomina_files else pd.DataFrame()
    contratos = pd.read_csv(contrato_files[-1]) if contrato_files else pd.DataFrame()
    return nomina, contratos


def extraer_apellido_proveedor(razon_social: str) -> list[str]:
    """
    Extrae tokens de apellido desde razón social.
    'PÉREZ JUAN SA' → ['perez', 'juan']
    'TECH SOLUCIONES SRL' → [] (no es una persona)
    """
    if not isinstance(razon_social, str):
        return []

    stop_words = {"sa", "srl", "sas", "sl", "sa.", "s.r.l.", "y", "e", "de", "del", "la", "el"}
    tokens = [t.lower().strip(".,") for t in razon_social.split()]
    return [t for t in tokens if t not in stop_words and len(t) >= MIN_APELLIDO_LEN]


def detectar_coincidencias_apellido(
    nomina: pd.DataFrame, contratos: pd.DataFrame
) -> list[dict]:
    """
    Cruza apellidos de funcionarios con tokens de proveedores.
    Genera candidatos para revisión manual.
    """
    coincidencias = []

    if nomina.empty or contratos.empty:
        log.warning("Datos vacíos, no se puede detectar parentesco.")
        return coincidencias

    # Preparar apellidos de funcionarios (no comunes)
    nomina = nomina.copy()
    nomina["apellido_norm"] = (
        nomina["apellido"]
        .fillna("")
        .str.lower()
        .str.strip()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )
    nomina_filtrada = nomina[
        (nomina["apellido_norm"].str.len() >= MIN_APELLIDO_LEN)
        & (~nomina["apellido_norm"].isin(APELLIDOS_COMUNES))
    ]

    apellidos_func = set(nomina_filtrada["apellido_norm"].unique())
    log.info("Apellidos no comunes en nómina: %d", len(apellidos_func))

    for _, contrato in contratos.iterrows():
        tokens = extraer_apellido_proveedor(str(contrato.get("proveedor", "")))
        for token in tokens:
            if token in apellidos_func:
                # Encontrar todos los funcionarios con ese apellido
                funcionarios = nomina_filtrada[nomina_filtrada["apellido_norm"] == token]
                for _, func in funcionarios.iterrows():
                    # Alerta más alta si el proveedor trabaja para el mismo organismo
                    mismo_organismo = (
                        str(contrato.get("organismo", "")).lower()
                        == str(func.get("organismo", "")).lower()
                    )
                    nivel = "alta" if mismo_organismo else "media"
                    coincidencias.append(
                        {
                            "cuil_a": func.get("cuil", ""),
                            "cuil_b": contrato.get("cuit_proveedor", ""),
                            "tipo_vinculo": "parentesco",
                            "subtipo": "apellido_coincidente",
                            "nivel_alerta": nivel,
                            "detalle": {
                                "funcionario": f"{func.get('nombre', '')} {func.get('apellido', '')}",
                                "cargo": func.get("cargo", ""),
                                "proveedor": contrato.get("proveedor", ""),
                                "monto": float(contrato.get("monto_adjudicado", 0) or 0),
                                "mismo_organismo": mismo_organismo,
                                "token_coincidente": token,
                            },
                            "fuente": "heuristica_apellido",
                            "fecha_deteccion": datetime.now().isoformat(),
                        }
                    )

    log.info("Coincidencias de apellido detectadas: %d", len(coincidencias))
    return coincidencias


def guardar_vinculos(vinculos: list[dict], engine=None) -> None:
    if not vinculos:
        return

    import json
    df = pd.DataFrame(vinculos)
    df["detalle"] = df["detalle"].apply(json.dumps)

    if engine:
        try:
            # Upsert: ignorar duplicados (cuil_a, cuil_b, tipo, subtipo)
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
            log.info("→ %d vínculos guardados en DB", len(df))
        except Exception as e:
            log.error("Error guardando vínculos: %s", e)
    else:
        os.makedirs("data", exist_ok=True)
        ruta = f"data/vinculos_parentesco_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(ruta, index=False, encoding="utf-8-sig")
        log.info("→ guardado en %s", ruta)


def generar_alertas(vinculos: list[dict], engine=None) -> None:
    """Inserta alertas de nepotismo en la tabla alertas."""
    if not vinculos or not engine:
        return

    import json
    alta_prio = [v for v in vinculos if v["nivel_alerta"] == "alta"]
    log.info("Alertas de nepotismo a insertar: %d", len(alta_prio))

    with engine.begin() as conn:
        for v in alta_prio:
            det = v["detalle"]
            conn.execute(
                text("""
                    INSERT INTO alertas
                        (tipo, nivel, titulo, descripcion, funcionario_cuil, proveedor_cuit, monto_involucrado)
                    VALUES
                        ('nepotismo', :nivel, :titulo, :descripcion, :funcionario_cuil, :proveedor_cuit, :monto)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "nivel": v["nivel_alerta"],
                    "titulo": f"Posible nepotismo: {det.get('funcionario')} / {det.get('proveedor')}",
                    "descripcion": (
                        f"Apellido '{det.get('token_coincidente')}' coincide entre funcionario "
                        f"({det.get('cargo')}) y proveedor ({det.get('proveedor')}). "
                        f"Mismo organismo: {det.get('mismo_organismo')}."
                    ),
                    "funcionario_cuil": v["cuil_a"],
                    "proveedor_cuit": v["cuil_b"],
                    "monto": det.get("monto", 0),
                },
            )


def main():
    engine = create_engine(DATABASE_URL) if DATABASE_URL else None

    if engine:
        nomina, contratos = cargar_datos(engine)
    else:
        log.warning("Sin DB — cargando desde CSV")
        nomina, contratos = cargar_desde_csv()

    vinculos = detectar_coincidencias_apellido(nomina, contratos)
    guardar_vinculos(vinculos, engine)
    if engine:
        generar_alertas(vinculos, engine)

    log.info("Matriz de parentesco finalizada.")


if __name__ == "__main__":
    main()