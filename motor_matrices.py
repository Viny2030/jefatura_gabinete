"""
motor_matrices.py
=================
Motor de cruce e inteligencia del portal anticorrupción JGM.
Implementa las 3 matrices fundamentales:

  1. Matriz de Parentesco   → detecta nepotismo
  2. Matriz Societaria      → detecta "capitalismo de amigos" (funcionario = socio de proveedor)
  3. Matriz Flujo de Fondos → detecta sobreprecios y desvíos

Además genera el grafo de nodos para visualización.

Uso:
    python motor_matrices.py
    python motor_matrices.py --solo parentesco
"""

import argparse
import json
import os
import re
from datetime import datetime
from difflib import SequenceMatcher

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
OUTPUT_DIR = DATA_DIR

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sim(a: str, b: str) -> float:
    """Similitud de strings (0-1). Umbral recomendado: 0.82"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _normalizar(nombre: str) -> str:
    """Normaliza nombre para comparación."""
    return re.sub(r"\s+", " ", nombre.strip().upper())


def _load_json(nombre: str) -> dict:
    path = os.path.join(DATA_DIR, nombre)
    if not os.path.exists(path):
        print(f"[WARN] {path} no encontrado — usando datos vacíos")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Datos base (nómina de funcionarios JGM)
# En producción: viene del scraper de RRHH / Transparencia JGM
# ---------------------------------------------------------------------------
FUNCIONARIOS_DEMO = [
    {"id": "F001", "nombre": "GARCIA PEREZ, JUAN CARLOS", "cargo": "Jefe de Gabinete", "area": "Jefatura", "cuit": "20-12345678-9"},
    {"id": "F002", "nombre": "MARTINEZ, ANA LUCIA", "cargo": "Secretaria de Innovación", "area": "Innovación", "cuit": "27-23456789-0"},
    {"id": "F003", "nombre": "LOPEZ RODRIGUEZ, CARLOS", "cargo": "Subsecretario RRHH", "area": "RRHH", "cuit": "20-34567890-1"},
    {"id": "F004", "nombre": "FERNANDEZ, MARIA JOSE", "cargo": "Directora Nacional", "area": "Compras", "cuit": "27-45678901-2"},
    {"id": "F005", "nombre": "GOMEZ, RICARDO DANIEL", "cargo": "Coordinador IT", "area": "Tecnología", "cuit": "20-56789012-3"},
]

# Vínculos familiares (en producción: cruza ANSES + Registro Civil)
PARENTESCOS_DEMO = [
    {"persona_a": "F003", "persona_b": "F005", "vinculo": "cuñados", "fuente": "declaracion_jurada"},
    {"persona_a": "F001", "persona_b": "F004", "vinculo": "cónyuge", "fuente": "anses"},
]

# ---------------------------------------------------------------------------
# MATRIZ 1: Parentesco
# ---------------------------------------------------------------------------

def matriz_parentesco(funcionarios: list, parentescos: list, contratos: list) -> list:
    """
    Cruza parentescos con contratos adjudicados.
    Si el proveedor ganador tiene relación familiar con un funcionario → ALERTA NEPOTISMO.
    """
    alertas = []
    func_map = {f["id"]: f for f in funcionarios}

    for contrato in contratos:
        if contrato.get("tipo") != "adjudicacion":
            continue
        proveedor_nombre = _normalizar(contrato.get("proveedor_nombre", ""))
        proveedor_cuit = contrato.get("proveedor_cuit", "")

        for parentesco in parentescos:
            for id_func in [parentesco["persona_a"], parentesco["persona_b"]]:
                func = func_map.get(id_func, {})
                func_nombre = _normalizar(func.get("nombre", ""))

                # Match por similitud de nombre o por CUIT
                similitud = _sim(proveedor_nombre, func_nombre)
                cuit_match = proveedor_cuit and proveedor_cuit == func.get("cuit")

                if similitud > 0.75 or cuit_match:
                    # Determinar quién es el familiar
                    otro_id = parentesco["persona_b"] if id_func == parentesco["persona_a"] else parentesco["persona_a"]
                    otro_func = func_map.get(otro_id, {})
                    alertas.append({
                        "tipo_alerta": "NEPOTISMO",
                        "nivel": "ALTA",
                        "descripcion": f"Proveedor '{contrato.get('proveedor_nombre')}' tiene vínculo '{parentesco['vinculo']}' con funcionario '{otro_func.get('nombre', otro_id)}'",
                        "funcionario_contratante": otro_func.get("nombre", otro_id),
                        "funcionario_cargo": otro_func.get("cargo", ""),
                        "funcionario_area": otro_func.get("area", ""),
                        "proveedor": contrato.get("proveedor_nombre"),
                        "proveedor_cuit": proveedor_cuit,
                        "contrato_numero": contrato.get("numero_proceso"),
                        "monto_m": round((contrato.get("monto_estimado", 0) or 0) / 1e6, 2),
                        "fecha": contrato.get("fecha_publicacion", ""),
                        "similitud_nombre": round(similitud, 2),
                        "fuente_parentesco": parentesco.get("fuente", "ND"),
                        "nodo_a": id_func,
                        "nodo_b": otro_id,
                        "tipo_enlace": "familiar"
                    })

    print(f"[MATRIZ 1] Parentesco: {len(alertas)} alertas")
    return alertas


# ---------------------------------------------------------------------------
# MATRIZ 2: Societaria
# ---------------------------------------------------------------------------

def matriz_societaria(funcionarios: list, proveedores: dict, contratos: list) -> list:
    """
    Detecta cuando un funcionario es socio/director de una empresa proveedora.
    En producción: cruza IGJ (igj.gob.ar tiene API) + AFIP (por CUIT).
    Aquí usa el padrón SIPRO de COMPRAR.
    """
    alertas = []

    for func in funcionarios:
        func_nombre = _normalizar(func.get("nombre", ""))
        func_cuit = func.get("cuit", "")

        for cuit, proveedor in proveedores.items():
            prov_nombre = _normalizar(proveedor.get("razon_social", ""))
            similitud = _sim(func_nombre.split(",")[0], prov_nombre)  # comparar solo apellido

            # Match si el nombre del funcionario aparece en el nombre del proveedor
            # (empresas frecuentemente tienen el apellido del dueño)
            if similitud > 0.70:
                # Buscar contratos de este proveedor
                contratos_proveedor = [
                    c for c in contratos
                    if c.get("proveedor_cuit") == cuit and c.get("tipo") == "adjudicacion"
                ]
                if not contratos_proveedor:
                    continue

                monto_total = sum(c.get("monto_estimado", 0) or 0 for c in contratos_proveedor)
                alertas.append({
                    "tipo_alerta": "CONFLICTO_SOCIETARIO",
                    "nivel": "ALTA" if monto_total > 10_000_000 else "MEDIA",
                    "descripcion": f"Funcionario '{func.get('nombre')}' podría ser propietario/director de '{proveedor.get('razon_social')}' (similitud {similitud:.2f})",
                    "funcionario": func.get("nombre"),
                    "funcionario_cargo": func.get("cargo", ""),
                    "funcionario_cuit": func_cuit,
                    "empresa_razon_social": proveedor.get("razon_social"),
                    "empresa_cuit": cuit,
                    "empresa_tipo": proveedor.get("tipo_empresa", ""),
                    "contratos_cantidad": len(contratos_proveedor),
                    "monto_total_m": round(monto_total / 1e6, 2),
                    "similitud_nombre": round(similitud, 2),
                    "requiere_verificacion_igj": True,
                    "nodo_a": func.get("id", ""),
                    "nodo_b": cuit,
                    "tipo_enlace": "comercial"
                })

    print(f"[MATRIZ 2] Societaria: {len(alertas)} alertas")
    return alertas


# ---------------------------------------------------------------------------
# MATRIZ 3: Flujo de Fondos
# ---------------------------------------------------------------------------

def matriz_flujo_fondos(contratos: list, tgn_data: dict, umbral_pct: float = 15.0) -> list:
    """
    Compara montos adjudicados (COMPRAR) vs montos pagados (TGN).
    Desvío > umbral_pct → alerta de sobreprecio o pago irregular.
    """
    alertas = []
    ejecucion = tgn_data.get("ejecucion_actual", {})
    iap_global = ejecucion.get("iap") or 0.94

    adjudicaciones = [c for c in contratos if c.get("tipo") == "adjudicacion" and c.get("monto_estimado", 0) > 0]

    # Comparativa global: si el IAP de JGM está muy lejos de 1.0
    if iap_global:
        desvio = abs(1 - iap_global) * 100
        if desvio > umbral_pct:
            alertas.append({
                "tipo_alerta": "DESVIO_IAP_GLOBAL",
                "nivel": "ALTA" if desvio > 30 else "MEDIA",
                "descripcion": f"IAP global JGM = {iap_global} (desvío {desvio:.1f}% respecto al presupuesto)",
                "iap": iap_global,
                "desvio_pct": round(desvio, 1),
                "credito_m": ejecucion.get("credito_vigente_m"),
                "devengado_m": ejecucion.get("devengado_m"),
                "fuente": "TGN vs Presupuesto",
                "nodo_a": "JGM",
                "nodo_b": "TGN",
                "tipo_enlace": "flujo"
            })

    # Por contrato: detectar montos atípicamente altos (top 10% por modalidad)
    if adjudicaciones:
        montos = sorted([c.get("monto_estimado", 0) for c in adjudicaciones])
        if montos:
            umbral_alto = montos[int(len(montos) * 0.9)]  # percentil 90
            for c in adjudicaciones:
                monto = c.get("monto_estimado", 0)
                if monto > umbral_alto and monto > 50_000_000:  # > 50M ARS
                    alertas.append({
                        "tipo_alerta": "MONTO_ATIPICO",
                        "nivel": "MEDIA",
                        "descripcion": f"Contrato en percentil top 10% del período: ${monto/1e6:.1f}M ARS",
                        "contrato_numero": c.get("numero_proceso"),
                        "descripcion_contrato": c.get("descripcion", "")[:150],
                        "monto_m": round(monto / 1e6, 2),
                        "umbral_p90_m": round(umbral_alto / 1e6, 2),
                        "proveedor": c.get("proveedor_nombre"),
                        "proveedor_cuit": c.get("proveedor_cuit"),
                        "fecha": c.get("fecha_publicacion", ""),
                        "fuente": "COMPRAR",
                        "nodo_a": c.get("proveedor_cuit", ""),
                        "nodo_b": "JGM",
                        "tipo_enlace": "flujo"
                    })

    print(f"[MATRIZ 3] Flujo de fondos: {len(alertas)} alertas")
    return alertas


# ---------------------------------------------------------------------------
# Grafo de Nodos (para visualización)
# ---------------------------------------------------------------------------

def construir_grafo(funcionarios: list, contratos: list, alertas: list) -> dict:
    """
    Construye el grafo de nodos para la visualización:
    - Nodos azules: funcionarios
    - Nodos verdes: empresas/proveedores
    - Aristas rojas: vínculos familiares
    - Aristas amarillas: vínculos comerciales
    - Aristas naranjas: flujo de fondos con desvío
    """
    nodos = {}
    aristas = []

    # Nodos de funcionarios
    for f in funcionarios:
        nodos[f["id"]] = {
            "id": f["id"],
            "label": f["nombre"],
            "tipo": "funcionario",
            "color": "azul",
            "cargo": f.get("cargo", ""),
            "area": f.get("area", ""),
            "alertas": 0
        }

    # Nodos de proveedores (de contratos adjudicados)
    proveedores_vistos = set()
    for c in contratos:
        if c.get("tipo") != "adjudicacion" or not c.get("proveedor_cuit"):
            continue
        cuit = c["proveedor_cuit"]
        if cuit not in proveedores_vistos:
            nodos[cuit] = {
                "id": cuit,
                "label": c.get("proveedor_nombre") or cuit,
                "tipo": "empresa",
                "color": "verde",
                "monto_total_m": 0,
                "contratos": 0,
                "alertas": 0
            }
            proveedores_vistos.add(cuit)
        nodos[cuit]["monto_total_m"] = round(
            nodos[cuit].get("monto_total_m", 0) + (c.get("monto_estimado", 0) or 0) / 1e6, 2
        )
        nodos[cuit]["contratos"] = nodos[cuit].get("contratos", 0) + 1

    # Aristas desde alertas
    for alerta in alertas:
        nodo_a = alerta.get("nodo_a", "")
        nodo_b = alerta.get("nodo_b", "")
        tipo = alerta.get("tipo_enlace", "")

        if not nodo_a or not nodo_b:
            continue

        color_arista = {
            "familiar": "rojo",
            "comercial": "amarillo",
            "flujo": "naranja"
        }.get(tipo, "gris")

        aristas.append({
            "from": nodo_a,
            "to": nodo_b,
            "tipo": tipo,
            "color": color_arista,
            "nivel": alerta.get("nivel", "MEDIA"),
            "descripcion": alerta.get("descripcion", "")[:100]
        })

        # Incrementar contador de alertas en los nodos
        for nid in [nodo_a, nodo_b]:
            if nid in nodos:
                nodos[nid]["alertas"] = nodos[nid].get("alertas", 0) + 1

    return {
        "nodos": list(nodos.values()),
        "aristas": aristas,
        "stats": {
            "total_nodos": len(nodos),
            "nodos_funcionarios": sum(1 for n in nodos.values() if n.get("tipo") == "funcionario"),
            "nodos_empresas": sum(1 for n in nodos.values() if n.get("tipo") == "empresa"),
            "total_aristas": len(aristas),
            "aristas_rojas": sum(1 for a in aristas if a["color"] == "rojo"),
            "aristas_amarillas": sum(1 for a in aristas if a["color"] == "amarillo"),
        }
    }


# ---------------------------------------------------------------------------
# Pipeline completo
# ---------------------------------------------------------------------------

def run(solo: str = None) -> dict:
    print("[MOTOR] Iniciando motor de matrices...")

    # Cargar datos de scrapers
    comprar_data = _load_json("comprar_raw.json")
    tgn_data = _load_json("tgn_raw.json")

    contratos = comprar_data.get("contratos", [])
    proveedores = comprar_data.get("proveedores", {})

    # Usar demo si no hay datos reales
    funcionarios = FUNCIONARIOS_DEMO
    parentescos = PARENTESCOS_DEMO

    alertas = []

    if not solo or solo == "parentesco":
        alertas += matriz_parentesco(funcionarios, parentescos, contratos)

    if not solo or solo == "societaria":
        alertas += matriz_societaria(funcionarios, proveedores, contratos)

    if not solo or solo == "flujo":
        alertas += matriz_flujo_fondos(contratos, tgn_data)

    grafo = construir_grafo(funcionarios, contratos, alertas)

    resultado = {
        "meta": {
            "ultima_actualizacion": datetime.now().isoformat(),
            "total_alertas": len(alertas),
            "alertas_alta": sum(1 for a in alertas if a.get("nivel") == "ALTA"),
            "alertas_media": sum(1 for a in alertas if a.get("nivel") == "MEDIA"),
            "funcionarios_analizados": len(funcionarios),
            "contratos_analizados": len(contratos),
            "proveedores_analizados": len(proveedores),
        },
        "alertas": sorted(alertas, key=lambda x: 0 if x.get("nivel") == "ALTA" else 1),
        "grafo": grafo,
        "funcionarios": funcionarios,
    }

    print(f"[MOTOR] Listo — {len(alertas)} alertas ({resultado['meta']['alertas_alta']} ALTA, {resultado['meta']['alertas_media']} MEDIA)")
    return resultado


def save(data: dict, path: str = None):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if path is None:
        path = os.path.join(OUTPUT_DIR, "inteligencia.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[MOTOR] Guardado en {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--solo", choices=["parentesco", "societaria", "flujo"], help="Correr solo una matriz")
    args = parser.parse_args()

    data = run(solo=args.solo)
    save(data)
