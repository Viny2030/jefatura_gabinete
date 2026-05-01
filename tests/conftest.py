"""
conftest.py — Fixtures compartidas para todos los tests del Monitor JGM.
"""
import pytest

# ── Datos de ejemplo reutilizables ────────────────────────────────────────────

SAMPLE_INTELIGENCIA = {
    "meta": {
        "ultima_actualizacion": "2026-05-01T00:00:00",
        "total_alertas": 3,
        "alertas_alta": 1,
        "alertas_media": 2,
    },
    "alertas": [
        {
            "id": "1",
            "tipo_alerta": "NEPOTISMO",
            "nivel": "ALTA",
            "titulo": "Posible nepotismo: Ramirez / Ramirez Consultora",
            "descripcion": "Apellido coincide entre funcionario y proveedor del mismo organismo.",
        },
        {
            "id": "2",
            "tipo_alerta": "CONFLICTO_SOCIETARIO",
            "nivel": "MEDIA",
            "titulo": "Funcionario es proveedor",
            "descripcion": "CUIL del funcionario aparece como CUIT de proveedor.",
        },
        {
            "id": "3",
            "tipo_alerta": "DESVIO_IAP_GLOBAL",
            "nivel": "MEDIA",
            "titulo": "Desvío de flujo detectado",
            "descripcion": "Transferencia a IAP sin justificación presupuestaria.",
        },
    ],
    "grafo": {
        "nodos": [
            {"id": "f1", "tipo": "funcionario", "nombre": "Carlos Ramirez"},
            {"id": "e1", "tipo": "empresa", "nombre": "Ramirez Consultora SRL"},
        ],
        "aristas": [
            {"source": "f1", "target": "e1", "tipo": "parentesco", "color": "rojo"}
        ],
        "stats": {
            "nodos_funcionarios": 2,
            "nodos_empresas": 1,
            "aristas_rojas": 1,
            "aristas_amarillas": 2,
        },
    },
}

SAMPLE_CONTRATOS = {
    "contratos": [
        {
            "id": "C001",
            "tipo": "adjudicacion",
            "proveedor": "EMPRESA TEST SA",
            "monto_estimado": 1_000_000,
            "organismo": "JGM",
        },
        {
            "id": "C002",
            "tipo": "convocatoria",
            "proveedor": "SERVICIOS XYZ SRL",
            "monto_estimado": 500_000,
            "organismo": "SGP",
        },
    ],
    "meta": {"total": 2, "ultima_actualizacion": "2026-05-01T00:00:00"},
}

SAMPLE_BORA = [
    {
        "titulo": "Designación en JGM",
        "fecha": "2026-05-01",
        "relevante_jgm": True,
        "url": "https://www.boletinoficial.gob.ar/test/1",
    },
    {
        "titulo": "Norma SGP",
        "fecha": "2026-05-01",
        "relevante_jgm": False,
        "url": "https://www.boletinoficial.gob.ar/test/2",
    },
]


@pytest.fixture
def sample_inteligencia():
    """Payload completo de inteligencia.json para mockear _load()."""
    import copy
    return copy.deepcopy(SAMPLE_INTELIGENCIA)


@pytest.fixture
def sample_contratos():
    """Payload de comprar_raw.json."""
    import copy
    return copy.deepcopy(SAMPLE_CONTRATOS)


@pytest.fixture
def sample_bora():
    """Lista de publicaciones BORA."""
    import copy
    return copy.deepcopy(SAMPLE_BORA)
