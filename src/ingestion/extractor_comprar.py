import json
import datetime

def mock_extractor_comprar():
    """
    Simula la ingesta de datos desde el portal de licitaciones COMPR.AR.
    """
    licitaciones = [
        {
            "id_licitacion": "LIC-2024-001",
            "objeto": "Servicios de Consultoría IT",
            "monto_adjudicado": 100000000.00,
            "empresa_adjudicataria": "Tech Soluciones SA",
            "cuit_adjudicataria": "30-71234567-8",
            "fecha": str(datetime.date.today())
        }
    ]
    return licitaciones

# Guardar logs de ingesta
with open('log_ingesta_comprar.json', 'w') as f:
    json.dump(mock_extractor_comprar(), f)
