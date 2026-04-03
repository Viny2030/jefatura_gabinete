def extract_tgn_payments(contrato_id):
    """
    Consulta los pagos ejecutados en la TGN para un ID de contrato específico.
    """
    # Simulación de respuesta de la base de pagos de la TGN
    pagos_db = {
        "LIC-2024-001": 145000000.00, # Caso de sobreprecio detectado en el doc
        "CONTR-055": 10000000.00
    }
    return pagos_db.get(contrato_id, 0.0)
