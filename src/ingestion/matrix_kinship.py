def check_nepotism_alert(pariente_nombre, base_contratos):
    """
    Verifica si un pariente de un funcionario ha ganado un contrato.
    """
    if pariente_nombre in base_contratos['Empresa_Adjudicataria'].values:
        return "ALERTA ALTA: Posible Nepotismo detectado." [cite: 23, 24]
    return "Sin alertas"
