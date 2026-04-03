import pandas as pd

def analizar_desvios_pagos(df_contratos):
    # Calcula el desvío porcentual [cite: 32]
    df_contratos['Diferencia_Absoluta'] = df_contratos['Pagado_TGN'] - df_contratos['Monto_Contrato']
    df_contratos['Porcentaje_Desvio'] = (df_contratos['Diferencia_Absoluta'] / df_contratos['Monto_Contrato']) * 100
    
    # Identifica "Red Flags" (ejemplo: +45% como en Tech Soluciones SA) [cite: 32, 33]
    df_contratos['Alerta'] = df_contratos['Porcentaje_Desvio'].apply(
        lambda x: 'RED FLAG' if x > 20 else 'NORMAL'
    )
    return df_contratos

# Datos de prueba basados en el documento [cite: 32]
data = {
    'ID_Contrato': ['LIC-2024-001', 'CONTR-055'],
    'Empresa': ['Tech Soluciones SA', 'Limpieza Ya SRL'],
    'Monto_Contrato': [100000000, 50000000],
    'Pagado_TGN': [145000000, 10000000]
}

df = pd.DataFrame(data)
print(analizar_desvios_pagos(df))
