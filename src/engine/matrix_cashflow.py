import pandas as pd

def analizar_pagos():
    # Comparación BORA/COMPR.AR vs TGN [cite: 31, 32]
    data = {
        'ID': ['LIC-2024-001', 'CONTR-055'],
        'Empresa': ['Tech Soluciones SA', 'Limpieza Ya SRL'],
        'Contrato_$': [100000000, 50000000],
        'Pagado_TGN_$': [145000000, 10000000]
    }
    df = pd.DataFrame(data)
    df['Desvio'] = ((df['Pagado_TGN_$'] - df['Contrato_$']) / df['Contrato_$']) * 100
    df['Status'] = df['Desvio'].apply(lambda x: '🚩 RED FLAG' if x > 20 else 'OK')
    print("\n--- MATRIZ 3: FLUJO DE FONDOS ---")
    print(df)

if __name__ == "__main__":
    analizar_pagos()
