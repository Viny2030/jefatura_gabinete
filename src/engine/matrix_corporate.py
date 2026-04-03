import pandas as pd

def analizar_vinculos_societarios():
    # Datos basados en el documento: Relacion Funcionario -> Empresa
    data = {
        'Persona': ['Ricardo Gómez', 'Juan Pérez'],
        'Relacion_Funcionario': ['Cuñado (de Ministro)', 'Ministro'],
        'Empresa_CUIT': ['30-71234567-8', '30-99887766-5'],
        'Rol': ['Presidente', 'Ex-Socio'],
        'Participacion_Pct': [50.0, 0.0]
    }
    
    df = pd.DataFrame(data)
    
    # Riesgo: Si tiene acciones vigentes es un conflicto flagrante
    df['Riesgo_Conflicto'] = df['Participacion_Pct'].apply(
        lambda x: 'ALTO' if x > 0 else 'HISTÓRICO'
    )
    
    print("--- MATRIZ SOCIETARIA (Vínculos IGJ) ---")
    print(df)

if __name__ == "__main__":
    analizar_vinculos_societarios()
