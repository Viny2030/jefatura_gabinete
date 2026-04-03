import pandas as pd

def check_nepotism():
    # Datos: Funcionario vs Pariente (Consanguinidad/Afinidad) [cite: 21, 23]
    parentesco = pd.DataFrame({
        'Funcionario': ['Juan Pérez', 'Ana López'],
        'Pariente': ['María Pérez', 'Carlos López'],
        'Vínculo': ['Hermana', 'Hijo']
    })
    # Simulación BORA/COMPR.AR 
    contratistas = ['Tech Soluciones SA', 'Carlos López']
    
    parentesco['Alerta'] = parentesco['Pariente'].apply(
        lambda x: '🚨 NEPOTISMO DETECTADO' if x in contratistas else 'OK'
    )
    print("--- MATRIZ 1: PARENTESCO ---")
    print(parentesco[parentesco['Alerta'] != 'OK'])

if __name__ == "__main__":
    check_nepotism()
