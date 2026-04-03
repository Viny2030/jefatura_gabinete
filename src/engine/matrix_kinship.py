import pandas as pd

def check_nepotism_alert(pariente_nombre, base_contratos):
    """
    Verifica si un pariente de un funcionario ha ganado un contrato. [cite: 24]
    """
    if pariente_nombre in base_contratos['Empresa_Adjudicataria'].values:
        return "ALERTA ALTA: Posible Nepotismo detectado." [cite: 23, 24]
    return "Sin alertas"

# 1. Matriz de Parentesco según el documento 
parentesco_data = {
    'Funcionario': ['Juan Pérez', 'Juan Pérez', 'Ana López'],
    'Pariente': ['María Pérez', 'Ricardo Gómez', 'Carlos López'],
    'Vínculo': ['Hermana', 'Cuñado', 'Hijo']
}
df_parentesco = pd.DataFrame(parentesco_data)

# 2. Simulación de Base de Contratos (BORA/COMPR.AR) [cite: 8, 9, 24]
# Aquí incluimos a "Carlos López" para disparar la alerta 
contratos_data = {
    'ID': ['LIC-001', 'CONTR-055', 'LIC-099'],
    'Empresa_Adjudicataria': ['Tech Soluciones SA', 'Limpieza Ya SRL', 'Carlos López']
}
df_contratos = pd.DataFrame(contratos_data)

# 3. Ejecución del Cruce de Inteligencia
print("--- PROCESANDO ALERTAS DE PARENTESCO ---")
for index, row in df_parentesco.iterrows():
    resultado = check_nepotism_alert(row['Pariente'], df_contratos)
    if "ALERTA" in resultado:
        print(f"⚠️ {resultado}")
        print(f"   Funcionario: {row['Funcionario']} | Pariente: {row['Pariente']} ({row['Vínculo']})") [cite: 23]
