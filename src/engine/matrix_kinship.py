import pandas as pd

def check_nepotism_alert(pariente_nombre, base_contratos):
    """Verifica si un pariente de un funcionario ha ganado un contrato."""
    if pariente_nombre in base_contratos['Empresa_Adjudicataria'].values:
        return "ALERTA ALTA: Posible Nepotismo detectado."
    return "Sin alertas"

# Datos de la Matriz de Parentesco (Matriz 1 del documento)
df_parentesco = pd.DataFrame({
    'Funcionario': ['Juan Pérez', 'Juan Pérez', 'Ana López'],
    'Pariente': ['María Pérez', 'Ricardo Gómez', 'Carlos López'],
    'Vínculo': ['Hermana', 'Cuñado', 'Hijo']
})

# Simulación de Base de Contratos (BORA/COMPR.AR)
df_contratos = pd.DataFrame({
    'Empresa_Adjudicataria': ['Tech Soluciones SA', 'Limpieza Ya SRL', 'Carlos López']
})

print("--- ANALIZANDO MATRIZ DE PARENTESCO ---")
for _, row in df_parentesco.iterrows():
    alerta = check_nepotism_alert(row['Pariente'], df_contratos)
    if "ALERTA" in alerta:
        print(f"⚠️ {alerta}")
        print(f"   Funcionario: {row['Funcionario']} | Pariente: {row['Pariente']} ({row['Vínculo']})")
