# Portal Anticorrupción - Jefatura de Gabinete

Este sistema automatiza la detección de conflictos de interés mediante tres matrices de inteligencia:

1. **Matriz de Parentesco**: Mapea consanguinidad y afinidad para detectar nepotismo.
2. **Matriz Societaria**: Conecta a funcionarios con empresas proveedoras (vía IGJ).
3. **Matriz de Flujo de Fondos**: Detecta sobreprecios comparando adjudicaciones (COMPR.AR) vs pagos (TGN).

## Visualización
El sistema genera un **Grafo de Nodos** donde:
* **Azul**: Funcionario.
* **Verde**: Empresa.
* **Rojo**: Vínculo familiar.
* **Amarillo**: Vínculo comercial.

## Cumplimiento
Se respeta la privacidad omitiendo datos sensibles como domicilios particulares, enfocándose en datos públicos (Sueldos, CUIT, Contratos).
