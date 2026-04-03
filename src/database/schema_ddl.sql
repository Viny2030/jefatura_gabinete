-- Tabla para Matriz de Parentesco [cite: 21, 23]
CREATE TABLE matriz_parentesco (
    id SERIAL PRIMARY KEY,
    funcionario_nombre VARCHAR(100),
    pariente_nombre VARCHAR(100),
    vinculo VARCHAR(50), -- Consanguinidad o Afinidad [cite: 21]
    fuente VARCHAR(100), -- ANSES, RRHH, Registro Civil [cite: 23]
    nivel_alerta VARCHAR(10) -- Alta, Media [cite: 23]
);

-- Tabla para Matriz Societaria [cite: 25, 28]
CREATE TABLE matriz_societaria (
    id SERIAL PRIMARY KEY,
    persona_nombre VARCHAR(100),
    empresa_cuit VARCHAR(20),
    rol VARCHAR(50), -- Presidente, Socio, Ex-Socio [cite: 28]
    paquete_accionario_pct DECIMAL(5,2) -- % de participación [cite: 28]
);
