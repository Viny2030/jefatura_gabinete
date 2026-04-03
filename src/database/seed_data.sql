-- Carga de Funcionarios y Parientes (Matriz 1)
INSERT INTO matriz_parentesco (funcionario_nombre, pariente_nombre, vinculo, fuente, nivel_alerta)
VALUES 
('Juan Pérez', 'María Pérez', 'Hermana', 'ANSES', 'Alta'),
('Juan Pérez', 'Ricardo Gómez', 'Cuñado', 'Registro Civil', 'Media'),
('Ana López', 'Carlos López', 'Hijo', 'DDJJ', 'Alta'); [cite: 23]

-- Carga de Vínculos Societarios (Matriz 2)
INSERT INTO matriz_societaria (persona_nombre, empresa_cuit, rol, paquete_accionario_pct)
VALUES 
('Ricardo Gómez', '30-71234567-8', 'Presidente', 50.00),
('Juan Pérez', '30-99887766-5', 'Ex-Socio', 0.00); [cite: 28]
