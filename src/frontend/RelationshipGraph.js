/**
 * Lógica de Visualización de Grafos - Jefatura de Gabinete
 * Representación de vínculos entre Sector Público y Privado.
 */

const graphData = {
  "nodes": [
    { "id": "Juan Pérez", "type": "Funcionario", "color": "azul" },
    { "id": "Ana López", "type": "Funcionario", "color": "azul" },
    { "id": "Ricardo Gómez", "type": "Familiar/Socio", "color": "rojo" },
    { "id": "Carlos López", "type": "Familiar/Proveedor", "color": "rojo" },
    { "id": "Tech Soluciones SA", "type": "Empresa", "color": "verde" }
  ],
  "links": [
    { "source": "Juan Pérez", "target": "Ricardo Gómez", "type": "Familiar", "label": "Cuñado", "color": "rojo" },
    { "source": "Ricardo Gómez", "target": "Tech Soluciones SA", "type": "Comercial", "label": "Presidente", "color": "amarillo" },
    { "source": "Ana López", "target": "Carlos López", "type": "Familiar", "label": "Hijo", "color": "rojo" }
  ]
};

console.log("Grafo de Relaciones cargado con éxito.");
console.log("Detectando líneas rojas (Familia) y amarillas (Comercio)...");
