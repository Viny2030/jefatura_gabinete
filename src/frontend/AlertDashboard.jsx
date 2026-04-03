const AlertItem = ({ id, empresa, desvio }) => (
  <div style={{ border: '1px solid red', padding: '10px', margin: '5px' }}>
    <p><strong>ID Contrato:</strong> {id} [cite: 32]</p>
    <p><strong>Empresa:</strong> {empresa} [cite: 32]</p>
    <p style={{ color: 'red' }}><strong>Desvío:</strong> {desvio}% - RED FLAG [cite: 32]</p>
  </div>
);
