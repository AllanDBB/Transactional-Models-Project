const { getSession } = require('../db/neo4j');
const { toNative } = require('../utils/neo4j');

const createOrden = async ({ cliente, orden, items = [] }) => {
  if (!cliente?.id) {
    throw new Error('cliente.id es requerido');
  }
  if (!orden?.id) {
    throw new Error('orden.id es requerido');
  }

  const session = getSession();
  const query = `
    MERGE (c:Cliente {id: $cliente.id})
    ON CREATE SET c.nombre = $cliente.nombre, c.genero = $cliente.genero, c.pais = $cliente.pais
    ON MATCH SET
      c.nombre = coalesce($cliente.nombre, c.nombre),
      c.genero = coalesce($cliente.genero, c.genero),
      c.pais = coalesce($cliente.pais, c.pais)
    WITH c
    CREATE (o:Orden {
      id: $orden.id,
      fecha: datetime($orden.fecha),
      canal: $orden.canal,
      moneda: $orden.moneda,
      total: $orden.total
    })
    MERGE (c)-[:REALIZO]->(o)
    WITH o, $items AS items
    FOREACH (item IN items |
      MATCH (p:Producto {id: item.productoId})
      MERGE (o)-[:CONTIENTE {cantidad: item.cantidad, precio_unit: item.precio_unit}]->(p)
    )
    RETURN o
  `;

  try {
    const result = await session.run(query, { cliente, orden, items });
    const record = result.records[0];
    if (!record) return null;
    return toNative(record.get('o').properties);
  } finally {
    await session.close();
  }
};

module.exports = {
  createOrden,
};
