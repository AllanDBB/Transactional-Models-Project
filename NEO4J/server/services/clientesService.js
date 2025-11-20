const { getSession } = require('../db/neo4j');
const { toNative } = require('../utils/neo4j');

const upsertCliente = async ({ id, nombre, genero, pais }) => {
  const session = getSession();
  const query = `
    MERGE (c:Cliente {id: $id})
    ON CREATE SET c.nombre = $nombre, c.genero = $genero, c.pais = $pais
    ON MATCH SET
      c.nombre = coalesce($nombre, c.nombre),
      c.genero = coalesce($genero, c.genero),
      c.pais = coalesce($pais, c.pais)
    RETURN c
  `;

  try {
    const result = await session.run(query, { id, nombre, genero, pais });
    const record = result.records[0];
    if (!record) return null;
    return toNative(record.get('c').properties);
  } finally {
    await session.close();
  }
};

const listClientes = async () => {
  const session = getSession();
  const query = 'MATCH (c:Cliente) RETURN c ORDER BY c.nombre';

  try {
    const result = await session.run(query);
    return result.records.map((record) => toNative(record.get('c').properties));
  } finally {
    await session.close();
  }
};

const getOrdenesByCliente = async (id) => {
  const session = getSession();
  const query = `
    MATCH (c:Cliente {id: $id})-[:REALIZO]->(o:Orden)
    OPTIONAL MATCH (o)-[cont:CONTIENTE]->(p:Producto)
    OPTIONAL MATCH (p)-[:PERTENECE_A]->(cat:Categoria)
    RETURN o, collect({producto: p, categoria: cat, cantidad: cont.cantidad, precio_unit: cont.precio_unit}) AS items
    ORDER BY o.fecha DESC
  `;

  try {
    const result = await session.run(query, { id });
    return result.records.map((record) => {
      const order = record.get('o');
      const items = record.get('items');

      return {
        ...toNative(order.properties),
        items: items
          .filter((item) => item.producto)
          .map((item) => ({
            producto: toNative(item.producto.properties),
            categoria: item.categoria ? toNative(item.categoria.properties) : null,
            cantidad: toNative(item.cantidad),
            precio_unit: toNative(item.precio_unit),
          })),
      };
    });
  } finally {
    await session.close();
  }
};

module.exports = {
  upsertCliente,
  listClientes,
  getOrdenesByCliente,
};
