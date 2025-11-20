const { getSession } = require('../db/neo4j');
const { toNative } = require('../utils/neo4j');

const upsertProducto = async ({
  id,
  nombre,
  categoria,
  sku,
  codigo_alt,
  codigo_mongo,
}) => {
  if (!id) {
    throw new Error('producto.id es requerido');
  }

  const session = getSession();
  const query = `
    MERGE (cat:Categoria {nombre: coalesce($categoria, 'Sin categoria')})
    MERGE (p:Producto {id: $id})
    SET
      p.nombre = $nombre,
      p.categoria = $categoria,
      p.sku = $sku,
      p.codigo_alt = $codigo_alt,
      p.codigo_mongo = $codigo_mongo
    MERGE (p)-[:PERTENECE_A]->(cat)
    RETURN p, cat
  `;

  try {
    const result = await session.run(query, {
      id,
      nombre,
      categoria,
      sku,
      codigo_alt,
      codigo_mongo,
    });

    const record = result.records[0];
    if (!record) return null;
    return {
      producto: toNative(record.get('p').properties),
      categoria: toNative(record.get('cat').properties),
    };
  } finally {
    await session.close();
  }
};

const listProductos = async () => {
  const session = getSession();
  const query = `
    MATCH (p:Producto)-[:PERTENECE_A]->(cat:Categoria)
    RETURN p, cat
    ORDER BY p.nombre
  `;

  try {
    const result = await session.run(query);
    return result.records.map((record) => ({
      producto: toNative(record.get('p').properties),
      categoria: toNative(record.get('cat').properties),
    }));
  } finally {
    await session.close();
  }
};

const linkEquivalente = async (id, equivalenteId) => {
  const session = getSession();
  const query = `
    MATCH (p:Producto {id: $id}), (eq:Producto {id: $equivalenteId})
    MERGE (p)-[:EQUIVALE_A]->(eq)
    MERGE (eq)-[:EQUIVALE_A]->(p)
    RETURN p, eq
  `;

  try {
    const result = await session.run(query, { id, equivalenteId });
    const record = result.records[0];
    if (!record) return null;
    return {
      producto: toNative(record.get('p').properties),
      equivalente: toNative(record.get('eq').properties),
    };
  } finally {
    await session.close();
  }
};

const getEquivalentes = async (id) => {
  const session = getSession();
  const query = `
    MATCH (p:Producto {id: $id})-[:EQUIVALE_A*1..2]-(eq:Producto)
    RETURN DISTINCT eq
  `;

  try {
    const result = await session.run(query, { id });
    return result.records.map((record) => toNative(record.get('eq').properties));
  } finally {
    await session.close();
  }
};

module.exports = {
  upsertProducto,
  listProductos,
  linkEquivalente,
  getEquivalentes,
};
