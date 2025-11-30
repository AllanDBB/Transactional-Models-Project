const { randomUUID } = require('crypto');
const { getSession } = require('../db/neo4j');
const { toNative } = require('../utils/neo4j');

const defaultId = (prefix) => `${prefix}-${randomUUID().slice(0, 8)}`;

const parseFecha = (raw) => {
  if (!raw) return new Date();
  if (raw instanceof Date) return raw;
  // Epoch num o string num
  if (typeof raw === 'number' || (/^\\d+$/.test(raw))) {
    const d = new Date(Number(raw));
    if (!Number.isNaN(d.getTime())) return d;
  }
  if (typeof raw === 'string') {
    // Intento directo
    let d = new Date(raw);
    if (!Number.isNaN(d.getTime())) return d;
    // Reemplazo guiones por slashes (ej 2024-12-01 -> 2024/12/01)
    d = new Date(raw.replace(/-/g, '/'));
    if (!Number.isNaN(d.getTime())) return d;
  }
  return new Date();
};

const normalizeOrdenPayload = ({ cliente = {}, orden = {}, items = [] }) => {
  const clienteNorm = {
    id: cliente.id || cliente.clienteId || defaultId('cli'),
    nombre: cliente.nombre || cliente.name || null,
    genero: cliente.genero || null,
    pais: cliente.pais || cliente.country || null,
  };

  const ordenNorm = {
    id: orden.id || orden.ordenId || defaultId('ord'),
    fecha: parseFecha(orden.fecha).toISOString(),
    canal: orden.canal || 'WEB',
    moneda: orden.moneda || 'CRC',
    total: Number(orden.total || 0),
  };

  const itemsNorm = (items || []).map((it) => ({
    productoId: it.productoId || it.id || it.sku || defaultId('prod'),
    nombre: it.nombre || it.productoNombre || null,
    sku: it.sku || null,
    categoria: it.categoria || null,
    cantidad: Number(it.cantidad || 1),
    precio_unit: Number(it.precio_unit || it.precioUnit || it.precio || 0),
  }));

  return { cliente: clienteNorm, orden: ordenNorm, items: itemsNorm };
};

const createOrden = async (payload) => {
  const normalized = normalizeOrdenPayload(payload || {});

  const session = getSession();
  const query = `
    MERGE (c:Cliente {id: $cliente.id})
    ON CREATE SET c.nombre = $cliente.nombre, c.genero = $cliente.genero, c.pais = $cliente.pais
    ON MATCH SET
      c.nombre = coalesce($cliente.nombre, c.nombre),
      c.genero = coalesce($cliente.genero, c.genero),
      c.pais = coalesce($cliente.pais, c.pais)
    WITH c
    MERGE (o:Orden {id: $orden.id})
    SET o.fecha = datetime($orden.fecha),
        o.canal = $orden.canal,
        o.moneda = $orden.moneda,
        o.total = $orden.total
    MERGE (c)-[:REALIZO]->(o)
    WITH o, $items AS items
    UNWIND items AS item
      MERGE (p:Producto {id: item.productoId})
      SET p.nombre = coalesce(item.nombre, p.nombre),
          p.sku = coalesce(item.sku, p.sku),
          p.categoria = coalesce(item.categoria, p.categoria)
      MERGE (o)-[:CONTIENTE {cantidad: item.cantidad, precio_unit: item.precio_unit}]->(p)
    RETURN o
  `;

  try {
    const result = await session.run(query, normalized);
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
