const { driver } = require('./neo4j');

const ensureConstraints = async () => {
  const session = driver.session();
  const statements = [
    'CREATE CONSTRAINT cliente_id IF NOT EXISTS FOR (c:Cliente) REQUIRE c.id IS UNIQUE',
    'CREATE CONSTRAINT producto_id IF NOT EXISTS FOR (p:Producto) REQUIRE p.id IS UNIQUE',
    'CREATE INDEX orden_fecha IF NOT EXISTS FOR (o:Orden) ON (o.fecha)',
  ];

  try {
    for (const cypher of statements) {
      // Run sequentially to avoid lock contention on startup
      await session.run(cypher);
    }
  } finally {
    await session.close();
  }
};

module.exports = {
  ensureConstraints,
};
