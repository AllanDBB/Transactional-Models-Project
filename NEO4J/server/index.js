require('dotenv').config();
const path = require('path');
const express = require('express');
const cors = require('cors');

const { ensureConstraints } = require('./db/setup');
const clientesRoutes = require('./routes/clientes');
const productosRoutes = require('./routes/productos');
const ordenesRoutes = require('./routes/ordenes');

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

app.use('/api/clientes', clientesRoutes);
app.use('/api/productos', productosRoutes);
app.use('/api/ordenes', ordenesRoutes);

// Servir el sitio estático (panel) desde /client
const staticDir = path.join(__dirname, '..', 'client');
app.use(express.static(staticDir));
app.get('/', (_req, res) => res.sendFile(path.join(staticDir, 'index.html')));

const PORT = process.env.PORT || 4000;

ensureConstraints()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`API Neo4j escuchando en puerto ${PORT}`);
      console.log(`Panel en http://localhost:${PORT}/`);
    });
  })
  .catch((err) => {
    console.error('Error inicializando constraints de Neo4j', err);
    process.exit(1);
  });
