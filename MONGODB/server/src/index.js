require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const path = require('path');

const app = express();
app.use(express.json());

const staticDir = path.join(__dirname, '..', '..', 'client', 'public');
app.use(express.static(staticDir));

// Conexion a MongoDB
const uri = process.env.MONGODB_URI;
mongoose.connect(uri)
  .then(() => console.log('MongoDB conectada'))
  .catch(err => {
    console.error('Error conectando a MongoDB', err);
    process.exit(1);
  });

// Rutas API
app.use('/api/clientes', require('./routes/clienteRoutes'));
app.use('/api/productos', require('./routes/productoRoutes'));
app.use('/api/ordenes', require('./routes/ordenRoutes'));
app.use('/api/recomendaciones', require('./routes/recomendacionesRoutes'));

app.get('/health', (_req, res) => res.send('ok'));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`API escuchando en puerto ${port}`));
