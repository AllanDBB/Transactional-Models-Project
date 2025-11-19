require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');

const app = express();
app.use(express.json()); 

// Conexión a MongoDB
const uri = process.env.MONGODB_URI;
mongoose.connect(uri)
  .then(() => console.log('MongoDB conectada'))
  .catch(err => {
    console.error('Error conectando a MongoDB', err);
    process.exit(1);
  });

app.get('/health', (_req, res) => res.send('ok'));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`API escuchando en puerto ${port}`));
