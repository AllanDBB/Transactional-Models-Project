require('dotenv').config();
const express = require('express');
const path = require('path');
const cors = require('cors');

const app = express();
app.use(express.json());
app.use(cors());

// Servir archivos estáticos
const staticDir = path.join(__dirname, '..', '..', 'client', 'public');
app.use(express.static(staticDir));

// Rutas API
app.use('/api/mssql', require('./routes/mssqlRoutes'));
app.use('/api/mysql', require('./routes/mysqlRoutes'));

// Health check
app.get('/health', (_req, res) => res.send('ok'));

// Ruta principal
app.get('/', (_req, res) => {
    res.sendFile(path.join(staticDir, 'index.html'));
});

const port = process.env.PORT || 3001;
app.listen(port, () => {
    console.log(`========================================`);
    console.log(`🚀 Servidor MSSQL corriendo en puerto ${port}`);
    console.log(`📊 Panel: http://localhost:${port}`);
    console.log(`💚 Health: http://localhost:${port}/health`);
    console.log(`========================================`);
});
