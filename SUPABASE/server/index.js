import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import recomendacionesRoutes from './routes/recomendaciones.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'Supabase API Server' });
});

// Routes
app.use('/api/recomendaciones', recomendacionesRoutes);

app.listen(PORT, () => {
  console.log(`✅ Supabase API Server escuchando en puerto ${PORT}`);
});
