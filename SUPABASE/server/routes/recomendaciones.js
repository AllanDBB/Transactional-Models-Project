import express from 'express';
import * as recomendacionesController from '../controllers/recomendacionesController.js';

const router = express.Router();

// GET /api/recomendaciones/producto/:productId?topN=10
router.get('/producto/:productId', recomendacionesController.getByProduct);

// POST /api/recomendaciones/carrito
router.post('/carrito', recomendacionesController.getByCart);

// GET /api/recomendaciones/stats
router.get('/stats', recomendacionesController.getStats);

export default router;
