const router = require('express').Router();
const controller = require('../controllers/recomendacionesController');

// Recomendaciones para un producto específico
router.get('/producto/:productId', controller.getByProduct);

// Recomendaciones para carrito de compras
router.post('/carrito', controller.getByCart);

// Estadísticas de Apriori
router.get('/stats', controller.getStats);

module.exports = router;
