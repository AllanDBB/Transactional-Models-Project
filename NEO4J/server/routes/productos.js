const express = require('express');
const router = express.Router();

const {
  upsertProducto,
  listProductos,
  linkEquivalente,
  getEquivalentes,
} = require('../services/productosService');

router.post('/', async (req, res) => {
  try {
    const data = await upsertProducto(req.body);
    res.status(201).json(data);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

router.post('/:id/equivalentes', async (req, res) => {
  const { equivalenteId } = req.body;
  if (!equivalenteId) {
    return res.status(400).json({ error: 'equivalenteId es requerido' });
  }

  try {
    const data = await linkEquivalente(req.params.id, equivalenteId);
    res.status(201).json(data);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

router.get('/:id/equivalentes', async (req, res) => {
  try {
    const equivalentes = await getEquivalentes(req.params.id);
    res.json({ id: req.params.id, equivalentes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/', async (_req, res) => {
  try {
    const productos = await listProductos();
    res.json(productos);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
