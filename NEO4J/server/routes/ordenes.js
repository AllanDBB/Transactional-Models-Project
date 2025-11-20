const express = require('express');
const router = express.Router();

const { createOrden } = require('../services/ordenesService');

router.post('/', async (req, res) => {
  try {
    const orden = await createOrden(req.body);
    res.status(201).json(orden);
  } catch (err) {
    const status = err.message?.includes('requerido') ? 400 : 500;
    res.status(status).json({ error: err.message });
  }
});

module.exports = router;
