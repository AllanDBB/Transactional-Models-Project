const express = require('express');
const router = express.Router();

const {
  upsertCliente,
  listClientes,
  getOrdenesByCliente,
} = require('../services/clientesService');

router.post('/', async (req, res) => {
  try {
    const cliente = await upsertCliente(req.body);
    res.status(201).json(cliente);
  } catch (err) {
    res.status(400).json({ error: err.message });
  }
});

router.get('/:id/ordenes', async (req, res) => {
  try {
    const ordenes = await getOrdenesByCliente(req.params.id);
    res.json({ clienteId: req.params.id, ordenes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/', async (_req, res) => {
  try {
    const clientes = await listClientes();
    res.json(clientes);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
