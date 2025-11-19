const Cliente = require('../models/clientes');

const list = async (_req, res) => {
    try {
        const clientes = await Cliente.find().lean();
        res.json(clientes);
    } catch (err) {
        res.status(500).json({ message: 'Error listando clientes', error: err.message });
    }
};

const getById = async (req, res) => {
    try {
        const cliente = await Cliente.findById(req.params.id).lean();
        if (!cliente) return res.status(404).json({ message: 'Cliente no encontrado' });
        res.json(cliente);
    } catch (err) {
        res.status(500).json({ message: 'Error obteniendo cliente', error: err.message });
    }
};

const create = async (req, res) => {
    try {
        const nuevo = await Cliente.create(req.body);
        res.status(201).json(nuevo);
    } catch (err) {
        res.status(400).json({ message: 'Error creando cliente', error: err.message });
    }
};

const update = async (req, res) => {
    try {
        const actualizado = await Cliente.findByIdAndUpdate(req.params.id, req.body, { new: true, runValidators: true }).lean();
        if (!actualizado) return res.status(404).json({ message: 'Cliente no encontrado' });
        res.json(actualizado);
    } catch (err) {
        res.status(400).json({ message: 'Error actualizando cliente', error: err.message });
    }
};

const remove = async (req, res) => {
    try {
        const eliminado = await Cliente.findByIdAndDelete(req.params.id).lean();
        if (!eliminado) return res.status(404).json({ message: 'Cliente no encontrado' });
        res.status(204).end();
    } catch (err) {
        res.status(500).json({ message: 'Error eliminando cliente', error: err.message });
    }
};

module.exports = { list, getById, create, update, remove };
