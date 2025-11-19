const Orden = require('../models/ordenes');

const list = async (_req, res) => {
    try {
        const ordenes = await Orden.find().populate('client_id').populate('items.producto_id').lean();
        res.json(ordenes);
    } catch (err) {
        res.status(500).json({ message: 'Error listando ordenes', error: err.message });
    }
};

const getById = async (req, res) => {
    try {
        const orden = await Orden.findById(req.params.id).populate('client_id').populate('items.producto_id').lean();
        if (!orden) return res.status(404).json({ message: 'Orden no encontrada' });
        res.json(orden);
    } catch (err) {
        res.status(500).json({ message: 'Error obteniendo orden', error: err.message });
    }
};

const create = async (req, res) => {
    try {
        const nueva = await Orden.create(req.body);
        res.status(201).json(nueva);
    } catch (err) {
        res.status(400).json({ message: 'Error creando orden', error: err.message });
    }
};

const update = async (req, res) => {
    try {
        const actualizada = await Orden.findByIdAndUpdate(req.params.id, req.body, { new: true, runValidators: true })
            .populate('client_id')
            .populate('items.producto_id')
            .lean();
        if (!actualizada) return res.status(404).json({ message: 'Orden no encontrada' });
        res.json(actualizada);
    } catch (err) {
        res.status(400).json({ message: 'Error actualizando orden', error: err.message });
    }
};

const remove = async (req, res) => {
    try {
        const eliminada = await Orden.findByIdAndDelete(req.params.id).lean();
        if (!eliminada) return res.status(404).json({ message: 'Orden no encontrada' });
        res.status(204).end();
    } catch (err) {
        res.status(500).json({ message: 'Error eliminando orden', error: err.message });
    }
};

module.exports = { list, getById, create, update, remove };
