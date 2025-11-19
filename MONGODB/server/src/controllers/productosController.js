const Producto = require('../models/productos');

const list = async (_req, res) => {
    try {
        const productos = await Producto.find().lean();
        res.json(productos);
    } catch (err) {
        res.status(500).json({ message: 'Error listando productos', error: err.message });
    }
};

const getById = async (req, res) => {
    try {
        const producto = await Producto.findById(req.params.id).lean();
        if (!producto) return res.status(404).json({ message: 'Producto no encontrado' });
        res.json(producto);
    } catch (err) {
        res.status(500).json({ message: 'Error obteniendo producto', error: err.message });
    }
};

const create = async (req, res) => {
    try {
        const nuevo = await Producto.create(req.body);
        res.status(201).json(nuevo);
    } catch (err) {
        res.status(400).json({ message: 'Error creando producto', error: err.message });
    }
};

const update = async (req, res) => {
    try {
        const actualizado = await Producto.findByIdAndUpdate(req.params.id, req.body, { new: true, runValidators: true }).lean();
        if (!actualizado) return res.status(404).json({ message: 'Producto no encontrado' });
        res.json(actualizado);
    } catch (err) {
        res.status(400).json({ message: 'Error actualizando producto', error: err.message });
    }
};

const remove = async (req, res) => {
    try {
        const eliminado = await Producto.findByIdAndDelete(req.params.id).lean();
        if (!eliminado) return res.status(404).json({ message: 'Producto no encontrado' });
        res.status(204).end();
    } catch (err) {
        res.status(500).json({ message: 'Error eliminando producto', error: err.message });
    }
};

module.exports = { list, getById, create, update, remove };
