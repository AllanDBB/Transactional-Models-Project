const mongoose = require('mongoose');
const Orden = require('../models/ordenes');
const Producto = require('../models/productos');

async function normalizeItems(items = []) {
    const normalized = [];
    for (const it of items) {
        let productoId = it.producto_id;
        // Si ya es un ObjectId válido, usamos tal cual
        if (mongoose.Types.ObjectId.isValid(productoId)) {
            normalized.push({ ...it, producto_id: productoId });
            continue;
        }
        // Intentar resolver por _id string o códigos alternos (codigo_mongo, sku, alt/codigo_alt)
        const prod = await Producto.findOne({
            $or: [
                { _id: productoId },
                { codigo_mongo: productoId },
                { 'equivalencias.sku': productoId },
                { 'equivalencias.codigo_alt': productoId },
                { 'equivalencias.alt': productoId },
            ],
        }).lean();

        if (!prod) {
            throw new Error(`Producto no encontrado para identificador '${productoId}'`);
        }
        normalized.push({ ...it, producto_id: prod._id });
    }
    return normalized;
}

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
        const payload = { ...req.body, items: await normalizeItems(req.body.items) };
        const nueva = await Orden.create(payload);
        res.status(201).json(nueva);
    } catch (err) {
        res.status(400).json({ message: 'Error creando orden', error: err.message });
    }
};

const update = async (req, res) => {
    try {
        const payload = { ...req.body, items: await normalizeItems(req.body.items) };
        const actualizada = await Orden.findByIdAndUpdate(req.params.id, payload, { new: true, runValidators: true })
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
