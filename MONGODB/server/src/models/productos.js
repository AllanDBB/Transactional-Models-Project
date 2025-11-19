const { Schema, model } = require('mongoose');

const productoSchema = new Schema({

    codigo_mongo: { type: String, required: true, unique: true }, // Example: MN-9981,
    nombre: { type: String, required: true },
    categoria: { type: String, required: true },
    equivalencias: {
        sku: { type: String }, // Example: SKU-9981
        alt: { type: String } // Example: ALT-9981
    }
});

module.exports = model('Producto', productoSchema);
