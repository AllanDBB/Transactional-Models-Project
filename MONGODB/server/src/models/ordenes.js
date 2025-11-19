const {Schema , model} = require('mongoose');

// CRC ordenSchema

const ordenSchema = new Schema({

    client_id: { type: Schema.Types.ObjectId, ref: 'Cliente', required: true },
    fecha: { type: Date, default: Date.now },
    canal: { type: String, enum: ['Online', 'Tienda', 'Telefono'], required: true },
    moneda: { type: String, enum: ['CRC'], required: true },
    // Montos en CRC enteros; no se permiten decimales.
    total: { type: Number, required: true, validate: { validator: Number.isInteger, message: 'total debe ser entero' } },
    items: [{
        producto_id: { type: Schema.Types.ObjectId, ref: 'Producto', required: true },
        cantidad: { type: Number, required: true, validate: { validator: Number.isInteger, message: 'cantidad debe ser entera' } },
        precio_unit: { type: Number, required: true, validate: { validator: Number.isInteger, message: 'precio_unit debe ser entero' } }
    }],
    metadatos: { cupon: { type: String } }
});

module.exports = model('Orden', ordenSchema);
