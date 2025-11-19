const { Schema, model } = require('mongoose');

const clienteSchema = new Schema({
    nombre: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    genero: { type: String, enum: ['Masculino', 'Femenino', 'Otro'], required: true },
    preferencias: {
        canal: { type: String, enum: ['WEB', 'TIENDA'] }
    },
    creado: { type: Date, default: Date.now }
});

module.exports = model('Cliente', clienteSchema);

