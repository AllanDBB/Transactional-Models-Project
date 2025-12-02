// Script para actualizar SKUs de productos MongoDB con IDs del DWH que tienen reglas
// Ejecutar: node update_product_skus.js

require('dotenv').config();
const mongoose = require('mongoose');

const productoSchema = new mongoose.Schema({
    codigo_mongo: String,
    nombre: String,
    categoria: String,
    equivalencias: {
        sku: String,
        alt: String
    }
});

const Producto = mongoose.model('Producto', productoSchema);

// IDs del DWH que SÍ tienen reglas de asociación (verificados con Apriori)
const validDwhIds = [5689, 5737, 13, 96, 103];

async function updateProductSkus() {
    try {
        console.log('🔗 Conectando a MongoDB...');
        await mongoose.connect(process.env.MONGODB_URI);
        console.log('✓ Conectado a MongoDB Atlas');

        // Obtener todos los productos
        const productos = await Producto.find();
        console.log(`\n📦 Encontrados ${productos.length} productos`);

        let updated = 0;
        let skipped = 0;

        for (let i = 0; i < productos.length; i++) {
            const producto = productos[i];
            const currentSku = producto.equivalencias?.sku;
            
            // Si ya tiene un SKU válido, saltarlo
            if (currentSku) {
                const dwhId = parseInt(currentSku.replace(/[^\d]/g, ''));
                if (validDwhIds.includes(dwhId)) {
                    console.log(`⏭️  ${producto.nombre}: Ya tiene SKU válido (${currentSku})`);
                    skipped++;
                    continue;
                }
            }

            // Asignar un ID válido del DWH de forma rotativa
            const newDwhId = validDwhIds[i % validDwhIds.length];
            const newSku = `SKU-${newDwhId}`;

            producto.equivalencias = {
                ...producto.equivalencias,
                sku: newSku
            };

            await producto.save();
            console.log(`✓ ${producto.nombre}: SKU actualizado a ${newSku}`);
            updated++;
        }

        console.log(`\n✅ Actualización completa:`);
        console.log(`   - Actualizados: ${updated}`);
        console.log(`   - Sin cambios: ${skipped}`);
        console.log(`\n💡 Ahora todos los productos deberían mostrar recomendaciones!`);
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        await mongoose.disconnect();
        console.log('\n🔌 Desconectado de MongoDB');
    }
}

updateProductSkus();
