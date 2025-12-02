/**
 * Script para actualizar los SKUs de productos en Supabase
 * con ProductIds del DWH que tienen recomendaciones
 * 
 * Ejecutar: node update_product_skus.js
 */

import { createClient } from '@supabase/supabase-js';
import { exec } from 'child_process';
import { promisify } from 'util';
import dotenv from 'dotenv';

dotenv.config();

const execAsync = promisify(exec);

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://vzcwfryxmtzocmjpayfz.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ6Y3dmcnl4bXR6b2NtanBheWZ6Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2NDg2NDQsImV4cCI6MjA3ODIyNDY0NH0.Azkwt-2uzwOVql0Cv-b0juvCK5ZPs7A-HT9QsPfGcWg';

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function getProductsWithRules() {
  console.log('📊 Consultando productos con recomendaciones en el DWH...');
  
  const query = `SELECT DISTINCT AntecedentProductIds FROM dwh.ProductAssociationRules WHERE Activo = 1 ORDER BY AntecedentProductIds`;
  
  const cmd = `docker exec sqlserver-dw /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "BasesDatos2!" -d MSSQL_DW -Q "${query}" -h -1 -W`;
  
  const { stdout } = await execAsync(cmd);
  
  // Debug: ver qué está devolviendo
  console.log('Raw output:', stdout.substring(0, 200));
  
  const productIds = stdout
    .trim()
    .split('\n')
    .map(line => line.trim())
    .filter(line => {
      const num = parseInt(line);
      return !isNaN(num) && num > 0;
    })
    .map(id => parseInt(id));
  
  console.log(`✅ Encontrados ${productIds.length} productos con recomendaciones`);
  console.log(`Primeros 10:`, productIds.slice(0, 10));
  return productIds;
}

async function updateSupabaseProducts() {
  try {
    // Obtener productos con recomendaciones del DWH
    const dwhProductIds = await getProductsWithRules();
    
    if (dwhProductIds.length === 0) {
      console.log('❌ No hay productos con recomendaciones en el DWH');
      return;
    }
    
    // Obtener productos de Supabase
    console.log('\n📦 Obteniendo productos de Supabase...');
    const { data: productos, error } = await supabase
      .from('producto')
      .select('producto_id, nombre, sku')
      .order('producto_id');
    
    if (error) {
      console.error('❌ Error al obtener productos:', error);
      return;
    }
    
    console.log(`✅ Encontrados ${productos.length} productos en Supabase`);
    
    // Asignar ProductIds en round-robin
    let updated = 0;
    for (let i = 0; i < productos.length; i++) {
      const producto = productos[i];
      const dwhProductId = dwhProductIds[i % dwhProductIds.length];
      
      // Actualizar SKU
      const { error: updateError } = await supabase
        .from('producto')
        .update({ sku: dwhProductId.toString() })
        .eq('producto_id', producto.producto_id);
      
      if (updateError) {
        console.error(`❌ Error actualizando producto ${producto.producto_id}:`, updateError);
      } else {
        console.log(`✅ Producto "${producto.nombre}" → SKU: ${dwhProductId}`);
        updated++;
      }
    }
    
    console.log(`\n🎉 Actualización completa: ${updated}/${productos.length} productos actualizados`);
    
  } catch (err) {
    console.error('❌ Error:', err.message);
  }
}

// Ejecutar
updateSupabaseProducts();
