const { exec } = require('child_process');
const { promisify } = require('util');
const execAsync = promisify(exec);

/**
 * Ejecuta una query SQL en el DWH usando docker exec
 * @param {string} query - Query SQL a ejecutar
 * @returns {Promise<Array>} Resultado parseado
 */
async function executeQuery(query) {
  const fs = require('fs');
  const path = require('path');
  
  try {
    const containerFile = `/tmp/bcp_output_${Date.now()}.json`;
    const hostFile = path.join(__dirname, `../../temp_${Date.now()}.json`);
    
    // Preparar query con FOR JSON PATH
    const jsonQuery = `${query} FOR JSON PATH`.replace(/\n/g, ' ').replace(/"/g, '\\"');
    
    // Usar BCP (Bulk Copy Program) que NO tiene límite de 256 caracteres
    // queryout: exportar resultados de query
    // -c: formato carácter
    const bcpCmd = `docker exec sqlserver-dw /opt/mssql-tools/bin/bcp "${jsonQuery}" queryout ${containerFile} -c -S localhost -U sa -P "BasesDatos2!" -d MSSQL_DW`;
    
    await execAsync(bcpCmd, { maxBuffer: 1024 * 1024 * 10 });
    
    // Copiar archivo del contenedor al host
    await execAsync(`docker cp sqlserver-dw:${containerFile} "${hostFile}"`);
    
    // Leer archivo
    let jsonText = fs.readFileSync(hostFile, 'utf8').trim();
    
    // Limpiar archivos temporales
    fs.unlinkSync(hostFile);
    await execAsync(`docker exec sqlserver-dw rm ${containerFile}`).catch(() => {});
    
    if (!jsonText || jsonText === 'null' || jsonText === '[]') {
      return [];
    }
    
    // Parsear JSON resultado
    const result = JSON.parse(jsonText);
    return Array.isArray(result) ? result : [result];
    
  } catch (err) {
    console.error('❌ Error ejecutando query en DWH:', err.message);
    throw new Error(`Error en DWH: ${err.message}`);
  }
}

/**
 * GET /api/recomendaciones/producto/:productId?topN=10
 * Obtiene recomendaciones para un producto específico
 */
exports.getByProduct = async (req, res) => {
  try {
    const { productId } = req.params;
    const topN = parseInt(req.query.topN) || 10;

    // Query directa en lugar de SP para poder usar FOR JSON AUTO
    const query = `
      SELECT TOP (${topN})
        r.RuleID,
        r.ConsequentProductIds,
        r.ConsequentNames,
        r.Support,
        r.Confidence,
        r.Lift,
        r.FechaCalculo
      FROM dwh.ProductAssociationRules r
      WHERE (
        r.AntecedentProductIds = '${productId}'
        OR r.AntecedentProductIds LIKE '${productId},%'
        OR r.AntecedentProductIds LIKE '%,${productId},%'
        OR r.AntecedentProductIds LIKE '%,${productId}'
      )
      AND r.Activo = 1
      ORDER BY r.Lift DESC
    `;
    
    const result = await executeQuery(query);

    res.json({
      success: true,
      productId: parseInt(productId),
      recomendaciones: result
    });
  } catch (err) {
    console.error('Error obteniendo recomendaciones por producto:', err);
    res.status(500).json({
      success: false,
      error: 'Error al obtener recomendaciones',
      details: err.message
    });
  }
};

/**
 * POST /api/recomendaciones/carrito
 * Body: { productIds: [5689, 5737], topN: 10 }
 * Obtiene recomendaciones basadas en productos en el carrito
 */
exports.getByCart = async (req, res) => {
  try {
    const { productIds, topN = 10 } = req.body;

    if (!productIds || !Array.isArray(productIds) || productIds.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'Se requiere un array de productIds'
      });
    }

    const productIdsStr = productIds.join(',');
    const query = `EXEC dwh.sp_get_cart_recommendations @ProductIds='${productIdsStr}', @TopN=${topN}`;
    const result = await executeQuery(query);

    res.json({
      success: true,
      carritoProductIds: productIds,
      recomendaciones: result
    });
  } catch (err) {
    console.error('Error obteniendo recomendaciones por carrito:', err);
    res.status(500).json({
      success: false,
      error: 'Error al obtener recomendaciones del carrito',
      details: err.message
    });
  }
};

/**
 * GET /api/recomendaciones/stats
 * Obtiene estadísticas generales del análisis Apriori
 */
exports.getStats = async (req, res) => {
  try {
    const query = `EXEC dwh.sp_get_apriori_stats`;
    const result = await executeQuery(query);

    res.json({
      success: true,
      stats: result[0] || {}
    });
  } catch (err) {
    console.error('Error obteniendo estadísticas de Apriori:', err);
    res.status(500).json({
      success: false,
      error: 'Error al obtener estadísticas',
      details: err.message
    });
  }
};
