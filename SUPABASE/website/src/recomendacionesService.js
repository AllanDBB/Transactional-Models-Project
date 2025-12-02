// Servicio para consultar recomendaciones del DWH
// Nota: Esto requiere un proxy/backend que conecte a SQL Server
// ya que los navegadores no pueden conectarse directamente a MSSQL

const DWH_API = import.meta.env.VITE_DWH_API_URL || 'http://localhost:3001';

/**
 * Obtiene recomendaciones para un producto específico
 * @param {number} productId - ID del producto en el DWH
 * @param {number} topN - Número de recomendaciones (default 10)
 */
export async function getProductRecommendations(productId, topN = 10) {
  try {
    const res = await fetch(`${DWH_API}/api/recomendaciones/producto/${productId}?topN=${topN}`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } catch (err) {
    console.error('Error obteniendo recomendaciones:', err);
    return { success: false, error: err.message };
  }
}

/**
 * Obtiene recomendaciones para un carrito de compras
 * @param {number[]} productIds - Array de IDs de productos
 * @param {number} topN - Número de recomendaciones (default 10)
 */
export async function getCartRecommendations(productIds, topN = 10) {
  try {
    const res = await fetch(`${DWH_API}/api/recomendaciones/carrito`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ productIds, topN })
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } catch (err) {
    console.error('Error obteniendo recomendaciones del carrito:', err);
    return { success: false, error: err.message };
  }
}

/**
 * Obtiene estadísticas del análisis Apriori
 */
export async function getAprioriStats() {
  try {
    const res = await fetch(`${DWH_API}/api/recomendaciones/stats`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } catch (err) {
    console.error('Error obteniendo estadísticas:', err);
    return { success: false, error: err.message };
  }
}
