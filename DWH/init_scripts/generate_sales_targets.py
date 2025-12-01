#!/usr/bin/env python3
"""
Genera metas de ventas (MetasVentas) basadas en histórico de ventas.
Las metas se calculan como: ventas_mes_anterior * 1.1 (10% de crecimiento).
"""

import logging
from datetime import datetime
from db_utils import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_sales_targets():
    """Generar metas de ventas para MetasVentas basado en histórico"""
    
    logger.info("="*60)
    logger.info("GENERANDO METAS DE VENTAS")
    logger.info("="*60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            
            # Limpiar tabla de metas
            logger.info("\n🗑️  Limpiando dwh.MetasVentas...")
            cur.execute("DELETE FROM dwh.MetasVentas")
            conn.commit()
            
            # Generar metas basadas en ventas históricas (2024)
            # Meta = Ventas del mes anterior en 2024 * 1.15 (15% crecimiento para 2025)
            logger.info("\n📊 Generando metas 2025 basadas en histórico 2024...")
            
            cur.execute("""
                INSERT INTO dwh.MetasVentas (customerId, productId, Anio, Mes, MetaUSD)
                SELECT 
                    fs.customerId,
                    fs.productId,
                    2025 as Anio,
                    t.month as Mes,
                    SUM(fs.lineTotalUSD) * 1.15 as MetaUSD
                FROM dwh.FactSales fs
                INNER JOIN dwh.DimTime t ON t.id = fs.timeId
                WHERE t.year = 2024
                GROUP BY fs.customerId, fs.productId, t.month
                HAVING SUM(fs.lineTotalUSD) > 0
            """)
            count_2025 = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count_2025:,} metas generadas para 2025")
            
            # Generar también metas para 2024 (basadas en ventas reales * 0.9)
            logger.info("\n📊 Generando metas 2024 (retrospectivas)...")
            cur.execute("""
                INSERT INTO dwh.MetasVentas (customerId, productId, Anio, Mes, MetaUSD)
                SELECT 
                    fs.customerId,
                    fs.productId,
                    2024 as Anio,
                    t.month as Mes,
                    SUM(fs.lineTotalUSD) * 0.9 as MetaUSD
                FROM dwh.FactSales fs
                INNER JOIN dwh.DimTime t ON t.id = fs.timeId
                WHERE t.year = 2024
                GROUP BY fs.customerId, fs.productId, t.month
                HAVING SUM(fs.lineTotalUSD) > 0
            """)
            count_2024 = cur.rowcount
            conn.commit()
            logger.info(f"✓ {count_2024:,} metas generadas para 2024")
            
            # Verificación
            logger.info("\n" + "="*60)
            logger.info("VERIFICACIÓN")
            logger.info("="*60)
            
            cur.execute("""
                SELECT 
                    Anio,
                    COUNT(*) as num_metas,
                    SUM(MetaUSD) as total_meta
                FROM dwh.MetasVentas
                GROUP BY Anio
                ORDER BY Anio
            """)
            
            for row in cur.fetchall():
                anio, num_metas, total_meta = row
                logger.info(f"   • {anio}: {num_metas:>8,} metas = ${total_meta:>15,.2f} USD")
            
            # Top 5 clientes con mayores metas
            logger.info("\n📈 Top 5 clientes con mayores metas 2025:")
            cur.execute("""
                SELECT TOP 5
                    c.name,
                    SUM(m.MetaUSD) as meta_total
                FROM dwh.MetasVentas m
                INNER JOIN dwh.DimCustomer c ON c.id = m.customerId
                WHERE m.Anio = 2025
                GROUP BY c.name
                ORDER BY meta_total DESC
            """)
            
            for idx, row in enumerate(cur.fetchall(), 1):
                nombre, meta = row
                logger.info(f"   {idx}. {nombre[:30]:<30} ${meta:>12,.2f}")
            
            logger.info("\n✅ GENERACIÓN DE METAS COMPLETADA")


if __name__ == "__main__":
    inicio = datetime.now()
    try:
        generate_sales_targets()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"\n⏱️  Duración: {duracion:.1f}s")
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
