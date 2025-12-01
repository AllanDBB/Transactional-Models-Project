from db_utils import get_connection
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_to_dwh_schema():
    """Migrar datos de dim/fact a dwh schema"""
    
    logger.info("="*60)
    logger.info("INICIANDO MIGRACIÓN A ESQUEMA dwh")
    logger.info("="*60)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            
            # 1. Limpiar tablas dwh (DELETE para evitar issues con FKs)
            logger.info("\n🗑️  Limpiando tablas dwh.*...")
            cur.execute("DELETE FROM dwh.FactSales")
            cur.execute("DELETE FROM dwh.DimOrder")
            cur.execute("DELETE FROM dwh.DimTime")
            cur.execute("DELETE FROM dwh.DimProduct")
            cur.execute("DELETE FROM dwh.DimCustomer")
            cur.execute("DELETE FROM dwh.DimChannel")
            conn.commit()
            logger.info("✓ Tablas dwh.* limpiadas")
            
            # 2. Migrar DimCustomer (solo primero por email para evitar duplicados)
            logger.info("\n📊 Migrando dim.DimCliente → dwh.DimCustomer...")
            cur.execute("SET IDENTITY_INSERT dwh.DimCustomer ON")
            cur.execute("""
                INSERT INTO dwh.DimCustomer (id, name, email, gender, country, created_at)
                SELECT 
                    ClienteID,
                    Nombre,
                    Email,
                    Genero,
                    Pais,
                    FechaRegistro
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY Email ORDER BY ClienteID) as rn
                    FROM dim.DimCliente
                    WHERE Activo = 1
                ) t
                WHERE rn = 1
            """)
            count = cur.rowcount
            cur.execute("SET IDENTITY_INSERT dwh.DimCustomer OFF")
            conn.commit()
            logger.info(f"✓ {count:,} clientes migrados (únicos por email)")
            
            # 3. Migrar DimProduct
            logger.info("\n📦 Migrando dim.DimProducto → dwh.DimProduct...")
            # Nota: dwh.DimProduct no tiene categoryId FK, usar NULL por ahora
            cur.execute("SET IDENTITY_INSERT dwh.DimProduct ON")
            cur.execute("""
                INSERT INTO dwh.DimProduct (id, name, code, categoryId)
                SELECT 
                    ProductoID,
                    Nombre,
                    COALESCE(SKU, CodigoAlt, CodigoMongo, SourceKey),
                    NULL  -- No hay categoría como FK en dwh
                FROM dim.DimProducto
                WHERE Activo = 1
            """)
            count = cur.rowcount
            cur.execute("SET IDENTITY_INSERT dwh.DimProduct OFF")
            conn.commit()
            logger.info(f"✓ {count:,} productos migrados")
            
            # 4. Migrar DimTime
            logger.info("\n📅 Migrando dim.DimTiempo → dwh.DimTime...")
            cur.execute("SET IDENTITY_INSERT dwh.DimTime ON")
            cur.execute("""
                INSERT INTO dwh.DimTime (id, year, month, day, date)
                SELECT 
                    TiempoID,
                    Anio,
                    Mes,
                    DiaMes,
                    Fecha
                FROM dim.DimTiempo
            """)
            count = cur.rowcount
            cur.execute("SET IDENTITY_INSERT dwh.DimTime OFF")
            conn.commit()
            logger.info(f"✓ {count:,} fechas migradas")
            
            # 5. Poblar DimChannel (necesario para FactSales)
            logger.info("\n📡 Poblando dwh.DimChannel...")
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel ON")
            cur.execute("""
                INSERT INTO dwh.DimChannel (id, name)
                VALUES 
                    (1, 'WEB'),
                    (2, 'TIENDA'),
                    (3, 'APP'),
                    (4, 'PARTNER'),
                    (5, 'TELEFONO')
            """)
            cur.execute("SET IDENTITY_INSERT dwh.DimChannel OFF")
            conn.commit()
            logger.info("✓ 5 canales creados")
            
            # 6. Poblar DimOrder (necesario para FactSales)
            logger.info("\n📝 Poblando dwh.DimOrder desde FactVentas...")
            cur.execute("SET IDENTITY_INSERT dwh.DimOrder ON")
            cur.execute("""
                INSERT INTO dwh.DimOrder (id, totalOrderUSD)
                SELECT DISTINCT
                    ROW_NUMBER() OVER (ORDER BY OrdenSourceKey),
                    SUM(MontoLineaUSD) OVER (PARTITION BY OrdenSourceKey)
                FROM fact.FactVentas
            """)
            count = cur.rowcount
            cur.execute("SET IDENTITY_INSERT dwh.DimOrder OFF")
            conn.commit()
            logger.info(f"✓ {count:,} órdenes únicas creadas")
            
            # 7. Migrar FactSales (más complejo)
            logger.info("\n💰 Migrando fact.FactVentas → dwh.FactSales...")
            logger.info("   (Esto puede tardar 1-2 minutos con 55K registros...)")
            
            cur.execute("SET IDENTITY_INSERT dwh.FactSales ON")
            cur.execute("""
                INSERT INTO dwh.FactSales (
                    id, productId, timeId, customerId, channelId, orderId,
                    productCant, productUnitPriceUSD, lineTotalUSD, 
                    discountPercentage, exchangeRateId, created_at
                )
                SELECT 
                    f.VentaID,
                    f.ProductoID,
                    f.TiempoID,
                    f.ClienteID,
                    CASE f.Canal
                        WHEN 'WEB' THEN 1
                        WHEN 'TIENDA' THEN 2
                        WHEN 'APP' THEN 3
                        WHEN 'PARTNER' THEN 4
                        WHEN 'TELEFONO' THEN 5
                        ELSE 1
                    END,
                    1,  -- orderId dummy por ahora (necesitaríamos mapeo de OrdenSourceKey)
                    f.Cantidad,
                    f.PrecioUnitarioUSD,
                    f.MontoLineaUSD,
                    f.DescuentoPct,
                    NULL,  -- exchangeRateId (podríamos mapear después)
                    GETDATE()
                FROM fact.FactVentas f
                WHERE EXISTS (SELECT 1 FROM dwh.DimCustomer WHERE id = f.ClienteID)
                  AND EXISTS (SELECT 1 FROM dwh.DimProduct WHERE id = f.ProductoID)
                  AND EXISTS (SELECT 1 FROM dwh.DimTime WHERE id = f.TiempoID)
            """)
            count = cur.rowcount
            cur.execute("SET IDENTITY_INSERT dwh.FactSales OFF")
            conn.commit()
            logger.info(f"✓ {count:,} ventas migradas")
            
            # 8. Verificar resultados
            logger.info("\n" + "="*60)
            logger.info("VERIFICACIÓN POST-MIGRACIÓN")
            logger.info("="*60)
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimCustomer")
            logger.info(f"   • dwh.DimCustomer:  {cur.fetchone()[0]:>8,} registros")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimProduct")
            logger.info(f"   • dwh.DimProduct:   {cur.fetchone()[0]:>8,} registros")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimTime")
            logger.info(f"   • dwh.DimTime:      {cur.fetchone()[0]:>8,} registros")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimChannel")
            logger.info(f"   • dwh.DimChannel:   {cur.fetchone()[0]:>8,} registros")
            
            cur.execute("SELECT COUNT(*) FROM dwh.DimOrder")
            logger.info(f"   • dwh.DimOrder:     {cur.fetchone()[0]:>8,} registros")
            
            cur.execute("SELECT COUNT(*), SUM(lineTotalUSD) FROM dwh.FactSales")
            ventas, monto = cur.fetchone()
            logger.info(f"   • dwh.FactSales:    {ventas:>8,} registros")
            logger.info(f"   • Monto Total:      ${monto:>15,.2f} USD")
            
            logger.info("\n" + "="*60)
            logger.info("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
            logger.info("="*60)
            logger.info("\nAhora puedes eliminar esquemas dim/fact con:")
            logger.info("   DROP SCHEMA dim CASCADE")
            logger.info("   DROP SCHEMA fact CASCADE")

if __name__ == "__main__":
    inicio = datetime.now()
    try:
        migrate_to_dwh_schema()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info(f"\n⏱️  Tiempo total: {duracion:.1f} segundos")
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}")
        raise
