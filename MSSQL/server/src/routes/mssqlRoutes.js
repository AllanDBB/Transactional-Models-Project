const express = require('express');
const router = express.Router();
const sql = require('mssql');

// Configuración de conexión
const config = {
    user: process.env.MSSQL_USER || 'sa',
    password: process.env.MSSQL_PASSWORD || 'SaPassword123!',
    server: process.env.MSSQL_SERVER || 'localhost',
    port: parseInt(process.env.MSSQL_PORT || '1433'),
    database: process.env.MSSQL_DATABASE || 'SalesDB_MSSQL',
    options: {
        encrypt: false,
        trustServerCertificate: true,
        enableArithAbort: true
    },
    requestTimeout: 120000,  // 2 minutos para operaciones pesadas
    connectionTimeout: 30000  // 30 segundos para conectar
};

// Función helper para ejecutar stored procedures
async function executeStoredProcedure(procedureName, timeout = 120000) {
    let pool;
    try {
        pool = await sql.connect(config);
        const request = pool.request();
        request.timeout = timeout;  // Timeout específico para este request
        const result = await request.execute(procedureName);
        return { success: true, result };
    } catch (error) {
        console.error(`Error ejecutando ${procedureName}:`, error);
        throw error;
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}

// Función helper para ejecutar queries
async function executeQuery(query, timeout = 30000) {
    let pool;
    try {
        pool = await sql.connect(config);
        const request = pool.request();
        request.timeout = timeout;
        const result = await request.query(query);
        return result;
    } catch (error) {
        console.error('Error ejecutando query:', error);
        throw error;
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}

// POST /api/mssql/init-schema - Inicializar schema
router.post('/init-schema', async (req, res) => {
    try {
        console.log('Inicializando schema...');
        await executeStoredProcedure('dbo.sp_init_schema');
        res.json({
            success: true,
            message: 'Schema inicializado exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mssql/drop-schema - Eliminar schema
router.post('/drop-schema', async (req, res) => {
    try {
        console.log('Eliminando schema...');
        await executeStoredProcedure('dbo.sp_drop_schema');
        res.json({
            success: true,
            message: 'Schema eliminado exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mssql/clean - Limpiar base de datos
router.post('/clean', async (req, res) => {
    try {
        console.log('Limpiando base de datos...');
        await executeStoredProcedure('sales_ms.sp_limpiar_bd');
        res.json({
            success: true,
            message: 'Base de datos limpiada exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mssql/generate-data - Generar datos de prueba
router.post('/generate-data', async (req, res) => {
    try {
        console.log('Generando datos de prueba...');
        await executeStoredProcedure('sales_ms.sp_generar_datos');
        res.json({
            success: true,
            message: 'Datos generados exitosamente: 600 clientes, 5000 productos, 5000 órdenes, 17500 detalles'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// GET /api/mssql/stats - Obtener estadísticas
router.get('/stats', async (req, res) => {
    try {
        const queries = {
            clientes: 'SELECT COUNT(*) as count FROM sales_ms.Cliente',
            productos: 'SELECT COUNT(*) as count FROM sales_ms.Producto',
            ordenes: 'SELECT COUNT(*) as count FROM sales_ms.Orden',
            detalles: 'SELECT COUNT(*) as count FROM sales_ms.OrdenDetalle'
        };

        const results = {};
        
        for (const [key, query] of Object.entries(queries)) {
            try {
                const result = await executeQuery(query);
                results[key] = result.recordset[0].count;
            } catch (error) {
                results[key] = 0;
            }
        }

        res.json(results);
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

module.exports = router;
