const express = require('express');
const router = express.Router();
const mysql = require('mysql2/promise');

// Configuración de conexión
const config = {
    host: process.env.MYSQL_HOST || 'localhost',
    port: parseInt(process.env.MYSQL_PORT || '3306'),
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || 'root123',
    database: process.env.MYSQL_DATABASE || 'sales_mysql',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// Función helper para ejecutar stored procedures
async function executeStoredProcedure(procedureName) {
    let connection;
    try {
        connection = await mysql.createConnection(config);
        await connection.query(`CALL ${procedureName}()`);
        return { success: true };
    } catch (error) {
        console.error(`Error ejecutando ${procedureName}:`, error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

// Función helper para ejecutar queries
async function executeQuery(query) {
    let connection;
    try {
        connection = await mysql.createConnection(config);
        const [rows] = await connection.query(query);
        return rows;
    } catch (error) {
        console.error('Error ejecutando query:', error);
        throw error;
    } finally {
        if (connection) {
            await connection.end();
        }
    }
}

// POST /api/mysql/init-schema - Inicializar schema
router.post('/init-schema', async (req, res) => {
    try {
        console.log('Inicializando schema MySQL...');
        await executeStoredProcedure('sp_init_schema');
        res.json({
            success: true,
            message: 'Schema MySQL inicializado exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mysql/drop-schema - Eliminar schema
router.post('/drop-schema', async (req, res) => {
    try {
        console.log('Eliminando schema MySQL...');
        await executeStoredProcedure('sp_drop_schema');
        res.json({
            success: true,
            message: 'Schema MySQL eliminado exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mysql/clean - Limpiar base de datos
router.post('/clean', async (req, res) => {
    try {
        console.log('Limpiando base de datos MySQL...');
        await executeStoredProcedure('sp_limpiar_bd');
        res.json({
            success: true,
            message: 'Base de datos MySQL limpiada exitosamente'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// POST /api/mysql/generate-data - Generar datos de prueba
router.post('/generate-data', async (req, res) => {
    try {
        console.log('Generando datos de prueba MySQL...');

        // Verificar que las tablas existan antes de generar datos
        const checkTablesQuery = `
            SELECT COUNT(*) as count FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = '${config.database}'
            AND TABLE_NAME IN ('Cliente', 'Producto', 'Orden', 'OrdenDetalle')
        `;
        const tables = await executeQuery(checkTablesQuery);

        if (tables[0].count !== 4) {
            return res.status(400).json({
                success: false,
                error: 'El schema no está inicializado. Por favor, haz clic en "Inicializar Schema" primero.'
            });
        }

        await executeStoredProcedure('sp_generar_datos');
        res.json({
            success: true,
            message: 'Datos MySQL generados exitosamente: 600 clientes, 5000 productos, 5000 órdenes, 17500 detalles'
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// GET /api/mysql/stats - Obtener estadísticas
router.get('/stats', async (req, res) => {
    try {
        const queries = {
            clientes: 'SELECT COUNT(*) as count FROM Cliente',
            productos: 'SELECT COUNT(*) as count FROM Producto',
            ordenes: 'SELECT COUNT(*) as count FROM Orden',
            detalles: 'SELECT COUNT(*) as count FROM OrdenDetalle'
        };

        const results = {};

        for (const [key, query] of Object.entries(queries)) {
            try {
                const rows = await executeQuery(query);
                results[key] = rows[0].count;
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

// GET /api/mysql/query/:table - Obtener datos de una tabla (primeros 100 registros)
router.get('/query/:table', async (req, res) => {
    try {
        const tableName = req.params.table;
        const allowedTables = ['Cliente', 'Producto', 'Orden', 'OrdenDetalle'];

        if (!allowedTables.includes(tableName)) {
            return res.status(400).json({
                success: false,
                error: 'Tabla no permitida'
            });
        }

        const query = `SELECT * FROM ${tableName} ORDER BY id DESC LIMIT 100`;
        const rows = await executeQuery(query);

        res.json({
            success: true,
            table: tableName,
            data: rows
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// GET /api/mysql/test-connection - Probar conexión
router.get('/test-connection', async (req, res) => {
    let connection;
    try {
        connection = await mysql.createConnection(config);
        res.json({
            success: true,
            message: 'Conexión MySQL exitosa',
            config: {
                host: config.host,
                port: config.port,
                database: config.database,
                user: config.user
            }
        });
    } catch (error) {
        res.json({
            success: false,
            error: error.message,
            config: {
                host: config.host,
                port: config.port,
                database: config.database,
                user: config.user
            }
        });
    } finally {
        if (connection) {
            await connection.end();
        }
    }
});

module.exports = router;
