// Estado global
let isLoading = false;

// Utilidades
function showStatus(elementId, message, type = 'info') {
    const el = document.getElementById(elementId);
    el.textContent = message;
    el.className = `status-message show ${type}`;
    setTimeout(() => {
        el.classList.remove('show');
    }, 5000);
}

function setButtonLoading(buttonId, loading) {
    const btn = document.getElementById(buttonId);
    const text = btn.querySelector('.button-text');
    const spinner = btn.querySelector('.spinner');

    btn.disabled = loading;
    if (loading) {
        text.style.display = 'none';
        spinner.style.display = 'inline-block';
    } else {
        text.style.display = 'inline-block';
        spinner.style.display = 'none';
    }
}

async function apiCall(endpoint, method = 'GET') {
    try {
        const response = await fetch(`/api${endpoint}`, {
            method,
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || `Error ${response.status}`);
        }

        return data;
    } catch (error) {
        throw error;
    }
}

// ======================================
// FUNCIONES MSSQL
// ======================================

// Inicializar Schema MSSQL
document.getElementById('btn-init-schema').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Está seguro de inicializar el schema MSSQL? Esto recreará todas las tablas.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-init-schema', true);

    try {
        const result = await apiCall('/mssql/init-schema', 'POST');
        showStatus('status-init-schema', result.message || 'Schema MSSQL inicializado exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-init-schema', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-init-schema', false);
        isLoading = false;
    }
});

// Eliminar Schema MSSQL
document.getElementById('btn-drop-schema').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('ADVERTENCIA: ¿Está completamente seguro de eliminar el schema MSSQL?\n\nEsto eliminará TODAS las tablas y su estructura.\n\nEsta acción NO se puede deshacer.')) {
        return;
    }

    // Segunda confirmación para seguridad
    const confirmText = prompt('Para confirmar, escriba "ELIMINAR" (en mayúsculas):');
    if (confirmText !== 'ELIMINAR') {
        showStatus('status-drop-schema', 'Operación cancelada', 'info');
        return;
    }

    isLoading = true;
    setButtonLoading('btn-drop-schema', true);

    try {
        const result = await apiCall('/mssql/drop-schema', 'POST');
        showStatus('status-drop-schema', result.message || 'Schema MSSQL eliminado exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-drop-schema', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-drop-schema', false);
        isLoading = false;
    }
});

// Limpiar Base de Datos MSSQL
document.getElementById('btn-clean-db').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Está seguro de eliminar TODOS los datos de MSSQL? Esta acción no se puede deshacer.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-clean-db', true);

    try {
        const result = await apiCall('/mssql/clean', 'POST');
        showStatus('status-clean-db', result.message || 'Base de datos MSSQL limpiada exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-clean-db', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-clean-db', false);
        isLoading = false;
    }
});

// Generar Datos MSSQL
document.getElementById('btn-generate-data').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Generar datos de prueba en MSSQL? Este proceso puede tardar varios minutos.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-generate-data', true);
    showStatus('status-generate-data', 'Generando datos... Por favor espera', 'info');

    try {
        const result = await apiCall('/mssql/generate-data', 'POST');
        showStatus('status-generate-data', 'Datos generados correctamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-generate-data', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-generate-data', false);
        isLoading = false;
    }
});

// Actualizar Estadísticas MSSQL
async function refreshStats() {
    setButtonLoading('btn-refresh-stats', true);

    try {
        const stats = await apiCall('/mssql/stats');

        document.getElementById('stat-clientes').textContent = stats.clientes || '0';
        document.getElementById('stat-productos').textContent = stats.productos || '0';
        document.getElementById('stat-ordenes').textContent = stats.ordenes || '0';
        document.getElementById('stat-detalles').textContent = stats.detalles || '0';

        showStatus('status-refresh-stats', 'Estado MSSQL actualizado', 'success');
    } catch (error) {
        showStatus('status-refresh-stats', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-refresh-stats', false);
    }
}

document.getElementById('btn-refresh-stats').addEventListener('click', refreshStats);

// ======================================
// FUNCIONES MYSQL
// ======================================

// Inicializar Schema MySQL
document.getElementById('btn-init-schema-mysql').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Está seguro de inicializar el schema MySQL? Esto recreará todas las tablas.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-init-schema-mysql', true);

    try {
        const result = await apiCall('/mysql/init-schema', 'POST');
        showStatus('status-init-schema-mysql', result.message || 'Schema MySQL inicializado exitosamente', 'success');
        await refreshStatsMySQL();
    } catch (error) {
        showStatus('status-init-schema-mysql', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-init-schema-mysql', false);
        isLoading = false;
    }
});

// Eliminar Schema MySQL
document.getElementById('btn-drop-schema-mysql').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('ADVERTENCIA: ¿Está completamente seguro de eliminar el schema MySQL?\n\nEsto eliminará TODAS las tablas y su estructura.\n\nEsta acción NO se puede deshacer.')) {
        return;
    }

    // Segunda confirmación para seguridad
    const confirmText = prompt('Para confirmar, escriba "ELIMINAR" (en mayúsculas):');
    if (confirmText !== 'ELIMINAR') {
        showStatus('status-drop-schema-mysql', 'Operación cancelada', 'info');
        return;
    }

    isLoading = true;
    setButtonLoading('btn-drop-schema-mysql', true);

    try {
        const result = await apiCall('/mysql/drop-schema', 'POST');
        showStatus('status-drop-schema-mysql', result.message || 'Schema MySQL eliminado exitosamente', 'success');
        await refreshStatsMySQL();
    } catch (error) {
        showStatus('status-drop-schema-mysql', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-drop-schema-mysql', false);
        isLoading = false;
    }
});

// Limpiar Base de Datos MySQL
document.getElementById('btn-clean-db-mysql').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Está seguro de eliminar TODOS los datos de MySQL? Esta acción no se puede deshacer.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-clean-db-mysql', true);

    try {
        const result = await apiCall('/mysql/clean', 'POST');
        showStatus('status-clean-db-mysql', result.message || 'Base de datos MySQL limpiada exitosamente', 'success');
        await refreshStatsMySQL();
    } catch (error) {
        showStatus('status-clean-db-mysql', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-clean-db-mysql', false);
        isLoading = false;
    }
});

// Generar Datos MySQL
document.getElementById('btn-generate-data-mysql').addEventListener('click', async () => {
    if (isLoading) return;

    if (!confirm('¿Generar datos de prueba en MySQL? Este proceso puede tardar varios minutos.')) {
        return;
    }

    isLoading = true;
    setButtonLoading('btn-generate-data-mysql', true);
    showStatus('status-generate-data-mysql', 'Generando datos... Por favor espera', 'info');

    try {
        const result = await apiCall('/mysql/generate-data', 'POST');
        showStatus('status-generate-data-mysql', 'Datos generados correctamente', 'success');
        await refreshStatsMySQL();
    } catch (error) {
        showStatus('status-generate-data-mysql', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-generate-data-mysql', false);
        isLoading = false;
    }
});

// Actualizar Estadísticas MySQL
async function refreshStatsMySQL() {
    setButtonLoading('btn-refresh-stats-mysql', true);

    try {
        const stats = await apiCall('/mysql/stats');

        document.getElementById('stat-clientes-mysql').textContent = stats.clientes || '0';
        document.getElementById('stat-productos-mysql').textContent = stats.productos || '0';
        document.getElementById('stat-ordenes-mysql').textContent = stats.ordenes || '0';
        document.getElementById('stat-detalles-mysql').textContent = stats.detalles || '0';

        showStatus('status-refresh-stats-mysql', 'Estado MySQL actualizado', 'success');
    } catch (error) {
        showStatus('status-refresh-stats-mysql', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-refresh-stats-mysql', false);
    }
}

document.getElementById('btn-refresh-stats-mysql').addEventListener('click', refreshStatsMySQL);

// ======================================
// QUERY RESULTS
// ======================================

function renderTable(data, containerId) {
    const container = document.getElementById(containerId);

    if (!data || data.length === 0) {
        container.innerHTML = '<p class="no-data">Sin datos disponibles</p>';
        return;
    }

    const keys = Object.keys(data[0]);
    let html = '<table><thead><tr>';

    keys.forEach(key => {
        html += `<th>${key}</th>`;
    });
    html += '</tr></thead><tbody>';

    data.forEach(row => {
        html += '<tr>';
        keys.forEach(key => {
            const value = row[key] !== null ? row[key] : '-';
            html += `<td>${value}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    container.innerHTML = html;
}

document.querySelectorAll('.query-btn').forEach(btn => {
    btn.addEventListener('click', async function() {
        const table = this.getAttribute('data-table');
        const db = this.getAttribute('data-db') || 'mssql';
        const containerId = db === 'mysql' ? 'query-results-mysql' : 'query-results';

        const container = this.closest('.query-buttons');
        container.querySelectorAll('.query-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');

        try {
            const response = await apiCall(`/${db}/query/${table}`);
            renderTable(response.data, containerId);
        } catch (error) {
            document.getElementById(containerId).innerHTML =
                `<p class="no-data">Error: ${error.message}</p>`;
        }
    });
});

// ======================================
// INICIALIZACIÓN
// ======================================

// Cargar estadísticas al inicio
window.addEventListener('DOMContentLoaded', async () => {
    await refreshStats();
    await refreshStatsMySQL();
});
