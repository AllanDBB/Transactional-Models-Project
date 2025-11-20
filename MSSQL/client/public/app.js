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

// Inicializar Schema
document.getElementById('btn-init-schema').addEventListener('click', async () => {
    if (isLoading) return;
    
    if (!confirm('¿Está seguro de inicializar el schema? Esto recreará todas las tablas.')) {
        return;
    }
    
    isLoading = true;
    setButtonLoading('btn-init-schema', true);
    
    try {
        const result = await apiCall('/mssql/init-schema', 'POST');
        showStatus('status-init-schema', result.message || 'Schema inicializado exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-init-schema', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-init-schema', false);
        isLoading = false;
    }
});

// Limpiar Base de Datos
document.getElementById('btn-clean-db').addEventListener('click', async () => {
    if (isLoading) return;
    
    if (!confirm('¿Está seguro de eliminar TODOS los datos? Esta acción no se puede deshacer.')) {
        return;
    }
    
    isLoading = true;
    setButtonLoading('btn-clean-db', true);
    
    try {
        const result = await apiCall('/mssql/clean', 'POST');
        showStatus('status-clean-db', result.message || 'Base de datos limpiada exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-clean-db', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-clean-db', false);
        isLoading = false;
    }
});

// Generar Datos
document.getElementById('btn-generate-data').addEventListener('click', async () => {
    if (isLoading) return;
    
    if (!confirm('¿Generar datos de prueba? (600 clientes, 5000 productos, 5000 órdenes, 17500 detalles)')) {
        return;
    }
    
    isLoading = true;
    setButtonLoading('btn-generate-data', true);
    showStatus('status-generate-data', 'Generando datos... Este proceso puede tardar 10-15 segundos', 'info');
    
    try {
        const result = await apiCall('/mssql/generate-data', 'POST');
        showStatus('status-generate-data', result.message || 'Datos generados exitosamente', 'success');
        await refreshStats();
    } catch (error) {
        showStatus('status-generate-data', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-generate-data', false);
        isLoading = false;
    }
});

// Actualizar Estadísticas
async function refreshStats() {
    setButtonLoading('btn-refresh-stats', true);
    
    try {
        const stats = await apiCall('/mssql/stats');
        
        document.getElementById('stat-clientes').textContent = stats.clientes || '0';
        document.getElementById('stat-productos').textContent = stats.productos || '0';
        document.getElementById('stat-ordenes').textContent = stats.ordenes || '0';
        document.getElementById('stat-detalles').textContent = stats.detalles || '0';
        
        showStatus('status-refresh-stats', 'Estado actualizado', 'success');
    } catch (error) {
        showStatus('status-refresh-stats', `Error: ${error.message}`, 'error');
    } finally {
        setButtonLoading('btn-refresh-stats', false);
    }
}

document.getElementById('btn-refresh-stats').addEventListener('click', refreshStats);

// Cargar estadísticas al inicio
window.addEventListener('DOMContentLoaded', async () => {
    await refreshStats();
});
