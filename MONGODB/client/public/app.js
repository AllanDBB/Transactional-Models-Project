const API_BASE = '/api';

let clientes = [];
let productos = [];
let ordenItems = [];
let clienteEditId = null;
let productoEditId = null;

const clienteForm = document.getElementById('cliente-form');
const productoForm = document.getElementById('producto-form');
const ordenForm = document.getElementById('orden-form');

async function request(path, options = {}) {
    const res = await fetch(`${API_BASE}${path}`, {
        headers: { 'Content-Type': 'application/json' },
        ...options,
    });
    if (!res.ok) {
        const msg = await res.text();
        throw new Error(msg || res.statusText);
    }
    return res.status === 204 ? null : res.json();
}

// ---- Clientes ----
async function loadClientes() {
    try {
        clientes = await request('/clientes');
        renderClientes();
        fillClienteSelect();
    } catch (err) {
        console.error(err);
    }
}

function renderClientes() {
    const list = document.getElementById('clientes-list');
    if (!clientes.length) {
        list.innerHTML = '<p class="muted">Sin clientes.</p>';
        return;
    }
    list.innerHTML = clientes.map(c => `
        <div class="item">
            <div>
                <strong>${c.nombre}</strong>
                <div class="meta">${c.email} · Genero: ${c.genero} ${c.preferencias?.canal ? ' · Canal: ' + c.preferencias.canal : ''}</div>
            </div>
            <div class="actions">
                <button class="ghost" onclick="startEditCliente('${c._id}')">Editar</button>
                <button class="danger" onclick="deleteCliente('${c._id}')">Eliminar</button>
            </div>
        </div>
    `).join('');
}

function startEditCliente(id) {
    const c = clientes.find(x => x._id === id);
    if (!c) return;
    clienteEditId = id;
    document.getElementById('cliente-id').value = id;
    document.getElementById('cliente-nombre').value = c.nombre || '';
    document.getElementById('cliente-email').value = c.email || '';
    document.getElementById('cliente-genero').value = c.genero || '';
    document.getElementById('cliente-canal').value = c.preferencias?.canal || '';
    document.getElementById('cliente-submit').textContent = 'Actualizar cliente';
    document.getElementById('cliente-cancel').hidden = false;
}

async function deleteCliente(id) {
    if (!confirm('Eliminar cliente?')) return;
    await request(`/clientes/${id}`, { method: 'DELETE' });
    await loadClientes();
    await loadOrdenes(); // por si hay dependencias
}

clienteForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const payload = {
        nombre: document.getElementById('cliente-nombre').value.trim(),
        email: document.getElementById('cliente-email').value.trim(),
        genero: document.getElementById('cliente-genero').value,
        preferencias: {},
    };
    const canal = document.getElementById('cliente-canal').value;
    if (canal) payload.preferencias.canal = canal;
    if (!Object.keys(payload.preferencias).length) delete payload.preferencias;

    try {
        if (clienteEditId) {
            await request(`/clientes/${clienteEditId}`, { method: 'PUT', body: JSON.stringify(payload) });
        } else {
            await request('/clientes', { method: 'POST', body: JSON.stringify(payload) });
        }
        resetClienteForm();
        await loadClientes();
    } catch (err) {
        alert(err.message);
    }
});

document.getElementById('cliente-cancel').addEventListener('click', resetClienteForm);

function resetClienteForm() {
    clienteEditId = null;
    clienteForm.reset();
    document.getElementById('cliente-submit').textContent = 'Guardar cliente';
    document.getElementById('cliente-cancel').hidden = true;
}

// ---- Productos ----
async function loadProductos() {
    try {
        productos = await request('/productos');
        renderProductos();
        fillProductoSelect();
    } catch (err) {
        console.error(err);
    }
}

function renderProductos() {
    const list = document.getElementById('productos-list');
    if (!productos.length) {
        list.innerHTML = '<p class="muted">Sin productos.</p>';
        return;
    }
    list.innerHTML = productos.map(p => `
        <div class="item">
            <div>
                <strong>${p.nombre}</strong>
                <div class="meta">Codigo: ${p.codigo_mongo} · Categoria: ${p.categoria}</div>
                <div class="meta">Equiv: ${p.equivalencias?.sku || '-'} ${p.equivalencias?.alt ? '/ ' + p.equivalencias.alt : ''}</div>
            </div>
            <div class="actions">
                <button class="ghost" onclick="startEditProducto('${p._id}')">Editar</button>
                <button class="danger" onclick="deleteProducto('${p._id}')">Eliminar</button>
            </div>
        </div>
    `).join('');
}

function startEditProducto(id) {
    const p = productos.find(x => x._id === id);
    if (!p) return;
    productoEditId = id;
    document.getElementById('producto-codigo').value = p.codigo_mongo || '';
    document.getElementById('producto-nombre').value = p.nombre || '';
    document.getElementById('producto-categoria').value = p.categoria || '';
    document.getElementById('producto-sku').value = p.equivalencias?.sku || '';
    document.getElementById('producto-alt').value = p.equivalencias?.alt || '';
    document.getElementById('producto-submit').textContent = 'Actualizar producto';
    document.getElementById('producto-cancel').hidden = false;
}

async function deleteProducto(id) {
    if (!confirm('Eliminar producto?')) return;
    await request(`/productos/${id}`, { method: 'DELETE' });
    await loadProductos();
    await loadOrdenes(); // por si hay referencias
}

productoForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const payload = {
        codigo_mongo: document.getElementById('producto-codigo').value.trim(),
        nombre: document.getElementById('producto-nombre').value.trim(),
        categoria: document.getElementById('producto-categoria').value.trim(),
        equivalencias: {},
    };
    const sku = document.getElementById('producto-sku').value.trim();
    const alt = document.getElementById('producto-alt').value.trim();
    if (sku) payload.equivalencias.sku = sku;
    if (alt) payload.equivalencias.alt = alt;
    if (!Object.keys(payload.equivalencias).length) delete payload.equivalencias;

    try {
        if (productoEditId) {
            await request(`/productos/${productoEditId}`, { method: 'PUT', body: JSON.stringify(payload) });
        } else {
            await request('/productos', { method: 'POST', body: JSON.stringify(payload) });
        }
        resetProductoForm();
        await loadProductos();
    } catch (err) {
        alert(err.message);
    }
});

document.getElementById('producto-cancel').addEventListener('click', resetProductoForm);

function resetProductoForm() {
    productoEditId = null;
    productoForm.reset();
    document.getElementById('producto-submit').textContent = 'Guardar producto';
    document.getElementById('producto-cancel').hidden = true;
}

// ---- Ordenes ----
async function loadOrdenes() {
    try {
        const ordenes = await request('/ordenes');
        renderOrdenes(ordenes);
    } catch (err) {
        console.error(err);
    }
}

function renderOrdenes(ordenes) {
    const list = document.getElementById('ordenes-list');
    if (!ordenes.length) {
        list.innerHTML = '<p class="muted">Sin ordenes.</p>';
        return;
    }
    list.innerHTML = ordenes.map(o => {
        const clienteNombre = o.client_id?.nombre || o.client_id?.email || o.client_id || '(cliente)';
        const total = new Intl.NumberFormat('es-CR').format(o.total || 0);
        const items = (o.items || []).map(it => {
            const prod = it.producto_id?.nombre || it.producto_id || '';
            return `${it.cantidad} x ${prod} @ ${it.precio_unit}`;
        }).join(' · ');
        return `
        <div class="item">
            <div>
                <strong>${clienteNombre}</strong>
                <div class="meta">${o.canal} · CRC ${total} · ${new Date(o.fecha).toLocaleDateString()}</div>
                <div class="meta">${items || 'Sin items'}</div>
                ${o.metadatos?.cupon ? `<div class="meta">Cupon: ${o.metadatos.cupon}</div>` : ''}
            </div>
            <div class="actions">
                <button class="danger" onclick="deleteOrden('${o._id}')">Eliminar</button>
            </div>
        </div>`;
    }).join('');
}

async function deleteOrden(id) {
    if (!confirm('Eliminar orden?')) return;
    await request(`/ordenes/${id}`, { method: 'DELETE' });
    await loadOrdenes();
}

function fillClienteSelect() {
    const select = document.getElementById('orden-cliente');
    const options = clientes.map(c => `<option value="${c._id}">${c.nombre || c.email}</option>`).join('');
    select.innerHTML = `<option value="">Selecciona cliente</option>${options}`;
}

function fillProductoSelect() {
    const select = document.getElementById('builder-producto');
    const options = productos.map(p => `<option value="${p._id}">${p.nombre}</option>`).join('');
    select.innerHTML = `<option value="">Selecciona producto</option>${options}`;
}

function renderOrdenItems() {
    const container = document.getElementById('orden-items');
    if (!ordenItems.length) {
        container.innerHTML = '<span class="muted">Sin items agregados.</span>';
        return;
    }
    container.innerHTML = ordenItems.map((it, idx) => `
        <span class="pill">
            <span>${it.cantidad} x ${findProductoNombre(it.producto_id)} @ ${it.precio_unit}</span>
            <button class="remove" onclick="removeOrdenItem(${idx})">×</button>
        </span>
    `).join('');
}

function findProductoNombre(id) {
    return productos.find(p => p._id === id)?.nombre || id;
}

document.getElementById('builder-add').addEventListener('click', () => {
    const productoId = document.getElementById('builder-producto').value;
    const cantidad = parseInt(document.getElementById('builder-cantidad').value, 10);
    const precio = parseInt(document.getElementById('builder-precio').value, 10);
    if (!productoId) return alert('Selecciona un producto');
    if (!Number.isInteger(cantidad) || cantidad <= 0) return alert('Cantidad debe ser entero > 0');
    if (!Number.isInteger(precio) || precio < 0) return alert('Precio debe ser entero >= 0');
    ordenItems.push({ producto_id: productoId, cantidad, precio_unit: precio });
    renderOrdenItems();
});

function removeOrdenItem(idx) {
    ordenItems.splice(idx, 1);
    renderOrdenItems();
}

document.getElementById('orden-clear').addEventListener('click', () => {
    ordenItems = [];
    renderOrdenItems();
});

ordenForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!ordenItems.length) return alert('Agrega al menos un item');
    const clientId = document.getElementById('orden-cliente').value;
    const canal = document.getElementById('orden-canal').value;
    if (!clientId) return alert('Selecciona cliente');
    if (!canal) return alert('Selecciona canal');
    const cupon = document.getElementById('orden-cupon').value.trim();
    const total = ordenItems.reduce((acc, it) => acc + it.cantidad * it.precio_unit, 0);
    const payload = {
        client_id: clientId,
        canal,
        moneda: 'CRC',
        total,
        items: ordenItems,
    };
    if (cupon) payload.metadatos = { cupon };
    try {
        await request('/ordenes', { method: 'POST', body: JSON.stringify(payload) });
        ordenItems = [];
        ordenForm.reset();
        renderOrdenItems();
        await loadOrdenes();
    } catch (err) {
        alert(err.message);
    }
});

// ---- Sync button ----
document.getElementById('refresh-all').addEventListener('click', () => {
    Promise.all([loadClientes(), loadProductos(), loadOrdenes()]).catch(console.error);
});

// ---- Drawer panel ----
const openDataPanelBtn = document.getElementById('open-data-panel');
const closeDataPanelBtn = document.getElementById('close-data-panel');
const dataPanel = document.getElementById('data-panel');

openDataPanelBtn.addEventListener('click', () => dataPanel.classList.remove('hidden'));
closeDataPanelBtn.addEventListener('click', () => dataPanel.classList.add('hidden'));
dataPanel.querySelector('.drawer__backdrop').addEventListener('click', () => dataPanel.classList.add('hidden'));

// ---- Bootstrap ----
(async function start() {
    renderOrdenItems();
    await loadClientes();
    await loadProductos();
    await loadOrdenes();
})(); 
