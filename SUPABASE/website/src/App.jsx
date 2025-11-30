import { useEffect, useMemo, useState } from "react";
import { supabase } from "./supabaseClient";
import "./App.css";

const CHANNELS = ["WEB", "APP", "PARTNER"];
const CURRENCIES = ["USD", "CRC"];
const GENDERS = ["Masculino", "Femenino", "Otro"];

const emptyCliente = { nombre: "", email: "", genero: "", pais: "" };
const emptyProducto = { nombre: "", categoria: "", sku: "" };

const emptyItem = { producto_id: "", cantidad: 1, precio_unit: 0 };

export default function App() {
  const [clientes, setClientes] = useState([]);
  const [productos, setProductos] = useState([]);
  const [ordenes, setOrdenes] = useState([]);
  const [clienteSearch, setClienteSearch] = useState("");
  const [productoSearch, setProductoSearch] = useState("");

  const [clienteForm, setClienteForm] = useState(emptyCliente);
  const [clienteEditId, setClienteEditId] = useState(null);

  const [productoForm, setProductoForm] = useState(emptyProducto);
  const [productoEditId, setProductoEditId] = useState(null);

  const [ordenClienteId, setOrdenClienteId] = useState("");
  const [ordenCanal, setOrdenCanal] = useState(CHANNELS[0]);
  const [ordenMoneda, setOrdenMoneda] = useState(CURRENCIES[0]);
  const [ordenItems, setOrdenItems] = useState([emptyItem]);

  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState({ type: "", text: "" });

  const totalOrden = useMemo(
    () =>
      ordenItems.reduce(
        (sum, it) => sum + (Number(it.cantidad) || 0) * (Number(it.precio_unit) || 0),
        0
      ),
    [ordenItems]
  );

  useEffect(() => {
    loadAll();
  }, []);

  const notify = (type, text) => {
    setToast({ type, text });
    setTimeout(() => setToast({ type: "", text: "" }), 3500);
  };

  const loadClientes = async () => {
    const { data, error } = await supabase
      .from("cliente")
      .select("cliente_id,nombre,email,genero,pais,fecha_registro")
      .order("fecha_registro", { ascending: false });
    if (error) throw error;
    setClientes(data || []);
  };

  const loadProductos = async () => {
    const { data, error } = await supabase
      .from("producto")
      .select("producto_id,nombre,categoria,sku,fecha_registro")
      .order("fecha_registro", { ascending: false });
    if (error) throw error;
    setProductos(data || []);
  };

  const loadOrdenes = async () => {
    const { data: ords, error } = await supabase
      .from("orden")
      .select("*")
      .order("fecha", { ascending: false })
      .limit(80); // limitar listado para evitar query enorme
    if (error) throw error;
    const ids = (ords || []).map((o) => o.orden_id);
    let detalles = [];
    if (ids.length) {
      const chunkSize = 80;
      for (let i = 0; i < ids.length; i += chunkSize) {
        const slice = ids.slice(i, i + chunkSize);
        const { data: dets, error: detErr } = await supabase
          .from("orden_detalle")
          .select("*")
          .in("orden_id", slice);
        if (detErr) throw detErr;
        detalles = detalles.concat(dets || []);
      }
    }
    const grouped = detalles.reduce((acc, d) => {
      if (!acc.has(d.orden_id)) acc.set(d.orden_id, []);
      acc.get(d.orden_id).push(d);
      return acc;
    }, new Map());
    setOrdenes(
      (ords || []).map((o) => ({
        ...o,
        items: grouped.get(o.orden_id) || [],
      }))
    );
  };

  const loadAll = async () => {
    setBusy(true);
    try {
      await Promise.all([loadClientes(), loadProductos(), loadOrdenes()]);
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo cargar Supabase.");
    } finally {
      setBusy(false);
    }
  };

  const filteredClientes = useMemo(() => {
    const term = clienteSearch.toLowerCase().trim();
    if (!term) return clientes;
    return clientes.filter(
      (c) =>
        (c.nombre || "").toLowerCase().includes(term) ||
        (c.email || "").toLowerCase().includes(term) ||
        (c.pais || "").toLowerCase().includes(term)
    );
  }, [clienteSearch, clientes]);

  const filteredProductos = useMemo(() => {
    const term = productoSearch.toLowerCase().trim();
    if (!term) return productos;
    return productos.filter(
      (p) =>
        (p.nombre || "").toLowerCase().includes(term) ||
        (p.categoria || "").toLowerCase().includes(term) ||
        (p.sku || "").toLowerCase().includes(term)
    );
  }, [productoSearch, productos]);

  const saveCliente = async (e) => {
    e.preventDefault();
    setBusy(true);
    try {
      const payload = {
        ...clienteForm,
        fecha_registro: clienteForm.fecha_registro || new Date().toISOString(),
      };
      if (clienteEditId) {
        const { error } = await supabase
          .from("cliente")
          .update(payload)
          .eq("cliente_id", clienteEditId);
        if (error) throw error;
        notify("ok", "Cliente actualizado.");
      } else {
        const { error } = await supabase.from("cliente").insert([payload]);
        if (error) throw error;
        notify("ok", "Cliente creado.");
      }
      setClienteForm(emptyCliente);
      setClienteEditId(null);
      await loadClientes();
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo guardar el cliente.");
    } finally {
      setBusy(false);
    }
  };

  const saveProducto = async (e) => {
    e.preventDefault();
    setBusy(true);
    try {
      const payload = {
        ...productoForm,
        fecha_registro: productoForm.fecha_registro || new Date().toISOString(),
      };
      if (productoEditId) {
        const { error } = await supabase
          .from("producto")
          .update(payload)
          .eq("producto_id", productoEditId);
        if (error) throw error;
        notify("ok", "Producto actualizado.");
      } else {
        const { error } = await supabase.from("producto").insert([payload]);
        if (error) throw error;
        notify("ok", "Producto creado.");
      }
      setProductoForm(emptyProducto);
      setProductoEditId(null);
      await loadProductos();
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo guardar el producto.");
    } finally {
      setBusy(false);
    }
  };

  const deleteCliente = async (id) => {
    if (!id || !confirm("¿Eliminar cliente?")) return;
    setBusy(true);
    try {
      const { error } = await supabase.from("cliente").delete().eq("cliente_id", id);
      if (error) throw error;
      notify("ok", "Cliente eliminado.");
      await Promise.all([loadClientes(), loadOrdenes()]);
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo eliminar el cliente.");
    } finally {
      setBusy(false);
    }
  };

  const deleteProducto = async (id) => {
    if (!id || !confirm("¿Eliminar producto?")) return;
    setBusy(true);
    try {
      const { error } = await supabase.from("producto").delete().eq("producto_id", id);
      if (error) throw error;
      notify("ok", "Producto eliminado.");
      await Promise.all([loadProductos(), loadOrdenes()]);
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo eliminar el producto.");
    } finally {
      setBusy(false);
    }
  };

  const addOrdenItem = () => setOrdenItems([...ordenItems, { ...emptyItem }]);

  const updateOrdenItem = (idx, field, value) => {
    const next = [...ordenItems];
    next[idx] = { ...next[idx], [field]: field === "producto_id" ? value : Number(value) };
    setOrdenItems(next);
  };

  const removeOrdenItem = (idx) => {
    if (ordenItems.length === 1) return;
    setOrdenItems(ordenItems.filter((_, i) => i !== idx));
  };

  const saveOrden = async (e) => {
    e.preventDefault();
    if (!ordenClienteId) {
      notify("error", "Selecciona un cliente para la orden.");
      return;
    }
    if (ordenItems.some((it) => !it.producto_id || it.cantidad <= 0 || it.precio_unit < 0)) {
      notify("error", "Revisa productos, cantidad y precio.");
      return;
    }

    setBusy(true);
    try {
      const { data: ordData, error: ordErr } = await supabase
        .from("orden")
        .insert([
          {
            cliente_id: ordenClienteId,
            fecha: new Date().toISOString(),
            canal: ordenCanal,
            moneda: ordenMoneda,
            total: totalOrden,
          },
        ])
        .select("orden_id")
        .single();
      if (ordErr) throw ordErr;

      const detallesPayload = ordenItems.map((it) => ({
        orden_id: ordData.orden_id,
        producto_id: it.producto_id,
        cantidad: it.cantidad,
        precio_unit: it.precio_unit,
      }));
      const { error: detErr } = await supabase.from("orden_detalle").insert(detallesPayload);
      if (detErr) throw detErr;

      notify("ok", "Orden creada.");
      setOrdenItems([emptyItem]);
      setOrdenCanal(CHANNELS[0]);
      setOrdenMoneda(CURRENCIES[0]);
      await loadOrdenes();
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo crear la orden.");
    } finally {
      setBusy(false);
    }
  };

  const deleteOrden = async (id) => {
    if (!id || !confirm("¿Eliminar orden y sus detalles?")) return;
    setBusy(true);
    try {
      await supabase.from("orden_detalle").delete().eq("orden_id", id);
      const { error } = await supabase.from("orden").delete().eq("orden_id", id);
      if (error) throw error;
      notify("ok", "Orden eliminada.");
      await loadOrdenes();
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo eliminar la orden.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="page">
      <header className="hero">
        <div>
          <p className="eyebrow">Supabase</p>
          <h1>Backoffice de Ventas</h1>
          <p className="subtitle">
            CRUD completo para clientes, productos y órdenes en Supabase. Todo en un panel rápido.
          </p>
          {toast.text && <div className={`toast ${toast.type}`}>{toast.text}</div>}
        </div>
        <div className="stats">
          <div className="stat">
            <span>Clientes</span>
            <strong>{clientes.length}</strong>
          </div>
          <div className="stat">
            <span>Productos</span>
            <strong>{productos.length}</strong>
          </div>
          <div className="stat">
            <span>Órdenes</span>
            <strong>{ordenes.length}</strong>
          </div>
        </div>
      </header>

      <section className="grid two">
        <div className="card">
          <div className="card-head">
            <h2>Clientes</h2>
            <button className="ghost" onClick={() => loadClientes()} disabled={busy}>
              ↻
            </button>
          </div>
          <div className="row" style={{ marginBottom: 8 }}>
            <label>Buscar cliente</label>
            <input
              placeholder="Nombre, email o país"
              value={clienteSearch}
              onChange={(e) => setClienteSearch(e.target.value)}
            />
          </div>
          <form className="form" onSubmit={saveCliente}>
            <div className="row">
              <label>Nombre</label>
              <input
                value={clienteForm.nombre}
                onChange={(e) => setClienteForm({ ...clienteForm, nombre: e.target.value })}
                required
              />
            </div>
            <div className="row">
              <label>Email</label>
              <input
                type="email"
                value={clienteForm.email}
                onChange={(e) => setClienteForm({ ...clienteForm, email: e.target.value })}
                required
              />
            </div>
            <div className="row split">
              <div>
                <label>Género</label>
                <select
                  value={clienteForm.genero}
                  onChange={(e) => setClienteForm({ ...clienteForm, genero: e.target.value })}
                >
                  <option value="">-</option>
                  {GENDERS.map((g) => (
                    <option key={g} value={g}>
                      {g}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label>País</label>
                <input
                  value={clienteForm.pais}
                  onChange={(e) => setClienteForm({ ...clienteForm, pais: e.target.value })}
                />
              </div>
            </div>
            <div className="actions">
              <button type="submit" disabled={busy}>
                {clienteEditId ? "Actualizar" : "Crear"} cliente
              </button>
              {clienteEditId && (
                <button
                  type="button"
                  className="ghost"
                  onClick={() => {
                    setClienteEditId(null);
                    setClienteForm(emptyCliente);
                  }}
                >
                  Cancelar
                </button>
              )}
            </div>
          </form>
          <div className="list scroll">
            {filteredClientes.map((c) => (
              <div key={c.cliente_id} className="list-row">
                <div>
                  <strong>{c.nombre}</strong>
                  <p className="muted">
                    {c.email} · {c.genero || "?"} · {c.pais || "—"}
                  </p>
                </div>
                <div className="row-actions">
                  <button
                    className="ghost"
                    onClick={() => {
                      setClienteEditId(c.cliente_id);
                      setClienteForm({
                        nombre: c.nombre || "",
                        email: c.email || "",
                        genero: c.genero || "",
                        pais: c.pais || "",
                      });
                    }}
                  >
                    Editar
                  </button>
                  <button className="danger" onClick={() => deleteCliente(c.cliente_id)} disabled={busy}>
                    Borrar
                  </button>
                </div>
              </div>
            ))}
            {!clientes.length && <p className="muted">No hay clientes.</p>}
          </div>
        </div>

        <div className="card">
          <div className="card-head">
            <h2>Productos</h2>
            <button className="ghost" onClick={() => loadProductos()} disabled={busy}>
              ↻
            </button>
          </div>
          <div className="row" style={{ marginBottom: 8 }}>
            <label>Buscar producto</label>
            <input
              placeholder="Nombre, categoría o SKU"
              value={productoSearch}
              onChange={(e) => setProductoSearch(e.target.value)}
            />
          </div>
          <form className="form" onSubmit={saveProducto}>
            <div className="row">
              <label>Nombre</label>
              <input
                value={productoForm.nombre}
                onChange={(e) => setProductoForm({ ...productoForm, nombre: e.target.value })}
                required
              />
            </div>
            <div className="row split">
              <div>
                <label>Categoria</label>
                <input
                  value={productoForm.categoria}
                  onChange={(e) => setProductoForm({ ...productoForm, categoria: e.target.value })}
                />
              </div>
              <div>
                <label>SKU</label>
                <input
                  value={productoForm.sku}
                  onChange={(e) => setProductoForm({ ...productoForm, sku: e.target.value })}
                  placeholder="Opcional"
                />
              </div>
            </div>
            <div className="actions">
              <button type="submit" disabled={busy}>
                {productoEditId ? "Actualizar" : "Crear"} producto
              </button>
              {productoEditId && (
                <button
                  type="button"
                  className="ghost"
                  onClick={() => {
                    setProductoEditId(null);
                    setProductoForm(emptyProducto);
                  }}
                >
                  Cancelar
                </button>
              )}
            </div>
          </form>
          <div className="list scroll">
            {filteredProductos.map((p) => (
              <div key={p.producto_id} className="list-row">
                <div>
                  <strong>{p.nombre}</strong>
                  <p className="muted">
                    {p.categoria || "Sin categoría"} · {p.sku || "Sin SKU"}
                  </p>
                </div>
                <div className="row-actions">
                  <button
                    className="ghost"
                    onClick={() => {
                      setProductoEditId(p.producto_id);
                      setProductoForm({
                        nombre: p.nombre || "",
                        categoria: p.categoria || "",
                        sku: p.sku || "",
                      });
                    }}
                  >
                    Editar
                  </button>
                  <button className="danger" onClick={() => deleteProducto(p.producto_id)} disabled={busy}>
                    Borrar
                  </button>
                </div>
              </div>
            ))}
            {!productos.length && <p className="muted">No hay productos.</p>}
          </div>
        </div>
      </section>

      <section className="card">
        <div className="card-head">
          <div>
            <p className="eyebrow">Crea órdenes</p>
            <h2>Builder de orden</h2>
          </div>
          <div className="pill total">
            Total: {totalOrden.toFixed(2)} {ordenMoneda}
          </div>
        </div>
        <form className="form" onSubmit={saveOrden}>
          <div className="row split">
            <div>
              <label>Cliente</label>
              <select value={ordenClienteId} onChange={(e) => setOrdenClienteId(e.target.value)} required>
                <option value="">Selecciona cliente</option>
                {clientes.map((c) => (
                  <option key={c.cliente_id} value={c.cliente_id}>
                    {c.nombre}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Canal</label>
              <select value={ordenCanal} onChange={(e) => setOrdenCanal(e.target.value)}>
                {CHANNELS.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label>Moneda</label>
              <select value={ordenMoneda} onChange={(e) => setOrdenMoneda(e.target.value)}>
                {CURRENCIES.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="items">
            {ordenItems.map((item, idx) => (
              <div key={idx} className="item-row">
                <select
                  value={item.producto_id}
                  onChange={(e) => updateOrdenItem(idx, "producto_id", e.target.value)}
                  required
                >
                  <option value="">Producto</option>
                  {productos.map((p) => (
                    <option key={p.producto_id} value={p.producto_id}>
                      {p.nombre}
                    </option>
                  ))}
                </select>
                <input
                  type="number"
                  min="1"
                  value={item.cantidad}
                  onChange={(e) => updateOrdenItem(idx, "cantidad", e.target.value)}
                  placeholder="Cantidad"
                  required
                />
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={item.precio_unit}
                  onChange={(e) => updateOrdenItem(idx, "precio_unit", e.target.value)}
                  placeholder="Precio unit."
                  required
                />
                <button type="button" className="ghost" onClick={() => removeOrdenItem(idx)} disabled={ordenItems.length === 1}>
                  ×
                </button>
              </div>
            ))}
          </div>
          <div className="actions">
            <button type="button" className="ghost" onClick={addOrdenItem}>
              + Agregar producto
            </button>
            <button type="submit" disabled={busy}>
              Guardar orden
            </button>
          </div>
        </form>
      </section>

      <section className="card">
        <div className="card-head">
          <div>
            <p className="eyebrow">Resumen</p>
            <h2>Órdenes recientes</h2>
          </div>
          <button className="ghost" onClick={() => loadOrdenes()} disabled={busy}>
            ↻
          </button>
        </div>
        <div className="list compact">
          {ordenes.map((o) => (
            <div key={o.orden_id} className="list-row">
              <div>
                <strong>#{o.orden_id.slice(0, 8)} · {o.canal}</strong>
                <p className="muted">
                  {o.moneda} {Number(o.total || 0).toFixed(2)} · {new Date(o.fecha).toLocaleDateString()}
                </p>
                <p className="muted">
                  {o.items.map((it) => {
                    const prod = productos.find((p) => p.producto_id === it.producto_id);
                    return `${it.cantidad}x ${prod?.nombre || it.producto_id} @ ${it.precio_unit}`;
                  }).join(" · ")}
                </p>
              </div>
              <div className="row-actions">
                <button className="danger" onClick={() => deleteOrden(o.orden_id)} disabled={busy}>
                  Borrar
                </button>
              </div>
            </div>
          ))}
          {!ordenes.length && <p className="muted">Aún no hay órdenes.</p>}
        </div>
      </section>
    </div>
  );
}
