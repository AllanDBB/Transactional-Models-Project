import { useEffect, useMemo, useState } from "react";
import { supabase } from "./supabaseClient";
import "./App.css";

const CHANNELS = ["WEB", "APP", "PARTNER"];
const CURRENCIES = ["USD", "CRC"];

const emptyItem = { producto_id: "", cantidad: 1, precio_unit: 0 };

export default function App() {
  const [clientes, setClientes] = useState([]);
  const [productos, setProductos] = useState([]);
  const [ordenes, setOrdenes] = useState([]);

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
      .limit(80);
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

  const updateOrdenItem = (idx, field, value) => {
    const copy = [...ordenItems];
    copy[idx][field] = value;
    setOrdenItems(copy);
  };

  const addOrdenItem = () => {
    setOrdenItems([...ordenItems, emptyItem]);
  };

  const removeOrdenItem = (idx) => {
    setOrdenItems(ordenItems.filter((_, i) => i !== idx));
  };

  const saveOrden = async (e) => {
    e.preventDefault();
    setBusy(true);
    try {
      const total = ordenItems.reduce(
        (sum, it) => sum + Number(it.cantidad) * Number(it.precio_unit),
        0
      );
      const { data: ord, error: ordErr } = await supabase
        .from("orden")
        .insert({
          cliente_id: ordenClienteId,
          fecha: new Date().toISOString(),
          canal: ordenCanal,
          moneda: ordenMoneda,
          total: total.toFixed(2),
        })
        .select()
        .single();
      if (ordErr) throw ordErr;

      const detalles = ordenItems.map((it) => ({
        orden_id: ord.orden_id,
        producto_id: it.producto_id,
        cantidad: Number(it.cantidad),
        precio_unit: Number(it.precio_unit),
      }));
      const { error: detErr } = await supabase.from("orden_detalle").insert(detalles);
      if (detErr) throw detErr;

      notify("ok", "Orden creada exitosamente.");
      setOrdenClienteId("");
      setOrdenCanal(CHANNELS[0]);
      setOrdenMoneda(CURRENCIES[0]);
      setOrdenItems([emptyItem]);
      await loadOrdenes();
    } catch (err) {
      console.error(err);
      notify("error", "No se pudo crear la orden.");
    } finally {
      setBusy(false);
    }
  };

  const deleteOrden = async (id) => {
    if (!confirm("¿Eliminar orden?")) return;
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
            Gestión de órdenes en Supabase
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
