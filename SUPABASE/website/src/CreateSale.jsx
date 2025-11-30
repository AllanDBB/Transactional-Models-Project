import { useEffect, useState } from "react";
import { supabase } from "./supabaseClient";

const CHANNELS = ["WEB", "APP", "PARTNER"];
const CURRENCIES = ["USD", "CRC"];

export default function CreateSale() {
  const [clientes, setClientes] = useState([]);
  const [productos, setProductos] = useState([]);
  const [clienteId, setClienteId] = useState("");
  const [canal, setCanal] = useState("WEB");
  const [moneda, setMoneda] = useState("USD");
  const [items, setItems] = useState([
    { producto_id: "", cantidad: 1, precio_unit: 0 },
  ]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState(null);
  const [error, setError] = useState(null);

  // 1) Cargar clientes y productos al montar
  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      setError(null);

      const { data: clientesData, error: clientesError } = await supabase
        .from("cliente")
        .select("cliente_id, nombre")
        .order("nombre", { ascending: true });

      const { data: productosData, error: productosError } = await supabase
        .from("producto")
        .select("producto_id, nombre")
        .order("nombre", { ascending: true });

      if (clientesError || productosError) {
        console.error(clientesError || productosError);
        setError("Error cargando clientes o productos.");
      } else {
        setClientes(clientesData || []);
        setProductos(productosData || []);
      }

      setLoading(false);
    };

    loadData();
  }, []);

  // 2) Helpers para manejar items (líneas de detalle)
  const handleItemChange = (index, field, value) => {
    const updated = [...items];
    if (field === "cantidad" || field === "precio_unit") {
      updated[index][field] = Number(value);
    } else {
      updated[index][field] = value;
    }
    setItems(updated);
  };

  const addItem = () => {
    setItems([...items, { producto_id: "", cantidad: 1, precio_unit: 0 }]);
  };

  const removeItem = (index) => {
    if (items.length === 1) return;
    const updated = items.filter((_, i) => i !== index);
    setItems(updated);
  };

  // 3) Calcular total de la orden
  const totalOrden = items.reduce(
    (sum, item) => sum + (item.cantidad || 0) * (item.precio_unit || 0),
    0
  );

  // 4) Submit: crear orden + orden_detalle
  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);
    setMessage(null);

    if (!clienteId) {
      setError("Selecciona un cliente.");
      return;
    }

    if (items.some((it) => !it.producto_id || it.cantidad <= 0 || it.precio_unit < 0)) {
      setError("Revisa los productos, cantidades y precios.");
      return;
    }

    setLoading(true);

    try {
      // 4.1) Insert en orden
      const { data: ordenInsert, error: ordenError } = await supabase
        .from("orden")
        .insert([
          {
            cliente_id: clienteId,
            // fecha: se usa el default NOW() del schema
            canal,
            moneda,
            total: totalOrden, // NUMERIC(18,2)
          },
        ])
        .select("orden_id")
        .single();

      if (ordenError) throw ordenError;

      const ordenId = ordenInsert.orden_id;

      // 4.2) Insert en orden_detalle (una fila por item)
      const detallesToInsert = items.map((it) => ({
        orden_id: ordenId,
        producto_id: it.producto_id,
        cantidad: it.cantidad,
        precio_unit: it.precio_unit,
      }));

      const { error: detallesError } = await supabase
        .from("orden_detalle")
        .insert(detallesToInsert);

      if (detallesError) throw detallesError;

      setMessage("Venta creada correctamente ✅");
      setItems([{ producto_id: "", cantidad: 1, precio_unit: 0 }]);
      setCanal("WEB");
      setMoneda("USD");
      // opcional: limpiar clienteId si quieres
      // setClienteId("");
    } catch (err) {
      console.error(err);
      setError("Error creando la venta.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ maxWidth: 800, margin: "0 auto", padding: 24 }}>
      <h1>Crear Venta (Supabase)</h1>

      {loading && <p>Cargando...</p>}
      {error && <p style={{ color: "red" }}>{error}</p>}
      {message && <p style={{ color: "green" }}>{message}</p>}

      <form onSubmit={handleSubmit}>
        {/* Cliente */}
        <div style={{ marginBottom: 16 }}>
          <label>
            Cliente:
            <select
              value={clienteId}
              onChange={(e) => setClienteId(e.target.value)}
              style={{ marginLeft: 8 }}
            >
              <option value="">-- Seleccione --</option>
              {clientes.map((c) => (
                <option key={c.cliente_id} value={c.cliente_id}>
                  {c.nombre}
                </option>
              ))}
            </select>
          </label>
        </div>

        {/* Canal y moneda */}
        <div style={{ display: "flex", gap: 16, marginBottom: 16 }}>
          <label>
            Canal:
            <select
              value={canal}
              onChange={(e) => setCanal(e.target.value)}
              style={{ marginLeft: 8 }}
            >
              {CHANNELS.map((ch) => (
                <option key={ch} value={ch}>
                  {ch}
                </option>
              ))}
            </select>
          </label>

          <label>
            Moneda:
            <select
              value={moneda}
              onChange={(e) => setMoneda(e.target.value)}
              style={{ marginLeft: 8 }}
            >
              {CURRENCIES.map((cur) => (
                <option key={cur} value={cur}>
                  {cur}
                </option>
              ))}
            </select>
          </label>
        </div>

        {/* Items */}
        <h2>Productos</h2>
        {items.map((item, index) => (
          <div
            key={index}
            style={{
              display: "grid",
              gridTemplateColumns: "2fr 1fr 1fr auto",
              gap: 8,
              marginBottom: 8,
              alignItems: "center",
            }}
          >
            <select
              value={item.producto_id}
              onChange={(e) =>
                handleItemChange(index, "producto_id", e.target.value)
              }
            >
              <option value="">-- Producto --</option>
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
              onChange={(e) =>
                handleItemChange(index, "cantidad", e.target.value)
              }
              placeholder="Cantidad"
            />

            <input
              type="number"
              step="0.01"
              min="0"
              value={item.precio_unit}
              onChange={(e) =>
                handleItemChange(index, "precio_unit", e.target.value)
              }
              placeholder="Precio unit."
            />

            <button
              type="button"
              onClick={() => removeItem(index)}
              style={{ padding: "4px 8px" }}
              disabled={items.length === 1}
            >
              X
            </button>
          </div>
        ))}

        <button type="button" onClick={addItem} style={{ marginBottom: 16 }}>
          + Agregar producto
        </button>

        {/* Total */}
        <div style={{ marginBottom: 16 }}>
          <strong>
            Total: {totalOrden.toFixed(2)} {moneda}
          </strong>
        </div>

        <button type="submit" disabled={loading}>
          Guardar venta
        </button>
      </form>
    </div>
  );
}
