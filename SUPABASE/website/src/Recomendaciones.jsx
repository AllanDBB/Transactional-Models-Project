import { useState, useEffect } from 'react';
import { getProductRecommendations } from './recomendacionesService';
import './Recomendaciones.css';

export default function Recomendaciones({ productoId, dwhProductId }) {
  const [recomendaciones, setRecomendaciones] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    if (visible && dwhProductId) {
      cargarRecomendaciones();
    }
  }, [visible, dwhProductId]);

  const cargarRecomendaciones = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const result = await getProductRecommendations(dwhProductId, 5);
      
      if (result.success && result.recomendaciones?.length > 0) {
        setRecomendaciones(result.recomendaciones);
      } else {
        setError('No hay recomendaciones disponibles');
      }
    } catch (err) {
      setError('Error al cargar recomendaciones');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  if (!dwhProductId) {
    return null; // No mostrar nada si no hay equivalencia con DWH
  }

  return (
    <div className="recomendaciones-widget">
      <button 
        className="rec-toggle"
        onClick={() => setVisible(!visible)}
      >
        {visible ? '▼' : '▶'} Ver recomendaciones
      </button>

      {visible && (
        <div className="rec-content">
          {loading && <p className="rec-loading">Cargando...</p>}
          
          {error && <p className="rec-error">{error}</p>}
          
          {!loading && !error && recomendaciones.length > 0 && (
            <div className="rec-list">
              <p className="rec-title">Clientes que compraron esto también compraron:</p>
              <ul>
                {recomendaciones.map((rec, idx) => (
                  <li key={idx}>
                    <strong>{rec.ConsequentNames}</strong>
                    <span className="rec-metrics">
                      Lift: {rec.Lift.toFixed(2)} · 
                      Confianza: {(rec.Confidence * 100).toFixed(1)}%
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
