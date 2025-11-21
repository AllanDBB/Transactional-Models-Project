#!/usr/bin/env python3
"""
Script de verificación: Valida que el histórico de tipos de cambio está cargado
y que el ExchangeRateHelper funciona correctamente
"""

import sys
from pathlib import Path
from datetime import date, timedelta
import pyodbc

# Agregar rutas
sys.path.insert(0, str(Path(__file__).parent / 'shared'))
sys.path.insert(0, str(Path(__file__).parent / 'MSSQL' / 'etl'))

try:
    from ExchangeRateHelper import ExchangeRateHelper
    from config import DatabaseConfig
except ImportError as e:
    print(f"Error importando módulos: {e}")
    sys.exit(1)


def verificar_tabla_dwh():
    """Verifica que DimExchangeRate exista y tenga datos"""
    print("\n[1] VERIFICANDO TABLA DimExchangeRate")
    print("─" * 70)
    
    try:
        conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
        cursor = conn.cursor()
        
        # Verificar que la tabla existe
        cursor.execute("""
            IF OBJECT_ID('DimExchangeRate', 'U') IS NULL
                SELECT 0
            ELSE
                SELECT 1
        """)
        
        existe = cursor.fetchone()[0]
        
        if not existe:
            print("❌ Tabla DimExchangeRate NO existe")
            cursor.close()
            conn.close()
            return False
        
        print("✅ Tabla DimExchangeRate existe")
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM DimExchangeRate")
        total = cursor.fetchone()[0]
        print(f"✅ Total de registros: {total:,}")
        
        if total == 0:
            print("⚠️  Tabla está vacía - ejecutar sp_cargar_historico_tipos_cambio")
            cursor.close()
            conn.close()
            return False
        
        # Verificar monedas disponibles
        cursor.execute("""
            SELECT DISTINCT fromCurrency 
            FROM DimExchangeRate 
            ORDER BY fromCurrency
        """)
        
        monedas = [row[0] for row in cursor.fetchall()]
        print(f"✅ Monedas disponibles: {', '.join(monedas)}")
        
        # Rango de fechas
        cursor.execute("""
            SELECT MIN(date), MAX(date)
            FROM DimExchangeRate
        """)
        
        fecha_min, fecha_max = cursor.fetchone()
        print(f"✅ Período: {fecha_min} a {fecha_max}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def verificar_sp_existe():
    """Verifica que el SP exista"""
    print("\n[2] VERIFICANDO STORED PROCEDURE")
    print("─" * 70)
    
    try:
        conn = pyodbc.connect(DatabaseConfig.get_dw_connection_string())
        cursor = conn.cursor()
        
        cursor.execute("""
            IF OBJECT_ID('dbo.sp_cargar_historico_tipos_cambio', 'P') IS NULL
                SELECT 0
            ELSE
                SELECT 1
        """)
        
        existe = cursor.fetchone()[0]
        
        if existe:
            print("✅ SP dbo.sp_cargar_historico_tipos_cambio existe")
        else:
            print("❌ SP dbo.sp_cargar_historico_tipos_cambio NO existe")
            print("   Ejecutar: DWH/init/06-sp_cargar_historico_tipos_cambio.sql")
        
        cursor.close()
        conn.close()
        return existe
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def verificar_exchange_rate_helper():
    """Verifica que ExchangeRateHelper funciona"""
    print("\n[3] VERIFICANDO ExchangeRateHelper")
    print("─" * 70)
    
    try:
        with ExchangeRateHelper(DatabaseConfig.get_dw_connection_string()) as helper:
            
            # Test 1: Obtener tasa reciente
            print("  Test 1: Obtener tasa reciente (CRC → USD)")
            tasa = helper.obtener_tasa_reciente('CRC', 'USD')
            if tasa:
                print(f"  ✅ Tasa CRC → USD: {tasa:.6f}")
            else:
                print("  ❌ No se obtuvo tasa")
                return False
            
            # Test 2: Convertir monto
            print("  Test 2: Convertir 100,000 CRC a USD")
            usd = helper.convertir_monto(100000, 'CRC', 'USD')
            if usd:
                print(f"  ✅ 100,000 CRC = {usd:.2f} USD")
            else:
                print("  ❌ Error en conversión")
                return False
            
            # Test 3: Obtener tasa para fecha específica
            print("  Test 3: Obtener tasa para fecha específica")
            fecha_test = date.today() - timedelta(days=30)
            tasa_fecha = helper.obtener_tasa_para_fecha('EUR', 'USD', fecha_test)
            if tasa_fecha:
                print(f"  ✅ EUR → USD ({fecha_test}): {tasa_fecha:.6f}")
            else:
                print(f"  ⚠️  No se encontró tasa exacta para {fecha_test}")
            
            # Test 4: Obtener rango
            print("  Test 4: Obtener rango de tasas")
            fecha_inicio = date.today() - timedelta(days=7)
            fecha_fin = date.today()
            tasas = helper.obtener_rango_tasas('MXN', 'USD', fecha_inicio, fecha_fin)
            print(f"  ✅ Tasas en últimos 7 días: {len(tasas)} registros")
            
            print("✅ ExchangeRateHelper funciona correctamente")
            return True
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def mostrar_ejemplos_consulta():
    """Muestra ejemplos de consultas SQL"""
    print("\n[4] EJEMPLOS DE CONSULTAS SQL")
    print("─" * 70)
    
    ejemplos = [
        ("Tasa específica para hoy (CRC → USD)", """
    SELECT rate 
    FROM DimExchangeRate
    WHERE fromCurrency = 'CRC'
    AND toCurrency = 'USD'
    AND date = CAST(GETDATE() AS DATE)
        """),
        
        ("Tasa más reciente disponible", """
    SELECT TOP 1 rate, date
    FROM DimExchangeRate
    WHERE fromCurrency = 'CRC'
    AND toCurrency = 'USD'
    ORDER BY date DESC
        """),
        
        ("Comparar tasas por moneda (últimos 30 días)", """
    SELECT 
        fromCurrency,
        COUNT(*) AS dias,
        MIN(rate) AS minima,
        MAX(rate) AS maxima,
        AVG(rate) AS promedio
    FROM DimExchangeRate
    WHERE date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
    GROUP BY fromCurrency
    ORDER BY fromCurrency
        """),
    ]
    
    for i, (titulo, query) in enumerate(ejemplos, 1):
        print(f"\n  Ejemplo {i}: {titulo}")
        print(f"  {'-' * 66}")
        for line in query.strip().split('\n'):
            print(f"  {line}")


def generar_reporte():
    """Genera reporte de verificación"""
    print("\n" + "=" * 70)
    print("VERIFICACION: HISTORICO DE TIPOS DE CAMBIO (PASO 1)")
    print("=" * 70)
    
    resultados = {
        'Tabla DimExchangeRate': verificar_tabla_dwh(),
        'Stored Procedure': verificar_sp_existe(),
        'ExchangeRateHelper': verificar_exchange_rate_helper(),
    }
    
    mostrar_ejemplos_consulta()
    
    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    
    todos_ok = True
    for componente, resultado in resultados.items():
        status = "✅ OK" if resultado else "❌ FALLO"
        print(f"{componente:30} {status}")
        if not resultado:
            todos_ok = False
    
    print("=" * 70)
    
    if todos_ok:
        print("\n✅ VERIFICACION EXITOSA")
        print("   El PASO 1 (Llenar tabla) está completo y listo")
        print("\n📋 Próximos pasos:")
        print("   PASO 2: Crear job para actualización automática")
        print("   PASO 3: Actualizar ETLs para usar ExchangeRateHelper")
        return 0
    else:
        print("\n❌ VERIFICACION CON ERRORES")
        print("   Revisar los componentes que fallaron")
        return 1


if __name__ == "__main__":
    sys.exit(generar_reporte())
