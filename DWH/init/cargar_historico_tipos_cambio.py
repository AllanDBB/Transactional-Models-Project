#!/usr/bin/env python3
"""
Script para cargar histórico de tipos de cambio CRC→USD desde BCCR
Usa el servicio web SOAP del BCCR
"""

import requests
import xml.etree.ElementTree as ET
import pyodbc
import datetime
import os
import sys
import time
import logging
import platform
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BCCRExchangeRateLoader:
    """Carga tipos de cambio desde BCCR al DWH"""
    
    def __init__(self):
        # Variables BCCR
        self.bccr_user = os.getenv("BCCR_USER", "guest@bccr.fi.cr")
        self.bccr_password = os.getenv("BCCR_PASSWORD")
        
        # Variables SQL Server
        self.server = os.getenv("serverenv", "localhost")
        self.database = os.getenv("databaseenv", "MSSQL_DW")
        self.username = os.getenv("usernameenv", "sa")
        self.password = os.getenv("passwordenv")
        
        # Driver ODBC
        if platform.system() == "Windows":
            self.driver = "ODBC Driver 17 for SQL Server"
        else:
            self.driver = "ODBC Driver 18 for SQL Server"
        
        # BCCR SOAP
        self.base_url = "https://gee.bccr.fi.cr/Indicadores/Suscripciones/WS/wsindicadoreseconomicos.asmx"
        self.indicador = "317"  # Tipo de cambio de referencia
    
    def get_exchange_rate_data(self, start_date, end_date):
        """Obtiene datos de tipos de cambio desde BCCR usando SOAP"""
        
        soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <ObtenerIndicadoresEconomicosXML xmlns="http://ws.sdde.bccr.fi.cr">
      <Indicador>{self.indicador}</Indicador>
      <FechaInicio>{start_date.strftime('%d/%m/%Y')}</FechaInicio>
      <FechaFinal>{end_date.strftime('%d/%m/%Y')}</FechaFinal>
      <Nombre>{self.bccr_user}</Nombre>
      <SubNiveles>N</SubNiveles>
      <CorreoElectronico>{self.bccr_user}</CorreoElectronico>
      <Token>{self.bccr_password}</Token>
    </ObtenerIndicadoresEconomicosXML>
  </soap:Body>
</soap:Envelope>"""

        headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': '"http://ws.sdde.bccr.fi.cr/ObtenerIndicadoresEconomicosXML"'
        }
        
        logger.info(f"Consultando BCCR desde {start_date} hasta {end_date}")
        
        try:
            response = requests.post(self.base_url, data=soap_body, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parsear XML respuesta SOAP
            root = ET.fromstring(response.content)
            
            # Buscar resultado XML dentro de SOAP
            soap_result = root.find('.//{http://ws.sdde.bccr.fi.cr}ObtenerIndicadoresEconomicosXMLResult')
            
            if soap_result is not None and soap_result.text:
                inner_xml = soap_result.text
                inner_root = ET.fromstring(inner_xml)
                
                # Extraer tasas
                exchange_rates = []
                for datos in inner_root.findall('.//INGC011_CAT_INDICADORECONOMIC'):
                    fecha_elem = datos.find('DES_FECHA')
                    valor_elem = datos.find('NUM_VALOR')
                    
                    if fecha_elem is not None and valor_elem is not None:
                        fecha_str = fecha_elem.text
                        valor_str = valor_elem.text
                        
                        if fecha_str and valor_str:
                            try:
                                fecha = datetime.datetime.strptime(fecha_str, '%Y-%m-%dT%H:%M:%S%z').date()
                                valor = float(valor_str)
                                exchange_rates.append({'fecha': fecha, 'tasa': valor})
                            except ValueError as e:
                                logger.warning(f"Error parsing date/value: {e}")
                                continue
                
                logger.info(f"✅ Obtenidos {len(exchange_rates)} registros del BCCR")
                return exchange_rates
            else:
                logger.warning("❌ No se encontró resultado XML en la respuesta SOAP")
                return []
        
        except requests.RequestException as e:
            logger.error(f"❌ Error al obtener datos del BCCR: {e}")
            return []
        except ET.ParseError as e:
            logger.error(f"❌ Error al parsear XML: {e}")
            return []
    
    def connect_to_database(self):
        """Conecta a SQL Server DWH"""
        try:
            if self.username and self.password:
                connection_string = (
                    f'DRIVER={{{self.driver}}};'
                    f'SERVER={self.server};'
                    f'DATABASE={self.database};'
                    f'UID={self.username};'
                    f'PWD={self.password};'
                    'TrustServerCertificate=yes;'
                )
                logger.info(f"Conectando a SQL: {self.server}/{self.database}")
            else:
                connection_string = (
                    f'DRIVER={{{self.driver}}};'
                    f'SERVER={self.server};'
                    f'DATABASE={self.database};'
                    'Trusted_Connection=yes;'
                    'TrustServerCertificate=yes;'
                )
                logger.info(f"Conectando con Windows auth: {self.server}/{self.database}")
            
            connection = pyodbc.connect(connection_string)
            logger.info("✅ Conexión a base de datos exitosa")
            return connection
        except pyodbc.Error as e:
            logger.error(f"❌ Error conectando a BD: {e}")
            return None
    
    def insert_exchange_rate(self, fecha, tasa):
        """Inserta o actualiza tasa en DimExchangeRate"""
        connection = self.connect_to_database()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            
            # Verificar si existe
            cursor.execute("""
                SELECT rate FROM DimExchangeRate 
                WHERE fromCurrency = 'CRC' 
                AND toCurrency = 'USD' 
                AND date = ?
            """, fecha)
            existing = cursor.fetchone()
            
            if existing:
                # Actualizar
                cursor.execute("""
                    UPDATE DimExchangeRate 
                    SET rate = ? 
                    WHERE fromCurrency = 'CRC' 
                    AND toCurrency = 'USD' 
                    AND date = ?
                """, tasa, fecha)
            else:
                # Insertar
                cursor.execute("""
                    INSERT INTO DimExchangeRate (fromCurrency, toCurrency, date, rate)
                    VALUES ('CRC', 'USD', ?, ?)
                """, fecha, tasa)
            
            connection.commit()
            return True
            
        except pyodbc.Error as e:
            logger.error(f"❌ Error en BD: {e}")
            connection.rollback()
            return False
        finally:
            connection.close()
    
    def populate_historical_data(self):
        """Carga 3 años de histórico de tipos de cambio"""
        
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=3*365)
        
        print("\n" + "=" * 80)
        print("CARGANDO HISTORICO DE TIPOS DE CAMBIO DESDE BCCR")
        print("=" * 80)
        print(f"\nPeríodo: {start_date} a {end_date}")
        print(f"Token: {self.bccr_password}\n")
        
        logger.info(f"Obteniendo datos históricos desde {start_date} hasta {end_date}")
        
        # Obtener de 180 en 180 días para no sobrecargar API
        current_date = start_date
        total_insertados = 0
        
        while current_date < end_date:
            chunk_end = min(current_date + datetime.timedelta(days=180), end_date)
            
            logger.info(f"Procesando chunk: {current_date} a {chunk_end}")
            exchange_rates = self.get_exchange_rate_data(current_date, chunk_end)
            
            for rate_data in exchange_rates:
                if self.insert_exchange_rate(rate_data['fecha'], rate_data['tasa']):
                    total_insertados += 1
            
            current_date = chunk_end + datetime.timedelta(days=1)
            time.sleep(2)  # Esperar entre requests
        
        print(f"\n{'=' * 80}")
        print(f"[✅] CARGA COMPLETADA: {total_insertados} registros insertados")
        print(f"{'=' * 80}\n")
        
        logger.info("Población de datos históricos completada")
    
    def update_current_rate(self):
        """Actualiza el tipo de cambio actual"""
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        
        logger.info(f"Actualizando tipo de cambio para hoy")
        
        exchange_rates = self.get_exchange_rate_data(yesterday, today)
        
        if exchange_rates:
            latest_rate = max(exchange_rates, key=lambda x: x['fecha'])
            if self.insert_exchange_rate(latest_rate['fecha'], latest_rate['tasa']):
                logger.info(f"✅ Tipo de cambio actualizado: {latest_rate['tasa']}")
            else:
                logger.error("❌ Error actualizando tipo de cambio")
        else:
            logger.warning("⚠️  No se pudo obtener el tipo de cambio actual")


def main():
    loader = BCCRExchangeRateLoader()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "populate":
            loader.populate_historical_data()
        elif command == "update-current":
            loader.update_current_rate()
        else:
            print("Comandos disponibles:")
            print("  python cargar_historico_tipos_cambio.py populate")
            print("  python cargar_historico_tipos_cambio.py update-current")
    else:
        loader.populate_historical_data()


if __name__ == "__main__":
    main()

