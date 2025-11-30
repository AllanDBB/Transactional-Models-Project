
import logging
import os
import platform
import time
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

import pymssql
import requests
from dotenv import load_dotenv

# Cargar variables de entorno (.env ya esta en la imagen)
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class BCCRExchangeRate:
    def __init__(self):
        self.bccr_user = os.getenv("BCCR_USER")
        self.bccr_password = os.getenv("BCCR_PASSWORD")

        self.server = os.getenv("serverenv", "localhost")
        self.database = os.getenv("databaseenv", "MSSQL_DW")
        self.username = os.getenv("usernameenv")
        self.password = os.getenv("passwordenv")

        # Referencia informativa; pymssql no usa drivers ODBC
        self.driver = "ODBC Driver 17 for SQL Server" if platform.system() == "Windows" else "ODBC Driver 18 for SQL Server"

        self.base_url = "https://gee.bccr.fi.cr/Indicadores/Suscripciones/WS/wsindicadoreseconomicos.asmx"
        self.indicador = "317"  # compra del dolar

    def get_exchange_rate_data(self, start_date, end_date):
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
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"http://ws.sdde.bccr.fi.cr/ObtenerIndicadoresEconomicosXML"',
        }

        logging.info(f"Consultando BCCR desde {start_date} hasta {end_date}")

        try:
            response = requests.post(self.base_url, data=soap_body, headers=headers)
            response.raise_for_status()

            root = ET.fromstring(response.content)
            soap_result = root.find(".//{http://ws.sdde.bccr.fi.cr}ObtenerIndicadoresEconomicosXMLResult")

            if soap_result is None or not soap_result.text:
                logging.warning("No se encontro resultado XML en la respuesta SOAP")
                return []

            inner_root = ET.fromstring(soap_result.text)

            exchange_rates = []
            for datos in inner_root.findall(".//INGC011_CAT_INDICADORECONOMIC"):
                fecha_str = datos.find("DES_FECHA").text if datos.find("DES_FECHA") is not None else None
                valor_str = datos.find("NUM_VALOR").text if datos.find("NUM_VALOR") is not None else None

                if fecha_str and valor_str:
                    try:
                        fecha = datetime.strptime(fecha_str, "%Y-%m-%dT%H:%M:%S%z").date()
                        valor = float(valor_str)
                        exchange_rates.append({"fecha": fecha, "tipo_cambio": valor})
                    except ValueError as e:
                        logging.warning(f"Error parsing date/value: {e}")
                        continue

            logging.info(f"Obtenidos {len(exchange_rates)} registros de tipos de cambio")
            return exchange_rates

        except requests.RequestException as e:
            logging.error(f"Error al obtener datos del BCCR: {e}")
            return []
        except ET.ParseError as e:
            logging.error(f"Error al parsear XML: {e}")
            return []

    def connect_to_database(self):
        try:
            logging.info(f"Conectando a SQL Server: {self.server}/{self.database}")
            server_parts = self.server.replace(",", ":").split(":")
            server_host = server_parts[0]
            server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433

            # Dentro de Docker usamos el nombre del servicio
            if server_host == "localhost":
                server_host = "sqlserver-dw"
                server_port = 1433

            logging.info(f"Intentando conectar a {server_host}:{server_port}/{self.database}")

            connection = pymssql.connect(
                server=server_host,
                port=server_port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=10,
                as_dict=False,
            )

            logging.info("Conexion a base de datos exitosa (pymssql/FreeTDS)")
            return connection

        except pymssql.DatabaseError as e:
            logging.error(f"Error de base de datos: {e}")
            return None
        except Exception as e:
            logging.error(f"Error conectando a la base de datos: {e}")
            return None

    def upsert_exchange_rates(self, rates):
        """
        Inserta o actualiza tipos de cambio en staging.tipo_cambio en batch.
        Espera lista de dicts con keys: fecha (date) y tipo_cambio (float).
        """
        if not rates:
            logging.info("No hay registros para insertar/actualizar")
            return

        connection = self.connect_to_database()
        if not connection:
            return

        try:
            cursor = connection.cursor()
            sql = """
MERGE staging.tipo_cambio AS target
USING (VALUES (%s, 'CRC', 'USD', %s, 'BCCR')) AS src(fecha, de_moneda, a_moneda, tasa, fuente)
ON target.fecha = src.fecha AND target.de_moneda = src.de_moneda AND target.a_moneda = src.a_moneda
WHEN MATCHED THEN
    UPDATE SET tasa = src.tasa, fecha_actualizacion = GETDATE(), fuente = src.fuente
WHEN NOT MATCHED THEN
    INSERT (fecha, de_moneda, a_moneda, tasa, fuente)
    VALUES (src.fecha, src.de_moneda, src.a_moneda, src.tasa, src.fuente);
"""
            params = []
            for r in rates:
                fecha = r["fecha"]
                if isinstance(fecha, str):
                    fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
                params.append((fecha, float(r["tipo_cambio"])))

            cursor.executemany(sql, params)
            connection.commit()
            logging.info(f"Upsert de tipos de cambio completado: {len(params)} registros")
        except Exception as e:
            logging.error(f"Error actualizando tipos de cambio: {e}")
            connection.rollback()
        finally:
            connection.close()

    def populate_historical_data(self):
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=3 * 365)  # 3 anos atras

        logging.info(f"Obteniendo datos historicos desde {start_date} hasta {end_date}")

        current_date = start_date
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=180), end_date)
            logging.info(f"Procesando chunk: {current_date} a {chunk_end}")

            exchange_rates = self.get_exchange_rate_data(current_date, chunk_end)
            self.upsert_exchange_rates(exchange_rates)

            current_date = chunk_end + timedelta(days=1)
            time.sleep(2)

        logging.info("Poblacion de datos historicos completada")
        self.promote_exchange_rates_to_dim()

    def update_current_rate(self):
        today = datetime.today().date()
        yesterday = today - timedelta(days=1)

        logging.info(f"Actualizando tipo de cambio para {today}")

        exchange_rates = self.get_exchange_rate_data(yesterday, today)

        if exchange_rates:
            latest_rate = max(exchange_rates, key=lambda x: x["fecha"])
            self.upsert_exchange_rates([latest_rate])
            logging.info(f"Tipo de cambio actualizado: {latest_rate['tipo_cambio']}")
            self.promote_exchange_rates_to_dim()
        else:
            logging.warning("No se pudo obtener el tipo de cambio actual")

    def promote_exchange_rates_to_dim(self):
        """
        Copia/actualiza staging.tipo_cambio -> dwh.DimExchangeRate con MERGE.
        """
        connection = self.connect_to_database()
        if not connection:
            return

        try:
            cursor = connection.cursor()
            sql = """
MERGE dwh.DimExchangeRate AS target
USING (
    SELECT fecha, de_moneda, a_moneda, tasa
    FROM staging.tipo_cambio
) AS src(fecha, de_moneda, a_moneda, tasa)
ON target.date = src.fecha AND target.fromCurrency = src.de_moneda AND target.toCurrency = src.a_moneda
WHEN MATCHED THEN
    UPDATE SET rate = src.tasa
WHEN NOT MATCHED THEN
    INSERT (toCurrency, fromCurrency, date, rate)
    VALUES (src.a_moneda, src.de_moneda, src.fecha, src.tasa);
"""
            cursor.execute(sql)
            connection.commit()
            logging.info("DimExchangeRate actualizada desde staging.tipo_cambio")
        except Exception as e:
            logging.error(f"Error promoviendo tipos de cambio a DimExchangeRate: {e}")
            connection.rollback()
        finally:
            connection.close()

    def start_scheduler(self, custom_hour=None, custom_minute=None):
        import subprocess
        import sys

        target_hour = custom_hour if custom_hour is not None else 5
        target_minute = custom_minute if custom_minute is not None else 0

        if not (0 <= target_hour <= 23):
            logging.error(f"Hora invalida: {target_hour}. Debe estar entre 0-23")
            return
        if not (0 <= target_minute <= 59):
            logging.error(f"Minuto invalido: {target_minute}. Debe estar entre 0-59")
            return

        script_path = os.path.abspath(__file__)
        python_path = sys.executable

        if platform.system() == "Windows":
            self._create_windows_task(target_hour, target_minute, python_path, script_path)
        else:
            self._create_unix_cron(target_hour, target_minute, python_path, script_path)

    def _create_windows_task(self, hour, minute, python_path, script_path):
        import subprocess

        task_name = "BCCR_Exchange_Rate_Update"

        try:
            subprocess.run(["schtasks", "/delete", "/tn", task_name, "/f"], capture_output=True, text=True)

            command = f'"{python_path}" "{script_path}" update-current'
            time_str = f"{hour:02d}:{minute:02d}"

            result = subprocess.run(
                [
                    "schtasks",
                    "/create",
                    "/tn",
                    task_name,
                    "/tr",
                    command,
                    "/sc",
                    "daily",
                    "/st",
                    time_str,
                    "/ru",
                    "SYSTEM",
                    "/rl",
                    "HIGHEST",
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode == 0:
                logging.info(f"Job creado para las {hour:02d}:{minute:02d}")
            else:
                logging.error(f"Error creando tarea de Windows: {result.stderr}")

        except Exception as e:
            logging.error(f"Error configurando Task Scheduler: {e}")

    def _create_unix_cron(self, hour, minute, python_path, script_path):
        import subprocess
        import tempfile

        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            current_cron = result.stdout if result.returncode == 0 else ""

            cron_line = f"{minute} {hour} * * * {python_path} {script_path} update-current\n"
            cron_comment = "# BCCR Exchange Rate Update\n"

            lines = current_cron.split("\n")
            filtered_lines = [line for line in lines if not (script_path in line and "update-current" in line)]

            new_cron = "\n".join(filtered_lines).strip()
            if new_cron:
                new_cron += "\n"
            new_cron += cron_comment + cron_line

            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cron") as f:
                f.write(new_cron)
                temp_file = f.name

            result = subprocess.run(["crontab", temp_file], capture_output=True, text=True)
            os.unlink(temp_file)

            if result.returncode == 0:
                logging.info(f"Job creado para las {hour:02d}:{minute:02d}")
            else:
                logging.error(f"Error creando cron job: {result.stderr}")

        except Exception as e:
            logging.error(f"Error configurando cron: {e}")

    def remove_scheduler(self):
        if platform.system() == "Windows":
            self._remove_windows_task()
        else:
            self._remove_unix_cron()

    def _remove_windows_task(self):
        import subprocess

        task_name = "BCCR_Exchange_Rate_Update"

        try:
            result = subprocess.run(["schtasks", "/delete", "/tn", task_name, "/f"], capture_output=True, text=True)

            if result.returncode == 0:
                logging.info(f"Tarea de Windows eliminada: '{task_name}'")
            else:
                logging.warning(f"La tarea '{task_name}' no existia o no se pudo eliminar")

        except Exception as e:
            logging.error(f"Error eliminando tarea de Windows: {e}")

    def _remove_unix_cron(self):
        import subprocess
        import tempfile

        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            current_cron = result.stdout if result.returncode == 0 else ""

            lines = current_cron.split("\n")
            filtered_lines = []
            script_path = os.path.abspath(__file__)

            for line in lines:
                if not (script_path in line and "update-current" in line):
                    if not line.strip().startswith("# BCCR Exchange Rate Update"):
                        filtered_lines.append(line)

            new_cron = "\n".join(filtered_lines).strip()

            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cron") as f:
                f.write(new_cron + "\n" if new_cron else "")
                temp_file = f.name

            result = subprocess.run(["crontab", temp_file], capture_output=True, text=True)
            os.unlink(temp_file)

            if result.returncode == 0:
                logging.info("Cron job de BCCR eliminado")
            else:
                logging.error(f"Error eliminando cron job: {result.stderr}")

        except Exception as e:
            logging.error(f"Error eliminando cron: {e}")


def main():
    import sys

    bccr = BCCRExchangeRate()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "populate":
            bccr.populate_historical_data()
        elif command == "update-current":
            bccr.update_current_rate()
        elif command == "scheduler":
            if len(sys.argv) >= 3:
                time_arg = sys.argv[2]
                try:
                    if ":" in time_arg:
                        hour, minute = map(int, time_arg.split(":"))
                        bccr.start_scheduler(custom_hour=hour, custom_minute=minute)
                    else:
                        print("Use formato HH:MM")
                except ValueError:
                    print("Use formato HH:MM")
            else:
                bccr.start_scheduler()
        elif command == "remove-scheduler":
            bccr.remove_scheduler()
        else:
            print("Comandos disponibles: populate, scheduler, remove-scheduler")
    else:
        print("  python bccr_exchange_rate.py populate")
        print("  python bccr_exchange_rate.py scheduler")
        print("  python bccr_exchange_rate.py scheduler HH:MM")
        print("  python bccr_exchange_rate.py remove-scheduler")


if __name__ == "__main__":
    main()
