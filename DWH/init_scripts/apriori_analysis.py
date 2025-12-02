"""
Análisis de Reglas de Asociación con Algoritmo Apriori
Extrae transacciones del DWH, aplica Apriori y guarda reglas en dwh.ProductAssociationRules
"""
import logging
import os
import sys
from datetime import datetime
from collections import defaultdict

import pandas as pd
import pymssql
from dotenv import load_dotenv
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/apriori_analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()


class AprioriAnalysis:
    """Análisis de reglas de asociación usando Apriori"""
    
    def __init__(self):
        self.server = os.getenv("serverenv", "localhost")
        self.database = os.getenv("databaseenv", "MSSQL_DW")
        self.username = os.getenv("usernameenv")
        self.password = os.getenv("passwordenv")
        
        # Parámetros del algoritmo Apriori
        self.min_support = float(os.getenv("APRIORI_MIN_SUPPORT", "0.01"))  # 1%
        self.min_confidence = float(os.getenv("APRIORI_MIN_CONFIDENCE", "0.3"))  # 30%
        self.min_lift = float(os.getenv("APRIORI_MIN_LIFT", "1.0"))
        
        logger.info(f"Parámetros Apriori: support={self.min_support}, confidence={self.min_confidence}, lift={self.min_lift}")
    
    def connect_to_database(self):
        """Conectar a SQL Server DWH"""
        try:
            server_parts = self.server.replace(",", ":").split(":")
            server_host = server_parts[0]
            server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433
            
            # Dentro de Docker usamos el nombre del servicio
            if server_host == "localhost":
                server_host = "sqlserver-dw"
                server_port = 1433
            
            logger.info(f"Conectando a {server_host}:{server_port}/{self.database}")
            
            connection = pymssql.connect(
                server=server_host,
                port=server_port,
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=30,
                as_dict=False
            )
            
            logger.info("Conexión exitosa a base de datos")
            return connection
        
        except pymssql.DatabaseError as e:
            logger.error(f"Error conectando a base de datos: {e}")
            return None
    
    def extract_transactions(self):
        """
        Extrae transacciones del DWH.
        Retorna: lista de transacciones, cada transacción es lista de product_ids
        """
        connection = self.connect_to_database()
        if not connection:
            return [], {}
        
        try:
            cursor = connection.cursor()
            
            # Extraer items por orden (transacción = orden)
            query = """
                SELECT 
                    o.id AS order_id,
                    p.id AS product_id,
                    p.name AS product_name
                FROM dwh.FactSales fs
                INNER JOIN dwh.DimOrder o ON o.id = fs.orderId
                INNER JOIN dwh.DimProduct p ON p.id = fs.productId
                WHERE fs.productCant > 0
                ORDER BY o.id, p.id
            """
            
            logger.info("Extrayendo transacciones desde FactSales...")
            cursor.execute(query)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning("No se encontraron transacciones en FactSales")
                return [], {}
            
            # Agrupar productos por orden
            transactions_dict = defaultdict(set)
            product_names = {}
            
            for order_id, product_id, product_name in rows:
                transactions_dict[order_id].add(product_id)
                product_names[product_id] = product_name
            
            # Convertir a lista de listas
            transactions = [list(items) for items in transactions_dict.values()]
            
            logger.info(f"Extraídas {len(transactions)} transacciones con {len(product_names)} productos únicos")
            
            # Estadísticas
            items_per_transaction = [len(t) for t in transactions]
            logger.info(f"Items por transacción - Min: {min(items_per_transaction)}, Max: {max(items_per_transaction)}, Promedio: {sum(items_per_transaction)/len(items_per_transaction):.2f}")
            
            return transactions, product_names
        
        except Exception as e:
            logger.error(f"Error extrayendo transacciones: {e}")
            return [], {}
        finally:
            connection.close()
    
    def run_apriori(self, transactions):
        """
        Ejecuta algoritmo Apriori sobre las transacciones.
        Retorna: DataFrame con reglas de asociación
        """
        if not transactions:
            logger.warning("No hay transacciones para analizar")
            return pd.DataFrame()
        
        try:
            logger.info("Ejecutando algoritmo Apriori...")
            
            # Convertir a formato one-hot encoding
            te = TransactionEncoder()
            te_ary = te.fit(transactions).transform(transactions)
            df_encoded = pd.DataFrame(te_ary, columns=te.columns_)
            
            logger.info(f"Matriz transaccional: {df_encoded.shape[0]} transacciones x {df_encoded.shape[1]} productos")
            
            # Fase 1: Encontrar itemsets frecuentes
            logger.info(f"Buscando itemsets frecuentes (min_support={self.min_support})...")
            frequent_itemsets = apriori(
                df_encoded,
                min_support=self.min_support,
                use_colnames=True,
                low_memory=True
            )
            
            if frequent_itemsets.empty:
                logger.warning("No se encontraron itemsets frecuentes. Considerar reducir min_support.")
                return pd.DataFrame()
            
            logger.info(f"Encontrados {len(frequent_itemsets)} itemsets frecuentes")
            
            # Mostrar itemsets de diferentes tamaños
            itemset_sizes = frequent_itemsets['itemsets'].apply(len).value_counts().sort_index()
            logger.info(f"Distribución de itemsets: {dict(itemset_sizes)}")
            
            # Fase 2: Generar reglas de asociación
            logger.info(f"Generando reglas de asociación (min_confidence={self.min_confidence})...")
            
            # Primero generar todas las reglas sin filtro de confidence
            try:
                all_rules = association_rules(
                    frequent_itemsets,
                    metric="confidence",
                    min_threshold=0.01  # Muy bajo para ver qué hay
                )
                logger.info(f"Total de reglas potenciales (confidence > 1%): {len(all_rules)}")
                
                if not all_rules.empty:
                    logger.info(f"  Confidence max: {all_rules['confidence'].max():.4f}")
                    logger.info(f"  Confidence min: {all_rules['confidence'].min():.4f}")
                    logger.info(f"  Confidence promedio: {all_rules['confidence'].mean():.4f}")
                
                # Ahora filtrar por el threshold configurado
                rules = all_rules[all_rules['confidence'] >= self.min_confidence]
            except Exception as e:
                logger.error(f"Error generando reglas: {e}")
                return pd.DataFrame()
            
            if rules.empty:
                logger.warning(f"No se generaron reglas con confidence >= {self.min_confidence}")
                logger.warning("Considera reducir APRIORI_MIN_CONFIDENCE en .env")
                return pd.DataFrame()
            
            # Filtrar por lift mínimo
            rules = rules[rules['lift'] >= self.min_lift]
            
            if rules.empty:
                logger.warning(f"No se encontraron reglas con lift >= {self.min_lift}")
                return pd.DataFrame()
            
            logger.info(f"Generadas {len(rules)} reglas de asociación")
            
            # Ordenar por lift descendente
            rules = rules.sort_values('lift', ascending=False)
            
            # Log de mejores reglas
            logger.info("\nTop 5 reglas de asociación:")
            for idx, row in rules.head(5).iterrows():
                logger.info(f"  {set(row['antecedents'])} => {set(row['consequents'])} | support={row['support']:.4f}, confidence={row['confidence']:.4f}, lift={row['lift']:.4f}")
            
            return rules
        
        except Exception as e:
            logger.error(f"Error ejecutando Apriori: {e}")
            return pd.DataFrame()
    
    def save_rules_to_database(self, rules, product_names):
        """
        Guarda las reglas de asociación en dwh.ProductAssociationRules.
        Marca reglas antiguas como inactivas y agrega las nuevas.
        """
        if rules.empty:
            logger.warning("No hay reglas para guardar")
            return
        
        connection = self.connect_to_database()
        if not connection:
            return
        
        try:
            cursor = connection.cursor()
            
            # 1. Marcar todas las reglas existentes como inactivas
            logger.info("Marcando reglas anteriores como inactivas...")
            cursor.execute("UPDATE dwh.ProductAssociationRules SET Activo = 0")
            connection.commit()
            
            # 2. Insertar nuevas reglas
            insert_query = """
                INSERT INTO dwh.ProductAssociationRules 
                (AntecedentProductIds, ConsequentProductIds, AntecedentNames, ConsequentNames, 
                 Support, Confidence, Lift, FechaCalculo, Activo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1)
            """
            
            inserted_count = 0
            
            for _, rule in rules.iterrows():
                # Convertir frozensets a listas de IDs
                antecedent_ids = list(rule['antecedents'])
                consequent_ids = list(rule['consequents'])
                
                # Crear strings separados por coma
                antecedent_str = ','.join(map(str, sorted(antecedent_ids)))
                consequent_str = ','.join(map(str, sorted(consequent_ids)))
                
                # Obtener nombres de productos
                antecedent_names = ', '.join([product_names.get(pid, f'ID:{pid}') for pid in antecedent_ids])
                consequent_names = ', '.join([product_names.get(pid, f'ID:{pid}') for pid in consequent_ids])
                
                # Truncar si es muy largo
                antecedent_names = antecedent_names[:1000]
                consequent_names = consequent_names[:1000]
                
                try:
                    cursor.execute(insert_query, (
                        antecedent_str,
                        consequent_str,
                        antecedent_names,
                        consequent_names,
                        float(rule['support']),
                        float(rule['confidence']),
                        float(rule['lift']),
                        datetime.now()
                    ))
                    inserted_count += 1
                
                except Exception as e:
                    logger.error(f"Error insertando regla {antecedent_str} => {consequent_str}: {e}")
                    continue
            
            connection.commit()
            logger.info(f"✓ {inserted_count} reglas de asociación guardadas en dwh.ProductAssociationRules")
            
            # 3. Estadísticas
            cursor.execute("SELECT COUNT(*) FROM dwh.ProductAssociationRules WHERE Activo = 1")
            active_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dwh.ProductAssociationRules WHERE Activo = 0")
            inactive_count = cursor.fetchone()[0]
            
            logger.info(f"Reglas activas: {active_count}, Reglas históricas: {inactive_count}")
        
        except Exception as e:
            logger.error(f"Error guardando reglas en base de datos: {e}")
            connection.rollback()
        finally:
            connection.close()
    
    def run_analysis(self):
        """Ejecuta el análisis completo de Apriori"""
        logger.info("=" * 80)
        logger.info("INICIANDO ANÁLISIS DE REGLAS DE ASOCIACIÓN (APRIORI)")
        logger.info("=" * 80)
        
        start_time = datetime.now()
        
        # 1. Extraer transacciones
        transactions, product_names = self.extract_transactions()
        
        if not transactions:
            logger.error("No se pudieron extraer transacciones. Abortando análisis.")
            return
        
        # 2. Ejecutar Apriori
        rules = self.run_apriori(transactions)
        
        if rules.empty:
            logger.warning("No se generaron reglas de asociación. Considerar ajustar parámetros.")
            return
        
        # 3. Guardar en base de datos
        self.save_rules_to_database(rules, product_names)
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"\n✓ Análisis Apriori completado en {elapsed_time:.2f} segundos")
        logger.info("=" * 80)


def main():
    """Punto de entrada principal"""
    import sys
    
    apriori = AprioriAnalysis()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "run":
            apriori.run_analysis()
        else:
            print(f"Comando desconocido: {command}")
            print("Uso: python apriori_analysis.py run")
    else:
        # Por defecto ejecutar análisis
        apriori.run_analysis()


if __name__ == "__main__":
    main()
