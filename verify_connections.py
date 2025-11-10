#!/usr/bin/env python3
"""
Script de verificación de conectividad para todas las bases de datos
"""

import os
from datetime import datetime

def print_header(title):
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)

def check_mongodb():
    print_header("MongoDB")
    try:
        from pymongo import MongoClient
        client = MongoClient(
            'mongodb://admin:admin123@localhost:27017/',
            serverSelectionTimeoutMS=3000
        )
        client.admin.command('ping')
        db_list = client.list_database_names()
        print("✅ MongoDB - CONECTADO")
        print(f"   Bases de datos: {', '.join(db_list)}")
        return True
    except Exception as e:
        print(f"❌ MongoDB - ERROR: {str(e)}")
        return False

def check_mysql():
    print_header("MySQL")
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='user',
            password='user123',
            database='transactional_db',
            connection_timeout=3
        )
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ MySQL - CONECTADO")
        print(f"   Versión: {version[0]}")
        return True
    except Exception as e:
        print(f"❌ MySQL - ERROR: {str(e)}")
        return False

def check_mssql():
    print_header("MS SQL Server")
    try:
        import pyodbc
        conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=localhost,1433;'
            'UID=sa;'
            'PWD=YourStrong@Password123;'
            'Timeout=3;'
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ MS SQL Server - CONECTADO")
        print(f"   Versión: {version[0][:50]}...")
        return True
    except Exception as e:
        print(f"❌ MS SQL Server - ERROR: {str(e)}")
        return False

def check_neo4j():
    print_header("Neo4j")
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            'bolt://localhost:7687',
            auth=('neo4j', 'password123')
        )
        with driver.session() as session:
            result = session.run("RETURN 1")
            result.single()
        driver.close()
        print("✅ Neo4j - CONECTADO")
        return True
    except Exception as e:
        print(f"❌ Neo4j - ERROR: {str(e)}")
        return False

def check_postgresql():
    print_header("PostgreSQL")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            user='postgres',
            password='postgres123',
            database='transactional_db',
            connect_timeout=3
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL - CONECTADO")
        print(f"   Versión: {version[0][:50]}...")
        return True
    except Exception as e:
        print(f"❌ PostgreSQL - ERROR: {str(e)}")
        return False

def check_clickhouse():
    print_header("ClickHouse")
    try:
        import clickhouse_connect
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='default',
            password='clickhouse123'
        )
        result = client.command('SELECT version()')
        print("✅ ClickHouse - CONECTADO")
        print(f"   Versión: {result}")
        return True
    except Exception as e:
        print(f"❌ ClickHouse - ERROR: {str(e)}")
        return False

def main():
    print("\n")
    print("╔" + "="*58 + "╗")
    print("║" + " "*10 + "VERIFICACIÓN DE CONECTIVIDAD" + " "*20 + "║")
    print("║" + f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + " "*38 + "║")
    print("╚" + "="*58 + "╝")
    
    results = {
        'MongoDB': check_mongodb(),
        'MySQL': check_mysql(),
        'MS SQL Server': check_mssql(),
        'Neo4j': check_neo4j(),
        'PostgreSQL': check_postgresql(),
        'ClickHouse': check_clickhouse()
    }
    
    print_header("RESUMEN")
    total = len(results)
    connected = sum(results.values())
    
    for db, status in results.items():
        icon = "✅" if status else "❌"
        print(f"{icon} {db}")
    
    print(f"\nTotal: {connected}/{total} servicios conectados")
    
    if connected == total:
        print("\n🎉 ¡Todos los servicios están funcionando correctamente!")
    else:
        print(f"\n⚠️  {total - connected} servicio(s) no conectado(s)")
        print("Verifica que los contenedores estén ejecutándose:")
        print("  docker ps")
    
    print("\n")

if __name__ == "__main__":
    # Verificar dependencias
    print("Verificando dependencias de Python...")
    required = [
        'pymongo',
        'mysql-connector-python',
        'pyodbc',
        'neo4j',
        'psycopg2-binary',
        'clickhouse-connect'
    ]
    
    missing = []
    for package in required:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing.append(package)
    
    if missing:
        print("\n⚠️  Faltan las siguientes dependencias:")
        for pkg in missing:
            print(f"  - {pkg}")
        print("\nInstalar con:")
        print(f"  pip install {' '.join(missing)}")
        print("\nO instalar todas:")
        print("  pip install pymongo mysql-connector-python pyodbc neo4j psycopg2-binary clickhouse-connect")
    else:
        main()
