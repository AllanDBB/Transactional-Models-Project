"""
Script para visualizar logs del ETL
Uso:
    python view_logs.py              # Ver último log
    python view_logs.py -a           # Ver todos los logs
    python view_logs.py -f <archivo> # Ver log específico
    python view_logs.py -e           # Ver solo errores del último log
"""
import sys
from pathlib import Path
import argparse

def get_latest_log():
    """Obtiene el archivo de log más reciente"""
    log_dir = Path(__file__).parent / 'logs'
    log_files = sorted(log_dir.glob('etl_mysql_*.log'), reverse=True)
    return log_files[0] if log_files else None

def view_log(log_file, errors_only=False):
    """Muestra el contenido de un log"""
    if not log_file.exists():
        print(f"ERROR: El archivo {log_file} no existe")
        return

    print(f"\n{'='*80}")
    print(f"LOG: {log_file.name}")
    print(f"{'='*80}\n")

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            if errors_only:
                if 'ERROR' in line or 'FAILED' in line or 'Falló' in line:
                    print(line.rstrip())
            else:
                print(line.rstrip())

def list_all_logs():
    """Lista todos los archivos de log disponibles"""
    log_dir = Path(__file__).parent / 'logs'
    log_files = sorted(log_dir.glob('etl_mysql_*.log'), reverse=True)

    if not log_files:
        print("No hay archivos de log disponibles")
        return

    print("\nArchivos de log disponibles:")
    print(f"{'='*80}")
    for i, log_file in enumerate(log_files, 1):
        size = log_file.stat().st_size / 1024  # KB
        print(f"{i}. {log_file.name} ({size:.2f} KB)")
    print(f"{'='*80}\n")

def main():
    parser = argparse.ArgumentParser(description='Visualizador de logs del ETL MySQL -> DWH')
    parser.add_argument('-a', '--all', action='store_true', help='Listar todos los logs')
    parser.add_argument('-f', '--file', type=str, help='Ver log específico')
    parser.add_argument('-e', '--errors', action='store_true', help='Mostrar solo errores')

    args = parser.parse_args()

    if args.all:
        list_all_logs()
        return

    if args.file:
        log_file = Path(__file__).parent / 'logs' / args.file
    else:
        log_file = get_latest_log()
        if not log_file:
            print("No hay archivos de log disponibles")
            return

    view_log(log_file, args.errors)

if __name__ == "__main__":
    main()
