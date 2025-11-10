"""
ETL Process for MongoDB
Extract -> Transform -> Load
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Agregar paths
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract.extract_data import extract_from_sources
from transform.transform_data import transform_data
from load.load_data import load_to_mongodb

# Cargar variables de entorno
load_dotenv()


def main():
    """
    Orquestador principal del ETL
    """
    print("=" * 50)
    print("MongoDB ETL Process Started")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 50)
    
    try:
        # 1. EXTRACT
        print("\n[1/3] EXTRACT: Extracting data from sources...")
        raw_data = extract_from_sources()
        print(f"✓ Extracted {len(raw_data)} records")
        
        # 2. TRANSFORM
        print("\n[2/3] TRANSFORM: Transforming data...")
        transformed_data = transform_data(raw_data)
        print(f"✓ Transformed {len(transformed_data)} records")
        
        # 3. LOAD
        print("\n[3/3] LOAD: Loading data to MongoDB...")
        result = load_to_mongodb(transformed_data)
        print(f"✓ Loaded {result['inserted']} records to MongoDB")
        
        print("\n" + "=" * 50)
        print("ETL Process Completed Successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ Error in ETL Process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
