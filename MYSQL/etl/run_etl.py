"""
ETL Process for MySQL
Extract -> Transform -> Load
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extract.extract_data import extract_from_sources
from transform.transform_data import transform_data
from load.load_data import load_to_mysql

load_dotenv()


def main():
    print("=" * 50)
    print("MySQL ETL Process Started")
    print(f"Timestamp: {datetime.now()}")
    print("=" * 50)
    
    try:
        # EXTRACT
        print("\n[1/3] EXTRACT: Extracting data from sources...")
        raw_data = extract_from_sources()
        print(f"✓ Extracted {len(raw_data)} records")
        
        # TRANSFORM
        print("\n[2/3] TRANSFORM: Transforming data...")
        transformed_data = transform_data(raw_data)
        print(f"✓ Transformed {len(transformed_data)} records")
        
        # LOAD
        print("\n[3/3] LOAD: Loading data to MySQL...")
        result = load_to_mysql(transformed_data)
        print(f"✓ Loaded {result['inserted']} records to MySQL")
        
        print("\n" + "=" * 50)
        print("ETL Process Completed Successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ Error in ETL Process: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
