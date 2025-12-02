#!/usr/bin/env python3
"""
Scheduler for BCCR Exchange Rate Updates
Runs the update-current command daily at 5:00 AM
"""
import schedule
import time
import subprocess
import sys
import logging
from datetime import datetime
from pathlib import Path
import os
from db_utils import execute_sp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Get the directory of this script
SCRIPT_DIR = Path(__file__).parent
BCCR_SCRIPT = SCRIPT_DIR / 'bccr_exchange_rate.py'
APRIORI_SCRIPT = SCRIPT_DIR / 'apriori_analysis.py'
ETL_SCRIPTS = [
    SCRIPT_DIR / 'etl_mongo.py',
    SCRIPT_DIR / 'etl_mssql_src.py',
    SCRIPT_DIR / 'etl_mysql.py',
    SCRIPT_DIR / 'etl_neo4j.py',
    SCRIPT_DIR / 'etl_supabase.py',
]

# Stored procedure maestro (ejecutado via sqlcmd/pymssql)
PROMOTE_SP = "sp_etl_run_all"


def job_exchange_rate():
    """Execute the BCCR exchange rate update"""
    logger.info("Starting scheduled exchange rate update...")
    try:
        result = subprocess.run(
            [sys.executable, str(BCCR_SCRIPT), 'update-current'],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info("Exchange rate update completed successfully")
            logger.debug(f"Output: {result.stdout}")
        else:
            logger.error(f"Exchange rate update failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Exchange rate update timed out (exceeded 5 minutes)")
    except Exception as e:
        logger.error(f"Unexpected error during update: {str(e)}")


def job_apriori():
    """Execute the Apriori association rules analysis"""
    logger.info("Starting scheduled Apriori analysis...")
    try:
        result = subprocess.run(
            [sys.executable, str(APRIORI_SCRIPT), 'run'],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout for large datasets
        )
        
        if result.returncode == 0:
            logger.info("Apriori analysis completed successfully")
            logger.debug(f"Output: {result.stdout}")
        else:
            logger.error(f"Apriori analysis failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Apriori analysis timed out (exceeded 30 minutes)")
    except Exception as e:
        logger.error(f"Unexpected error during Apriori analysis: {str(e)}")


def run_etl_scripts_once():
    """Run all ETL scripts (extract->landing->staging) before scheduler loop."""
    for script in ETL_SCRIPTS:
        if not script.exists():
            logger.warning(f"ETL script not found, skipping: {script}")
            continue
        logger.info(f"Running ETL script: {script.name}")
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode == 0:
            logger.info(f"ETL {script.name} completed")
            if result.stdout:
                logger.debug(result.stdout)
        else:
            logger.error(f"ETL {script.name} failed with code {result.returncode}")
            logger.error(result.stderr)

    # Ejecutar SP maestro de promoción (si existe)
    try:
        logger.info("Executing promotion stored procedure sp_etl_run_all...")
        execute_sp("sp_etl_run_all")
        logger.info("sp_etl_run_all executed successfully")
    except Exception as e:
        logger.error(f"Error executing sp_etl_run_all: {e}")


def main():
    """Main scheduler loop"""
    logger.info("=" * 80)
    logger.info("SCHEDULER INICIADO - DWH Automation")
    logger.info("=" * 80)
    logger.info("Schedule:")
    logger.info("  - BCCR Exchange Rate: Daily at 5:00 AM")
    logger.info("  - Apriori Analysis: Weekly on Sundays at 2:00 AM")
    logger.info("NOTE: ETLs must be run manually. Scheduler only handles BCCR & Apriori.")
    logger.info("=" * 80)

    # NO ejecutar ETLs al inicio - la base debe estar limpia
    # Poblar datos históricos de BCCR (3 años) una sola vez al inicio
    try:
        logger.info("Populating historical BCCR exchange rates (3 years)...")
        result = subprocess.run(
            [sys.executable, str(BCCR_SCRIPT), 'populate'],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=600  # 10 minutos para población histórica
        )
        if result.returncode == 0:
            logger.info("✓ Historical exchange rate population completed successfully")
            if result.stdout:
                logger.debug(f"Output: {result.stdout}")
        else:
            logger.error(f"✗ Historical population failed with code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("✗ Historical population timed out (exceeded 10 minutes)")
    except Exception as e:
        logger.error(f"✗ Unexpected error during historical population: {str(e)}")
    
    # Schedule jobs
    schedule.every().day.at("05:00").do(job_exchange_rate)
    schedule.every().sunday.at("02:00").do(job_apriori)
    
    logger.info("\n✓ Scheduler configured and running...")
    logger.info("Waiting for scheduled tasks...\n")
    
    # Keep the scheduler running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("\n✓ Scheduler stopped by user")
    except Exception as e:
        logger.error(f"\n✗ Scheduler error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
