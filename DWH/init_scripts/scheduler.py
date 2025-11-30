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


def job():
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


def main():
    """Main scheduler loop"""
    logger.info("BCCR Exchange Rate Scheduler started")
    logger.info("Schedule: Daily at 5:00 AM")
    
    # Schedule the job to run daily at 5:00 AM
    schedule.every().day.at("05:00").do(job)
    
    # Keep the scheduler running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
