import logging
import time

# IMPORT ALL THREE MODULES!
from etl_pipeline import FactoryDatabase
from spc_monitor import SPCAnalyzer
from daily_reporter import DailyReporter  # <-- NEW!

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("Master")

def run_factory_automation():
    logger.info("=========================================")
    logger.info("🏭 STARTING DAILY FACTORY AUTOMATION 🏭")
    logger.info("=========================================\n")
    
    try:
        # --- PHASE 1: DATA INGESTION ---
        logger.info(">>> STEP 1: Waking up ETL Pipeline...")
        time.sleep(1) 
        db_system = FactoryDatabase()
        db_system.create_schema() 
        db_system.load_data()     
        logger.info(">>> STEP 1 COMPLETE: Data successfully secured in database.\n")
        
        # --- PHASE 2: SPC ALERTS (Only emails if things are BROKEN) ---
        logger.info(">>> STEP 2: Waking up Quality Assurance Monitor...")
        time.sleep(1)
        qa_system = SPCAnalyzer()
        qa_system.run_daily_spc_checks() 
        logger.info(">>> STEP 2 COMPLETE: Quality checks finished.\n")
        
        # --- PHASE 3: DAILY REPORT (Emails EVERY DAY to summarize) ---
        logger.info(">>> STEP 3: Waking up Daily Reporter...")
        time.sleep(1)
        reporter = DailyReporter()
        reporter.send_daily_report()
        logger.info(">>> STEP 3 COMPLETE: Daily summary delivered to management.\n")
        
        logger.info("=========================================")
        logger.info("✅ ALL DAILY OPERATIONS COMPLETED ✅")
        logger.info("=========================================")
        
    except Exception as e:
        logger.error(f"❌ CRITICAL PIPELINE FAILURE: {e}")

if __name__ == "__main__":
    run_factory_automation()
