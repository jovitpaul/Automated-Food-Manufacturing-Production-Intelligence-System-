import sqlite3
import pandas as pd
import logging
import shutil
from pathlib import Path
from datetime import datetime

# Configure Professional Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ETL_Pipeline")

class FactoryDatabase:
    """Handles ETL processes, database schemas, and file archiving."""
    
    def __init__(self, db_name: str = "factory_operations.db"):
        self.db_name = db_name
        self.data_dir = Path("data")
        self.archive_dir = Path("archive")
        
        # Ensure our folders actually exist!
        self.data_dir.mkdir(exist_ok=True)
        self.archive_dir.mkdir(exist_ok=True)
        
    def create_schema(self):
        """Creates the database tables with strict rules."""
        logger.info("Connecting to database and verifying schema...")
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS production_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    production_date DATE NOT NULL,
                    sku TEXT NOT NULL,
                    category TEXT NOT NULL,
                    qty_produced INTEGER NOT NULL,
                    qty_defective INTEGER NOT NULL,
                    UNIQUE(production_date, sku)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS store_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    delivery_date DATE NOT NULL,
                    sku TEXT NOT NULL,
                    store_id TEXT NOT NULL,
                    qty_delivered INTEGER NOT NULL
                )
            """)
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Schema creation failed: {e}")
            raise
        finally:
            conn.close()

    def _archive_file(self, file_path: Path):
        """Moves a processed file to the archive folder with a timestamp."""
        # Create a timestamp like '20240415_143000'
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create a new filename: "production_log_20240415_143000.csv"
        new_filename = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        archive_path = self.archive_dir / new_filename
        
        # Move the file physically on the computer
        shutil.move(str(file_path), str(archive_path))
        logger.info(f"ARCHIVED: Moved to {archive_path}")

    def load_data(self):
        """Finds any matching CSVs, extracts data, loads to DB, and archives the file."""
        logger.info("Scanning 'data' Drop Zone for new files...")
        
        # 1. FUZZY MATCHING: Find ANY file with 'production' in the name (case-insensitive)
        # Using .glob("*[Pp]roduction*.csv") means anything before or after the word production is fine.
        production_files = list(self.data_dir.glob("*[Pp]roduction*.csv"))
        delivery_files = list(self.data_dir.glob("*[Dd]elivery*.csv"))

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()

        try:
            # --- PROCESS PRODUCTION FILES ---
            if not production_files:
                logger.warning("No new production files found in Drop Zone.")
            else:
                for file_path in production_files:
                    logger.info(f"Processing found file: {file_path.name}")
                    df_prod = pd.read_csv(file_path)
                    
                    # Convert to list of tuples for SQLite
                    prod_records = df_prod.to_records(index=False).tolist()
                    
                    cursor.executemany("""
                        INSERT OR IGNORE INTO production_metrics 
                        (production_date, sku, category, qty_produced, qty_defective)
                        VALUES (?, ?, ?, ?, ?)
                    """, prod_records)
                    
                    conn.commit()
                    logger.info(f"Successfully loaded {len(df_prod)} rows into database.")
                    
                    # MOVE FILE TO ARCHIVE!
                    self._archive_file(file_path)

            # --- PROCESS DELIVERY FILES ---
            if not delivery_files:
                logger.warning("No new delivery files found in Drop Zone.")
            else:
                for file_path in delivery_files:
                    logger.info(f"Processing found file: {file_path.name}")
                    df_deliv = pd.read_csv(file_path)
                    deliv_records = df_deliv.to_records(index=False).tolist()
                    
                    cursor.executemany("""
                        INSERT OR IGNORE INTO store_deliveries 
                        (delivery_date, sku, store_id, qty_delivered)
                        VALUES (?, ?, ?, ?)
                    """, deliv_records)
                    
                    conn.commit()
                    logger.info(f"Successfully loaded {len(df_deliv)} rows into database.")
                    
                    # MOVE FILE TO ARCHIVE!
                    self._archive_file(file_path)

        except Exception as e:
            logger.error(f"ETL Pipeline Failed: {e}")
            conn.rollback()
        finally:
            conn.close()

if __name__ == "__main__":
    db_manager = FactoryDatabase()
    db_manager.create_schema()
    db_manager.load_data()
