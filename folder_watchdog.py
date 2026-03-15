import time
import logging
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Import your Master Controller!
from run_daily_operations import run_factory_automation

# Professional Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - WATCHDOG - %(message)s")
logger = logging.getLogger("Watchdog")

class DropZoneHandler(FileSystemEventHandler):
    """Listens for new files dropped into the data folder."""
    
    def on_created(self, event):
        # We only care about actual files (not folders) that are CSVs
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            file_name = os.path.basename(event.src_path)
            logger.info(f"🚨 NEW FILE DETECTED: {file_name}")
            
            # SENIOR DEV TRICK: Wait 3 seconds! 
            # If the encoder is copying a large file, we don't want Python to 
            # try to read it before Windows finishes copying the data!
            logger.info("Waiting 3 seconds for file transfer to complete...")
            time.sleep(3)
            
            logger.info("Triggering Master Controller...")
            try:
                # RUN THE ENTIRE FACTORY PIPELINE!
                run_factory_automation()
                logger.info("✅ Pipeline finished. Resuming watch...")
            except Exception as e:
                logger.error(f"Pipeline crashed during automated run: {e}")

def start_watchdog(folder_to_watch="data"):
    """Starts the 24/7 security guard on the Drop Zone folder."""
    
    # Ensure the folder exists so the program doesn't crash on startup
    if not os.path.exists(folder_to_watch):
        os.makedirs(folder_to_watch)

    event_handler = DropZoneHandler()
    observer = Observer()
    observer.schedule(event_handler, path=folder_to_watch, recursive=False)
    
    observer.start()
    logger.info("=====================================================")
    logger.info(f"👁️ WATCHDOG ACTIVE: Monitoring '{folder_to_watch}' folder 24/7")
    logger.info("Drop a .csv file into the folder to trigger the pipeline.")
    logger.info("Keep this black window open. Close it to stop the watchdog.")
    logger.info("=====================================================\n")
    
    try:
        while True:
            # Keeps the script running forever in the background
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Watchdog shutting down...")
        observer.stop()
    
    observer.join()

if __name__ == "__main__":
    try:
        start_watchdog()
    except Exception as e:
        logger.error(f"❌ CRASH DETECTED ON STARTUP: {e}")
        # This forces the black window to stay open so you can read the error!
        input("\nPress Enter to close this window...")
