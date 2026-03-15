import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import json
import smtplib
from email.message import EmailMessage
from pathlib import Path

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SPC_Monitor")

class SPCAnalyzer:
    """Calculates Statistical Process Control limits and sends email alerts."""
    
    def __init__(self, db_name: str = "factory_operations.db"):
        self.db_name = db_name
        self.reports_dir = Path("spc_reports")
        self.reports_dir.mkdir(exist_ok=True)
        
    def fetch_historical_data(self, sku: str) -> pd.DataFrame:
        """Pulls historical defect rates for a specific SKU from the database."""
        conn = sqlite3.connect(self.db_name)
        try:
            # We removed the SQL "ORDER BY" because SQL sorts dates alphabetically (which is bad!)
            query = """
                SELECT production_date, 
                       (CAST(qty_defective AS FLOAT) / qty_produced) * 100 as defect_pct
                FROM production_metrics
                WHERE sku = ?
            """
            df = pd.read_sql_query(query, conn, params=(sku,))
            
            # 1. Tell Pandas to handle messy Excel dates and turn them into real Time objects
            df['production_date'] = pd.to_datetime(df['production_date'], format='mixed')
            
            # 2. Now that they are real Time objects, tell Python to sort them chronologically
            df = df.sort_values(by='production_date', ascending=True).reset_index(drop=True)
            
            return df
        finally:
            conn.close()

    def generate_spc_chart(self, sku: str, df: pd.DataFrame) -> tuple[bool, str]:
        """Calculates 3-Sigma limits, generates the chart, and checks for breaches."""
        if df.empty or len(df) < 10:
            logger.warning(f"Not enough historical data to calculate Sigma for {sku}.")
            return False, ""

        mean_defect = df['defect_pct'].mean()
        sigma = df['defect_pct'].std()
        ucl = mean_defect + (3 * sigma)
        
        latest_date = df['production_date'].iloc[-1]
        latest_defect = df['defect_pct'].iloc[-1]
        
        is_breached = latest_defect > ucl
        
        plt.figure(figsize=(10, 5))
        plt.plot(df['production_date'], df['defect_pct'], marker='o', label="Daily Defect %", color='blue')
        plt.axhline(mean_defect, color='green', linestyle='--', label=f"Mean ({mean_defect:.2f}%)")
        plt.axhline(ucl, color='red', linestyle='-', linewidth=2, label=f"UCL (3-Sigma: {ucl:.2f}%)")
        
        plt.title(f"Statistical Process Control (SPC) Chart - {sku}")
        plt.xlabel("Date")
        plt.ylabel("Defect Rate (%)")
        plt.xticks(rotation=45)
        
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(10)) 
        plt.legend()
        plt.tight_layout()
        
        # Format date safely for Windows files
        safe_date_string = latest_date.strftime('%Y-%m-%d')
        chart_path = self.reports_dir / f"SPC_{sku}_{safe_date_string}.png"
        
        plt.savefig(chart_path)
        plt.close() 
        
        logger.info(f"Generated SPC chart for {sku}. Breach Status: {is_breached}")
        return is_breached, str(chart_path)

    def send_email_alert(self, sku: str, chart_path: str):
        """Sends an automated email to multiple managers with the SPC chart attached."""
        try:
            with open('credentials.json', 'r') as f:
                creds = json.load(f)
                email_sender = creds['email_sender']
                email_password = creds['email_password'] 
                manager_emails = creds['manager_emails'] 
                
            msg = EmailMessage()
            msg['Subject'] = f"URGENT: SPC Control Limit Breached - {sku}"
            msg['From'] = email_sender
            msg['To'] = ", ".join(manager_emails) 
            
            email_body = f"""
            Managers,
            
            The production for {sku} has exceeded the 3-Sigma Upper Control Limit.
            Please see the attached Statistical Process Control (SPC) chart for historical context.
            
            This is an automated alert.
            """
            msg.set_content(email_body)
            
            with open(chart_path, 'rb') as img:
                img_data = img.read()
                msg.add_attachment(img_data, maintype='image', subtype='png', filename=Path(chart_path).name)
                
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(email_sender, email_password)
                smtp.send_message(msg)
                
            logger.info(f"Email alert successfully sent to: {', '.join(manager_emails)}")
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")

    def run_daily_spc_checks(self):
        """Main pipeline to run checks across all SKUs."""
        logger.info("Starting Daily SPC Analysis...")
        
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT sku FROM production_metrics")
        skus = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        for sku in skus:
            df = self.fetch_historical_data(sku)
            is_breached, chart_path = self.generate_spc_chart(sku, df)
            
            if is_breached:
                logger.warning(f"ACTION REQUIRED: {sku} breached control limits!")
                self.send_email_alert(sku, chart_path)

if __name__ == "__main__":
    monitor = SPCAnalyzer()
    monitor.run_daily_spc_checks()
