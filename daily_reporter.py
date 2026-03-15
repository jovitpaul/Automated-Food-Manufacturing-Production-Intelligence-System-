import sqlite3
import pandas as pd
import json
import smtplib
from email.message import EmailMessage
import logging
from datetime import datetime
import matplotlib.pyplot as plt
from pathlib import Path

# Professional Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Daily_Reporter")

class DailyReporter:
    """Generates and emails the daily production summary with embedded charts."""
    
    def __init__(self, db_name: str = "factory_operations.db"):
        self.db_name = db_name
        self.reports_dir = Path("spc_reports") 
        self.reports_dir.mkdir(exist_ok=True)

    def get_latest_data(self):
        """Pulls data and uses Pandas to find the TRUE chronological latest date."""
        conn = sqlite3.connect(self.db_name)
        try:
            # 1. Pull all the data
            query = "SELECT production_date, sku, qty_produced, qty_defective FROM production_metrics"
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                return None, pd.DataFrame()

            # 2. Let Pandas fix the messy Excel dates into real Time objects!
            df['real_date'] = pd.to_datetime(df['production_date'], format='mixed')
            
            # 3. Find the TRUE chronological latest date
            true_latest = df['real_date'].max()
            
            # 4. Filter the dataframe to only keep the rows for that exact day
            latest_df = df[df['real_date'] == true_latest].copy()
            
            # 5. Format the date nicely for the email title
            formatted_date = true_latest.strftime('%Y-%m-%d')
            
            return formatted_date, latest_df
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return None, pd.DataFrame()
        finally:
            conn.close()

    def generate_charts(self, df: pd.DataFrame, date: str):
        """Generates two horizontal bar charts and saves them as PNG files."""
        safe_date = str(date).replace('/', '-').replace('\\', '-')
        
        df['defect_rate'] = (df['qty_defective'] / df['qty_produced']) * 100
        df['clean_sku'] = df['sku'].str.replace('SKU_', '')
        
        # Volume Chart
        df_vol = df.sort_values('qty_produced', ascending=True)
        plt.figure(figsize=(6, 4))
        plt.barh(df_vol['clean_sku'], df_vol['qty_produced'], color='#1f77b4')
        plt.title('Daily Volume Produced')
        plt.xlabel('Total Units')
        plt.tight_layout()
        vol_path = self.reports_dir / f"email_vol_{safe_date}.png"
        plt.savefig(vol_path)
        plt.close()

        # Defect Rate Chart
        df_def = df.sort_values('defect_rate', ascending=True)
        plt.figure(figsize=(6, 4))
        plt.barh(df_def['clean_sku'], df_def['defect_rate'], color='#d62728')
        plt.title('Daily Defect Rate (%)')
        plt.xlabel('Defect Rate (%)')
        plt.tight_layout()
        def_path = self.reports_dir / f"email_def_{safe_date}.png"
        plt.savefig(def_path)
        plt.close()

        return str(vol_path), str(def_path)

    def format_html_email(self, date: str, df: pd.DataFrame) -> str:
        """Converts the daily data into a beautiful HTML email."""
        
        total_produced = df['qty_produced'].sum()
        total_defects = df['qty_defective'].sum()
        total_good = total_produced - total_defects
        yield_pct = (total_good / total_produced) * 100 if total_produced > 0 else 0
        
        df['defect_rate'] = (df['qty_defective'] / df['qty_produced']) * 100
        df = df.sort_values('defect_rate', ascending=False)
        
        table_rows = ""
        for _, row in df.iterrows():
            table_rows += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;">{row['sku'].replace('SKU_', '')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">{row['qty_produced']:,}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center; color: #d62728;"><b>{row['defect_rate']:.1f}%</b></td>
            </tr>
            """

        html_content = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #2C3E50;">🏭 Daily Commissary Report: {date}</h2>
                
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                    <h3 style="margin-top: 0; color: #1f77b4;">Executive Summary</h3>
                    <p><b>Total Yield (Good Units):</b> {total_good:,}</p>
                    <p><b>Percentage Yield:</b> {yield_pct:.1f}%</p>
                </div>

                <h3 style="color: #2C3E50;">SKU Performance Deep Dive</h3>
                <table style="width: 100%; max-width: 800px;">
                    <tr>
                        <td style="width: 50%;"><img src="cid:vol_chart" style="width: 100%;"></td>
                        <td style="width: 50%;"><img src="cid:def_chart" style="width: 100%;"></td>
                    </tr>
                </table>

                <h3 style="color: #2C3E50;">SKU Raw Data (Ranked by Reject %)</h3>
                <table style="border-collapse: collapse; width: 100%; max-width: 600px; margin-bottom: 30px;">
                    <thead>
                        <tr style="background-color: #1f77b4; color: white;">
                            <th style="padding: 10px; text-align: left;">SKU Name</th>
                            <th style="padding: 10px; text-align: center;">Total Produced</th>
                            <th style="padding: 10px; text-align: center;">Reject Rate</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
                
                <hr style="border: 1px solid #eee; margin-bottom: 20px;">
                <p style="font-size: 14px; text-align: center;">Want to view historical trends and cumulative data?</p>
                <div style="text-align: center; margin-bottom: 30px;">
                    <a href="http://localhost:8501" style="display: inline-block; padding: 12px 24px; background-color: #2ca02c; color: white; text-decoration: none; border-radius: 5px; font-weight: bold; font-size: 16px;">📈 View Live Dashboard</a>
                </div>
                
                <p style="font-size: 12px; color: #777; text-align: center;">This is an automated report generated by the Commissary Intelligence System.</p>
            </body>
        </html>
        """
        return html_content

    def send_daily_report(self):
        """Main function to grab data, generate charts, format HTML, and send email."""
        logger.info("Preparing Daily Email Report...")
        
        latest_date, df = self.get_latest_data()
        
        if df is None or df.empty:
            logger.warning("No data available to report.")
            return

        vol_chart_path, def_chart_path = self.generate_charts(df, latest_date)
        html_body = self.format_html_email(latest_date, df)

        try:
            with open('credentials.json', 'r') as f:
                creds = json.load(f)
                email_sender = creds['email_sender']
                email_password = creds['email_password'] 
                manager_emails = creds['manager_emails'] 
                
            msg = EmailMessage()
            msg['Subject'] = f"📊 Daily Commissary Report - {latest_date}"
            msg['From'] = email_sender
            msg['To'] = ", ".join(manager_emails) 
            
            msg.set_content("Please enable HTML to view this email.")
            msg.add_alternative(html_body, subtype='html')
            
            with open(vol_chart_path, 'rb') as img:
                msg.get_payload()[1].add_related(img.read(), 'image', 'png', cid='<vol_chart>')
                
            with open(def_chart_path, 'rb') as img:
                msg.get_payload()[1].add_related(img.read(), 'image', 'png', cid='<def_chart>')
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
                smtp.login(email_sender, email_password)
                smtp.send_message(msg)
                
            logger.info(f"✅ Daily Report for {latest_date} successfully emailed to managers!")
            
        except Exception as e:
            logger.error(f"Failed to send daily report email: {e}")

if __name__ == "__main__":
    reporter = DailyReporter()
    reporter.send_daily_report()
