import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

# Configure Professional Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DataGenerator")

class BakeryDataGenerator:
    """
    Generates realistic, relational synthetic data for a bakery manufacturing plant.
    Follows strict business rules: Deliveries <= (Produced - Defects).
    """
    
    def __init__(self, start_date: str, weeks: int = 12, num_stores: int = 100):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.weeks = weeks
        self.num_stores = num_stores
        self.stores = [f"Store_{str(i).zfill(3)}" for i in range(1, num_stores + 1)]
        
        # Define SKUs (5 Cakes, 1 Bun, 1 Loaf) and their average daily production volume
        self.products = {
            "SKU_C01_Chocolate_Cake": {"category": "Cake", "avg_vol": 500, "defect_rate": 0.03},
            "SKU_C02_Red_Velvet_Cake": {"category": "Cake", "avg_vol": 450, "defect_rate": 0.04},
            "SKU_C03_Vanilla_Sponge": {"category": "Cake", "avg_vol": 600, "defect_rate": 0.02},
            "SKU_C04_Caramel_Cheesecake": {"category": "Cake", "avg_vol": 300, "defect_rate": 0.05},
            "SKU_C05_Lemon_Drizzle": {"category": "Cake", "avg_vol": 400, "defect_rate": 0.03},
            "SKU_B01_Classic_Bun_Pack": {"category": "Bun", "avg_vol": 3000, "defect_rate": 0.01},
            "SKU_L01_Whole_Wheat_Loaf": {"category": "Loaf", "avg_vol": 2000, "defect_rate": 0.015},
        }
        
        # Set seed for reproducibility (Standard Quant/Data Engineering practice)
        np.random.seed(42)

    def _generate_working_days(self) -> list:
        """Generates a list of dates, excluding Sundays (6 days/week production)."""
        days = []
        current_date = self.start_date
        end_date = self.start_date + timedelta(weeks=self.weeks)
        
        while current_date < end_date:
            if current_date.weekday() != 6: # 6 represents Sunday
                days.append(current_date)
            current_date += timedelta(days=1)
            
        logger.info(f"Generated {len(days)} working days (excluding Sundays).")
        return days

    def generate_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Core pipeline to generate production and delivery datasets."""
        working_days = self._generate_working_days()
        
        production_records = []
        delivery_records = []
        
        logger.info("Generating production and delivery metrics...")
        
        try:
            for date in working_days:
                for sku, specs in self.products.items():
                    # 1. Generate Production & Defects
                    # Add +/- 15% random variance to the average volume
                    variance = np.random.uniform(0.85, 1.15)
                    qty_produced = int(specs["avg_vol"] * variance)
                    
                    # Defect quantities follow a binomial distribution based on defect rate
                    qty_defective = np.random.binomial(n=qty_produced, p=specs["defect_rate"])
                    qty_good = qty_produced - qty_defective
                    
                    production_records.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "sku": sku,
                        "category": specs["category"],
                        "qty_produced": qty_produced,
                        "qty_defective": qty_defective
                    })
                    
                    # 2. Generate Store Deliveries
                    # Distribute the `qty_good` exactly across 100 stores using a multinomial distribution
                    # (Assumes equal probability of delivery to each store for simplicity)
                    store_distributions = np.random.multinomial(qty_good, [1/self.num_stores]*self.num_stores)
                    
                    for store, qty in zip(self.stores, store_distributions):
                        if qty > 0: # Only record if actual items were delivered
                            delivery_records.append({
                                "date": date.strftime("%Y-%m-%d"),
                                "sku": sku,
                                "store_id": store,
                                "qty_delivered": qty
                            })
                            
            df_production = pd.DataFrame(production_records)
            df_deliveries = pd.DataFrame(delivery_records)
            
            logger.info("Data generation successful.")
            return df_production, df_deliveries
            
        except Exception as e:
            logger.error(f"Error during data generation: {str(e)}")
            raise

    def export_to_csv(self, output_dir: str = "data"):
        """Saves generated DataFrames to local CSV files."""
        out_path = Path(output_dir)
        out_path.mkdir(exist_ok=True) # Create 'data' directory if it doesn't exist
        
        df_production, df_deliveries = self.generate_data()
        
        try:
            prod_path = out_path / "production_log.csv"
            deliv_path = out_path / "delivery_log.csv"
            
            df_production.to_csv(prod_path, index=False)
            df_deliveries.to_csv(deliv_path, index=False)
            
            logger.info(f"Saved {len(df_production)} rows to {prod_path}")
            logger.info(f"Saved {len(df_deliveries)} rows to {deliv_path}")
            
        except Exception as e:
            logger.error(f"Failed to export CSV files: {str(e)}")
            raise

if __name__ == "__main__":
    # Initialize generator for roughly 3 months (13 weeks) starting Jan 1st, 2024
    generator = BakeryDataGenerator(start_date="2024-01-01", weeks=13, num_stores=100)
    
    # Run pipeline and export
    generator.export_to_csv()
