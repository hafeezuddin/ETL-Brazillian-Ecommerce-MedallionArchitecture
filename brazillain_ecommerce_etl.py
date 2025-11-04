import pandas as pd
import numpy as np
from datetime import datetime
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPipeline:
    def __init__(self, base_path="./data"):
        self.base_path = Path(base_path)
        self.bronze = self.base_path / "bronze"
        self.silver = self.base_path / "silver"
        self.gold = self.base_path / "gold"
        
        for p in [self.bronze, self.silver, self.gold]:
            p.mkdir(parents=True, exist_ok=True)
    
    def load_raw_data(self, filepath, name):
        """Load CSV into bronze layer"""
        df = pd.read_csv(filepath)
        df['loaded_at'] = datetime.now()
        
        output = self.bronze / f"{name}.parquet"
        df.to_parquet(output, index=False)
        logger.info(f"Loaded {len(df)} rows to {name}")
        return df
    
    def clean_orders(self):
        """Clean up orders data - remove dupes, fix dates"""
        df = pd.read_parquet(self.bronze / "orders.parquet")
        
        # drop dupes
        before = len(df)
        df = df.drop_duplicates(subset=['order_id'])
        logger.info(f"Removed {before - len(df)} duplicate orders")
        
        # fix date columns
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_carrier_date', 'order_delivered_customer_date']
        
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # calc delivery time
        df['days_to_deliver'] = (df['order_delivered_customer_date'] - 
                                 df['order_purchase_timestamp']).dt.days
        
        output = self.silver / "orders.parquet"
        df.to_parquet(output, index=False)
        logger.info(f"Cleaned orders saved: {len(df)} rows")
        return df
    
    def clean_items(self):
        """Clean order items"""
        df = pd.read_parquet(self.bronze / "order_items.parquet")
        
        df = df.drop_duplicates(subset=['order_id', 'order_item_id'])
        
        # filter bad data
        df = df[df['price'] > 0]
        df = df[df['freight_value'] >= 0]
        
        df['total_price'] = df['price'] + df['freight_value']
        
        output = self.silver / "order_items.parquet"
        df.to_parquet(output, index=False)
        logger.info(f"Cleaned {len(df)} order items")
        return df
    
    def clean_customers(self):
        df = pd.read_parquet(self.bronze / "customers.parquet")
        df = df.drop_duplicates(subset=['customer_id'])
        
        # standardize
        if 'customer_city' in df.columns:
            df['customer_city'] = df['customer_city'].str.upper()
        if 'customer_state' in df.columns:
            df['customer_state'] = df['customer_state'].str.upper()
        
        output = self.silver / "customers.parquet"
        df.to_parquet(output, index=False)
        return df
    
    def daily_metrics(self):
        """Calculate daily sales metrics"""
        orders = pd.read_parquet(self.silver / "orders.parquet")
        items = pd.read_parquet(self.silver / "order_items.parquet")
        
        # join em up
        df = orders.merge(items, on='order_id')
        
        df['date'] = df['order_purchase_timestamp'].dt.date
        
        daily = df.groupby('date').agg({
            'order_id': 'nunique',
            'total_price': 'sum',
            'days_to_deliver': 'mean'
        }).reset_index()
        
        daily.columns = ['date', 'orders', 'revenue', 'avg_delivery_days']
        
        output = self.gold / "daily_sales.parquet"
        daily.to_parquet(output, index=False)
        logger.info(f"Daily metrics: {len(daily)} days")
        return daily
    
    def customer_stats(self):
        """Get customer level stats"""
        orders = pd.read_parquet(self.silver / "orders.parquet")
        items = pd.read_parquet(self.silver / "order_items.parquet")
        customers = pd.read_parquet(self.silver / "customers.parquet")
        
        df = orders.merge(items, on='order_id')
        df = df.merge(customers, on='customer_id', how='left')
        
        stats = df.groupby('customer_id').agg({
            'order_id': 'nunique',
            'total_price': 'sum',
            'customer_state': 'first'
        }).reset_index()
        
        stats.columns = ['customer_id', 'num_orders', 'total_spent', 'state']
        stats['avg_order_value'] = stats['total_spent'] / stats['num_orders']
        
        output = self.gold / "customer_stats.parquet"
        stats.to_parquet(output, index=False)
        logger.info(f"Stats for {len(stats)} customers")
        return stats
    
    def run(self, source_dir):
        """Run the whole thing"""
        logger.info("Starting pipeline...")
        
        # bronze
        self.load_raw_data(f"{source_dir}/olist_orders_dataset.csv", "orders")
        self.load_raw_data(f"{source_dir}/olist_order_items_dataset.csv", "order_items")
        self.load_raw_data(f"{source_dir}/olist_customers_dataset.csv", "customers")
        
        # silver
        self.clean_orders()
        self.clean_items()
        self.clean_customers()
        
        # gold
        self.daily_metrics()
        self.customer_stats()
        
        logger.info("Done!")


if __name__ == "__main__":
    pipeline = DataPipeline()
    
    # Update this path
    SOURCE = "./source_data"
    
    if os.path.exists(SOURCE):
        pipeline.run(SOURCE)
    else:
        print(f"Put your data files in {SOURCE}")
        print("Get them from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")