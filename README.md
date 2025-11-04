# Brazilian E-commerce ETL Pipeline

This project implements a professional ETL (Extract, Transform, Load) pipeline for the Brazilian E-commerce dataset using the Medallion Architecture pattern. The pipeline processes e-commerce data through three layers: Bronze (raw data), Silver (cleaned data), and Gold (business metrics).

## Architecture

### Medallion Architecture
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Business-ready analytics and metrics

## Features

- Data cleaning and standardization
- Duplicate removal
- Date format normalization
- Business metrics calculation
- Customer analytics
- State-level performance analysis

## Metrics Generated

1. Daily Sales Metrics
   - Order count
   - Revenue
   - Average delivery time

2. Customer Statistics
   - Orders per customer
   - Total spend
   - Average order value
   - State distribution

## Prerequisites

- Python 3.9+
- Required packages:
  - pandas
  - numpy
  - pyarrow

## Setup

1. Clone the repository
2. Download the dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
3. Create a `source_data` directory and place these files in it:
   - `olist_orders_dataset.csv`
   - `olist_order_items_dataset.csv`
   - `olist_customers_dataset.csv`

## Installation

```bash
pip install pandas numpy pyarrow
```

## Usage

Run the ETL pipeline:
```bash
python brazillain_ecommerce_etl.py
```

## Output

The pipeline creates three directories:
- `data/bronze/`: Raw data in Parquet format
- `data/silver/`: Cleaned and validated data
- `data/gold/`: Business metrics and analytics

A pipeline report is generated at `data/pipeline_report.json`

## Data Source

The data is sourced from the [Brazilian E-commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), available on Kaggle.