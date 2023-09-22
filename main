import os
import argparse
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

def load_data(order_csv, order_items_csv, products_csv):
    # Load the orders dataset
    orders_df = pd.read_csv(order_csv)

    # Load the order_items dataset
    order_items_df = pd.read_csv(order_items_csv)

    # Load the product dataset
    products_df = pd.read_csv(products_csv)

    return orders_df, order_items_df, products_df

def preprocess_data(orders_df, order_items_df, products_df):
    # Data Preparation and Transformation

    orders_df = orders_df[['order_id', 'order_purchase_timestamp']]
    data = pd.merge(order_items_df, orders_df, on='order_id')

    data_joined = data[['product_id', 'order_purchase_timestamp', 'order_item_id', 'price']].copy()

    #Convert datetime to date fromat
    data_joined['order_purchase_timestamp'] = pd.to_datetime(data_joined['order_purchase_timestamp']).dt.normalize() 
    #Taking data from '2016-03-01' to be able to apply SMA 
    data_joined = data_joined[data_joined['order_purchase_timestamp'] >= '2016-03-01']
    
    #Find out the week start date
    data_joined['week_start_date'] = data_joined['order_purchase_timestamp'] - pd.to_timedelta(data_joined['order_purchase_timestamp'].dt.weekday, unit='D')
    
    data_grouped = data_joined.groupby(['product_id', 'week_start_date'])['price'].sum().reset_index()
    weekly_sales = data_grouped.rename(columns={'order_item_id': 'quantity_sold'})

    # Create a function to apply the Simple Moving Average (SMA) for forecasting
    def simple_moving_average(series, window_size):
        return series.rolling(window=window_size).mean()

    # Define the window size for SMA (4 weeks for a month of data)
    window_size = 4

    # Apply SMA to forecast sales for each product
    weekly_sales['sales_forecast'] = weekly_sales.groupby('product_id')['price'].transform(lambda x: simple_moving_average(x, window_size))

    # Create a new column for the forecasted week (next week)
    weekly_sales['forecasted_week'] = weekly_sales['week_start_date'] + timedelta(weeks=1)

    return weekly_sales

def save_data(weekly_sales, output_dir):

    # Create the output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, 'products.parquet')
    
    # Create PyArrow Table from the DataFrame
    table = pa.Table.from_pandas(weekly_sales)
    
    # Write the PyArrow Table to Parquet, partitioned by 'product_id'
    pq.write_to_dataset(table, file_path, partition_cols=['product_id'])

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Sales Forecasting ETL')
    parser.add_argument('--order_csv', required=True, help='Path to the orders CSV file')
    parser.add_argument('--order_items_csv', required=True, help='Path to the order_items CSV file')
    parser.add_argument('--products_csv', required=True, help='Path to the products CSV file')
    parser.add_argument('--output_dir', required=True, help='Output directory for Parquet files')

    args = parser.parse_args()

    # Load data
    orders, order_items, products = load_data(args.order_csv, args.order_items_csv, args.products_csv)

    # Preprocess data
    weekly_sales = preprocess_data(orders, order_items, products)

    # Save data as Parquet files
    save_data(weekly_sales, args.output_dir)



