import os
import pytest

from main import load_data, preprocess_data, save_data


order_csv = 'data/olist_orders_dataset.csv'
order_items_csv = 'data/olist_order_items_dataset.csv'
products_csv = 'data/olist_products_dataset.csv'

output_dir = 'test_output'

@pytest.fixture
def setup_output_dir():
    # Create the output directory for testing
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def test_etl_process(setup_output_dir):
    # Load data
    orders, order_items, products = load_data(order_csv, order_items_csv, products_csv)

    # Preprocess data
    weekly_sales = preprocess_data(orders, order_items, products)

    # Save data
    save_data(weekly_sales, output_dir)

    # Check if the output directory was created
    assert os.path.exists(output_dir)

    # Check if the file to store parquet was created
    file_path = os.path.join(output_dir, 'products.parquet')
    assert os.path.exists(file_path)


def test_data_preprocessing():
    orders, order_items, products = load_data(order_csv, order_items_csv, products_csv)

    weekly_sales = preprocess_data(orders, order_items, products)

    # Check attributes in the final datset data
    assert 'sales_forecast' in weekly_sales.columns
    assert 'forecasted_week' in weekly_sales.columns
    

