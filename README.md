# OlistSalesForecast

### _ETL process using Pandas_


##### Components of this project: 
main.py - python script to run Pandas ETL process, arguments can be passed using CLI:
```sh
python main.py --order_csv path/to/olist_orders_dataset.csv
               --order_items_csv path/to/olist_order_items_dataset.csv
               --products_csv path/to/olist_products_dataset.csv
               --output_dir path/to/output_directory
```

main.py script contains 3 methods:
- **load()** - gets and reads input CSV files from argument parser and creates DF

- **preprocess_data()** - joins necessary datasets, converts columns to correct data types, adds new columns to dataframe, calculates the total price for each product for each week,returns final weekly_sales dataset which contains five columns: 
product_id	
week_start_date	
price	
sales_forecast	
forecasted_week

> I used Simple Moving Average (SMA) as forecasting method, because I am not familiar with >other complex ML forecasting methods, but at least I tried to integrate ARIMA, SARIMA, Exponential Smoothing, Naive Forecasting methods and they can be used instead of SMA.
SMA windows size I chose 4 weeks, so sales forecast for next week can be evaluated based on historical data for previous 4 weeks.

- **save_data()** - creates directory to save parquet file partitioned by product_id column (using pyarrow library)
