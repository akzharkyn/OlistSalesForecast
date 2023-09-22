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

#### Answers to questions

- **Which features would you extract and how from the tables?**
For sales forecasting I used only two tables: olist_orders_dataset.csv (Orders) and olist_order_items_dataset.csv (Order_items), because they have all the necessary columns.
From Orders extracted features such as order_id, order_purchase_timestamp,
from Order_items used features like product_id,  price, order_id. 
From joined dataset of these two tables, I find out the start date of each week (using order_purchase_timestamp column) and grouping by each product, I calculated the total price amount for each product for each week.
How extracted?
Using Pandas read_csv() method created dataframe and selected particular columns.

- **How would you use the remaining tables?**
We can use Customers table to perform customer division based on location, like in which states most of the customers, also identify the loyal customers. So we can develop strategies to retain customers, such as loyalty programs or targeted offers.
Can use Products table to identify top-selling products, perform product categorization, from which product categories people buy most of the time, using columns like length, height, width do some visualizations to assess the measures of products. Also we can identify slow-moving or not sold product  items.
Using Orders table we can analyze order processing times, and delivery performance then optimize order fulfillment processes for better customer satisfaction.
Using Reviews table we can extract insights from customer feedback to improve products or services, identify common issues or positive stuff mentioned in reviews.
Using Geolocation table we can visualize customer distribution on map to identify regions with high demand, also using longitude and latitude can plot maps and find distances between sellers and customers and optimize delivery routes
Using Payments table we can analyze and identify most preferable payment types and monitor for suspicious activities or transactions.

- **How would you turn it into an application in production?**
I would implement these steps below:
- Data ingestion: Creating ingestion pipeline to ingest source CSV files or database data into our raw folder in a datalake
- Data pipeline: Implementing a data pipeline that automates data extraction, transformation, and loading from the source data (database or data lake) to the application.
- Storage: Storing the refined and processed data in a datalake that is optimized for future read operations, processed data also can be stored in a database for creating data marts or other analytical/reporting purposes. 
- Orchestration: Using ADF or other job services to run pipelines in scheduled manner
- Deployment: Using some CI/CD tool for application and its dependencies to easy deployment and scaling.
- Web Service: Creating RESTful API or web service or other visualization dashboards (e.g PowerBI) that provides endpoints/graphs for users or other services to request sales forecasts.
- Monitoring/Logging: Implementing monitoring and logging to track the performance of the application 
- Updates: Regularly maintaining and updating the application, for example using newly available forecasting models or adding other features to predict the future outcomes.
- **How would you design an application if you knew that you would have to build a similar solution for a couple other countries, and the data schema might be different for them, however, you can get the same underlying data?**
1. Maybe using configuration files or settings that specify which country's data schema to use for a given instance of the application. Each configuration should define the schema mapping for the selected country. For example, creating separate configuration files like config_country_A.yml and config_country_B.yml.
These files define the schema mapping for their respective countries. For example, if the keys are our common internal schema, values will be country_A’s schema in config_country_A.yml:
  Order_id: Order_number
  Product_id: product_code
  Price: Pricing
2. Designing the application with ‘Extract’ activity for each country, each with its data extraction logic.  for example, in this ETL process we need several columns to be passed to transformation step, so we can ingest different countries’ data in different activities (e.g in ADF) and as final result we get only necessary columns.
3. Maintaining a metadata that stores information about the data sources, including information about each country's data schema, such as column names, data types, and any specific considerations (e.g., date formats, currency symbols). So instead of hardcoding schema mappings within application, we can dynamically generate mappings based on the metadata.
