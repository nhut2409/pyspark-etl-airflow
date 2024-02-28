import pyspark
from pyspark.sql.functions import *
from spark import spark


def transform_data():

       order_items_file = "olist_order_items_dataset.parquet"
       orders_file = "olist_orders_dataset.parquet"
       products_file = "olist_products_dataset.parquet"
       products_trans_file = "product_category_name_translation.parquet"
       bucket_url = "s3a://olistsalesbucket/"



       # Read file to dataframe
       df_items = spark.read.parquet(bucket_url + order_items_file)
       df_orders = spark.read.parquet(bucket_url + orders_file)
       df_products = spark.read.parquet(bucket_url + products_file)
       df_products_trans = spark.read.parquet(bucket_url + products_trans_file)


       # Rename column product_category_name to product_name in df_products_trans
       df_products_trans= df_products_trans.withColumnRenamed('product_category_name','product_name')

       # Translate name of product on df_products
       df_products_translated = df_products.join(df_products_trans, df_products.product_category_name == df_products_trans.product_name).drop('product_category_name','product_name')
       df_products_translated = df_products_translated.withColumnRenamed('product_category_name_english','product_category_name').select(df_products.columns)



       # Create SQL Table Views from dfs for SQL querying
       df_items.createOrReplaceTempView('items')
       df_orders.createOrReplaceTempView('orders')
       df_products.createOrReplaceTempView('products')
       # df_products_trans.createOrReplaceTempView('products_trans')
       df_products_translated.createOrReplaceTempView('products_translated')



       late_carrier_deliveries = spark.sql("""
       SELECT i.order_id, i.seller_id, DATE(i.shipping_limit_date), FLOAT(i.price), FLOAT(i.freight_value),
              p.product_id, p.product_category_name,
              o.customer_id, o.order_status, DATE(o.order_purchase_timestamp), DATE(o.order_delivered_carrier_date),
              DATE(o.order_delivered_customer_date), DATE(o.order_estimated_delivery_date)
       FROM items AS i
       JOIN orders AS o
       ON i.order_id = o.order_id
       JOIN products_translated AS p
       ON i.product_id = p.product_id
       WHERE i.shipping_limit_date < o.order_delivered_carrier_date
       """)

       
       
       # Load transformed data to s3 bucket
       late_carrier_deliveries.write.mode("overwrite").parquet("s3a://olistsalesbucket/late_carrier_deliveries.parquet")

       spark.stop()
if __name__ =="__main__":
       transform_data()