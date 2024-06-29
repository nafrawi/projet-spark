from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import time 
from src.fr.hymaia.exo4.utils.monitor import HardwareMonitor
import pandas as pd 


def calculate_total_price_per_day(df):
    window_spec_per_day = Window.partitionBy("category", "date")
    return df.withColumn("total_price_per_category_per_day", sum("price").over(window_spec_per_day))

def calculate_total_price_per_day_last_30_days(df):
    window_spec_last_30_days = Window.partitionBy("category").orderBy("date").rowsBetween(-30, 0)
    return df.withColumn("total_price_per_category_per_day_last_30_days", sum("price").over(window_spec_last_30_days))

def filter_by_category(df, category):
    return df.filter(col("category") == category)

def main():
    
    spark = SparkSession.builder.appName("exo4_no_udf").master("local[*]").getOrCreate()
    monitor = HardwareMonitor()
    monitor.start()
    read_start = time.time()
    sell_df = spark.read.csv("src/resources/exo4/sell.csv",header=True)
    read_end = time.time()
    op_start = time.time()
    sell_df =  sell_df.withColumn("category_name", F.when(sell_df["category"] < 6, "food").otherwise("furniture"))
    op_end = time.time()
    write_start = time.time()
    sell_df.write.mode("overwrite").parquet("src/output/no_udf_noWindow.parquet")
    write_end = time.time()
    monitor.stop()
    data = {
        'read_time': read_end - read_start,
        'op_time': op_end - op_start,
        'write_time': write_end - write_start,
        'avg_cpu_usage': monitor.get_avg_cpu(),
        'avg_memory_usage': monitor.get_avg_memory(),
        'peak_memory_usage': monitor.get_peak_memory(),
    }
    print(data)
    data = pd.DataFrame([data])
    data.to_csv('csvs/no_udf.csv')
    
    sell_df = calculate_total_price_per_day(sell_df)
    sell_df = calculate_total_price_per_day_last_30_days(sell_df)
    category_1_df = filter_by_category(sell_df, 5)
    category_1_df.write.mode('overwrite').parquet('window_func.parquet')    
