from pyspark.sql import SparkSession
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
import pandas as pd
import time
import psutil
import threading



from src.fr.hymaia.exo4.monitor import HardwareMonitor 
spark = SparkSession.builder.appName('exo4').master("local[*]").getOrCreate()

@pandas_udf("string", PandasUDFType.SCALAR)
def create_category_name(category: pd.Series) -> pd.Series:
    return category.apply(lambda x: "food" if int(x) < 6 else "furniture")

def main():
    monitor = HardwareMonitor()
    monitor.start()
    
    read_start = time.time()
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    read_end = time.time()
    
    op_start = time.time()
    df = df.withColumn("category_name", create_category_name(df["category"]))
    op_end = time.time()
    
    write_start = time.time()
    df.write.mode("overwrite").parquet("src/output/python_udf.parquet")
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
    data = pd.DataFrame([data])
    data.to_csv('pandas_udf.csv')        
    
    print(data)
