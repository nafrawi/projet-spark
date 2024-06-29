import time

from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import pandas as pd 

import psutil 

import threading 


from src.fr.hymaia.exo4.utils.monitor import HardwareMonitor 


spark = SparkSession.builder.appName("exo4").master(
    "local[*]").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()  # type: ignore
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(  # type: ignore
        _to_seq(sc, [col], _to_java_column)
    ))


def main():
    
    read_start = time.time()
    monitor = HardwareMonitor()
    monitor.start()

    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    read_end = time.time()
    op_start = time.time()
    df = df.withColumn("category_name", addCategoryName(df["category"]))
    op_end = time.time()
    write_start = time.time()
    df.write.mode("overwrite").parquet("src/output/scala_udf.parquet")
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
    data.to_csv('csvs/scala_udf.csv')        
    print(data)    
