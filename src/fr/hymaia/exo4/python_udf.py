from pyspark.sql import SparkSession 
from pyspark.sql.functions import udf

import pandas as pd

import time

import psutil 

import threading


spark = SparkSession.builder.appName('exo4').master("local[*]").getOrCreate()

class HardwareMonitor:
    def __init__(self, interval=1):
        self.interval = interval
        self.cpu_percentages = []
        self.memory_usages = []
        self._stop_event = threading.Event()

    def start(self):
        self._monitor_thread = threading.Thread(target=self._monitor)
        self._monitor_thread.start()

    def stop(self):
        self._stop_event.set()
        self._monitor_thread.join()

    def _monitor(self):
        while not self._stop_event.is_set():
            self.cpu_percentages.append(psutil.cpu_percent(interval=self.interval))
            self.memory_usages.append(psutil.virtual_memory().used / (1024 ** 3))  # GB
            time.sleep(self.interval)

    def get_avg_cpu(self):
        return sum(self.cpu_percentages) / len(self.cpu_percentages) if self.cpu_percentages else 0

    def get_avg_memory(self):
        return sum(self.memory_usages) / len(self.memory_usages) if self.memory_usages else 0

    def get_peak_memory(self):
        return max(self.memory_usages) if self.memory_usages else 0


@udf 
def create_category_name(category: int) -> str:
    return "food" if int(category) < 6 else "furniture"

    
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
    print(data)
