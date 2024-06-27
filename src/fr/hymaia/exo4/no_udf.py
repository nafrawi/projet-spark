import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
import psutil
import threading

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

def main():
    spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()
    
    monitor = HardwareMonitor()
    monitor.start()

    read_start = time.time()
    df1 = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    read_end = time.time()

    op_start = time.time()
    df1 = df1.withColumn("category_name", 
        f.when(f.col("category") < 6, "food")
        .otherwise("furniture")
    )
    op_end = time.time()
    
    df = df1.withColumn("date", f.to_date("date"))
    df = calculate_total_price_per_category_per_day(df)
    df = calculate_total_price_per_category_per_day_last_30_days(df)

    write_start = time.time()
    df.write.mode("overwrite").parquet("src/output/no_udf.parquet")
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

def calculate_total_price_per_category_per_day(df):
    window_spec = Window.partitionBy("category", "date")
    df = df.withColumn("total_price_per_category_per_day",
                       f.sum("price").over(window_spec))
    return df

def calculate_total_price_per_category_per_day_last_30_days(df):
    df = df.dropDuplicates(['date', "category_name"])
    window_spec = Window.partitionBy(
        "category_name").orderBy("date").rowsBetween(-29, 0)
    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum(
        "price").over(window_spec))
    return df.select("id", "date", "category", "price", "category_name", "total_price_per_category_per_day_last_30_days")

if __name__ == "__main__":
    main()
