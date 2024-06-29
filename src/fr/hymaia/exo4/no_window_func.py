from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
import time
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

spark = SparkSession.builder \
    .appName("exo4") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "200")


def calculate_total_price_per_category_per_day_last_30_days(df: DataFrame) -> DataFrame:
    df = df.withColumn("date", col("date").cast(DateType()))
    df = df.withColumn("start_date", F.expr("date_sub(date, 30)"))
    df.persist()
    filtered_df = df.select("id", "date", "category", "price", "start_date", "category_name")
    joined_df = filtered_df.alias("df1").join(
        filtered_df.alias("df2"),
        (F.col("df2.date") <= F.col("df1.date")) &
        (F.col("df2.date") > F.col("df1.start_date")) &
        (F.col("df2.category") == F.col("df1.category")),
        "inner"
    )

    result_df = joined_df.groupBy(
        "df1.id", "df1.date", "df1.category", "df1.price", "df1.category_name"
    ).agg(
        F.sum("df2.price").alias("total_price_per_category_per_day_last_30_days")
    )

    return result_df.select(
        col("id"),
        col("date").cast("string"), 
        col("category"),
        col("price"),
        col("category_name"),
        col("total_price_per_category_per_day_last_30_days")
    )

def main():
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    df = df.withColumn("category_name", (
        F.when(F.col("category") < 6, "food")
        .otherwise("furniture")))
    df = df.withColumn("date", F.to_date("date"))
    start_time = time.time()
    df = calculate_total_price_per_category_per_day_last_30_days(df)
    df.write.mode("overwrite").parquet("no_window_func.parquet")
    end_time = time.time()
    print(f'no window func process took {end_time - start_time}')

if __name__ == "__main__":
    main()
