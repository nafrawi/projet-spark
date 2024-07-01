
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("Aggregate Client Data").getOrCreate()


def read_data(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def aggregate_data(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("departement")
        .agg(count("name").alias("nb_people"))
        .orderBy(col("nb_people").desc(), col("departement"))
    )


def write_aggregate(df: DataFrame, path: str):
    df.write.option("header", "true").mode("overwrite").csv(path)


def main():
    spark = create_spark_session()
    client_data_df = read_data(spark, "data/exo2/clean")
    aggregated_df = aggregate_data(client_data_df)
    write_aggregate(aggregated_df, "data/exo2/aggregate")


if __name__ == "__main__":
    main()
