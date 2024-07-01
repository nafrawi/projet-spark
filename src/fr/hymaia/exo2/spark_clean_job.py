
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("Client Data Processing").getOrCreate()


def load_data(spark: SparkSession, path: str, cast: bool = False) -> DataFrame:
    df = spark.read.option("header", "true").csv(path)
    if cast:
        df = df.withColumn("age", col("age").cast(IntegerType()))
    return df


def drop_duplicates(df: DataFrame, subset: list[str]) -> DataFrame:
    return df.dropDuplicates(subset)


def filter_adult_clients(clients_df: DataFrame) -> DataFrame:
    if clients_df.schema["age"].dataType != IntegerType():
        raise TypeError("La colonne 'age' doit être un entier de type Int.")
    return clients_df.filter(clients_df.age >= 18)


def join_clients_cities(clients_df: DataFrame, cities_df: DataFrame) -> DataFrame:
    return clients_df.join(cities_df, on="zip", how="left")


def compute_department(zip_code: str) -> str:
    if not zip_code.isdigit():
        raise ValueError("Le zip code ne doit contenir que des Int")

    if zip_code.startswith("20"):
        int_zip = int(zip_code)
        if 20000 <= int_zip <= 20190:
            return "2A"
        elif 20191 <= int_zip <= 20999:
            return "2B"
    return zip_code[:2]


def add_department_column(df: DataFrame) -> DataFrame:
    """
    cette fonction est essentielle pour ne pas surcharger `compute_department` avec
    des features supplémentaires. pour le TDD, il faut que chaque fonction fait une
    seule fonctionnalité
    """

    compute_department_udf = udf(compute_department, StringType())
    return df.withColumn("departement", compute_department_udf(col("zip")))


def write_output(df: DataFrame, path: str):
    df.write.mode("overwrite").parquet(path)


def main():
    spark = create_spark_session()
    clients_df = load_data(spark, "src/resources/exo2/clients_bdd.csv", True)
    cities_df = load_data(spark, "src/resources/exo2/city_zipcode.csv")
    cities_df = drop_duplicates(cities_df, ["zip"])
    """
    IMPORTANT!!! : La dataset cities contient des duplicates!
    cities_df.groupBy("zip").count().filter("count > 1").show()
    
        +-----+-----+
        |  zip|count|
        +-----+-----+
        |20219|    4|
        |06660|    3|
        |07200|   20|
        |52800|   16|
        |53380|    3|
        |35640|    4|
        |09120|   17|
        |35350|    3|
        |39350|   10|
        |59169|    5|
        |16250|   17|
        |16320|   13|
        |42370|    8|
        |13610|    2|
        |45300|   34|
        |18130|   12|
        |47140|    9|
        |49290|    4|
        |69460|    8|
        |70170|   11|
        +-----+-----+
    """
    adults_df = filter_adult_clients(clients_df)
    joined_df = join_clients_cities(adults_df, cities_df)
    final_df = add_department_column(joined_df)
    write_output(final_df, "data/exo2/clean")


if __name__ == "__main__":
    main()
