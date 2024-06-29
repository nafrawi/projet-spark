import unittest
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, when
from src.fr.hymaia.exo4.no_udf import add_category_name, calculate_total_price_per_day, calculate_total_price_per_day_last_30_days

class TestNoUdf(unittest.TestCase):
    def setUp(self):
        self.spark = spark

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True)
        ])

        self.input_data = [
            (0, "2019-02-17", 6, 40.0),
            (1, "2015-10-01", 4, 69.0),
            (2, "2019-02-17", 6, 33.0),
            (3, "2019-02-17", 4, 12.0),
            (4, "2019-02-18", 6, 20.0),
            (5, "2019-02-18", 6, 25.0)
        ]

        self.input_df = self.spark.createDataFrame(self.input_data, schema)

    def test_add_category_name(self):
        df = add_category_name(self.input_df)
        expected_data = [
            (0, "2019-02-17", 6, 40.0, "furniture"),
            (1, "2015-10-01", 4, 69.0, "food"),
            (2, "2019-02-17", 6, 33.0, "furniture"),
            (3, "2019-02-17", 4, 12.0, "food"),
            (4, "2019-02-18", 6, 20.0, "furniture"),
            (5, "2019-02-18", 6, 25.0, "furniture")
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)
        self.assertEqual(df.collect(), expected_df.collect())

    def test_calculate_total_price_per_day(self):
        df = add_category_name(self.input_df)
        df = calculate_total_price_per_day(df)

        expected_data = [
            (0, "2019-02-17", 6, 40.0, "furniture", 73.0),
            (1, "2015-10-01", 4, 69.0, "food", 69.0),
            (2, "2019-02-17", 6, 33.0, "furniture", 73.0),
            (3, "2019-02-17", 4, 12.0, "food", 12.0),
            (4, "2019-02-18", 6, 20.0, "furniture", 45.0),
            (5, "2019-02-18", 6, 25.0, "furniture", 45.0)
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True),
            StructField("total_price_per_category_per_day", FloatType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        
        df_sorted = df.orderBy("id")
        expected_df_sorted = expected_df.orderBy("id")
        self.assertEqual(df_sorted.collect(), expected_df_sorted.collect())

    def test_calculate_total_price_last_30_days(self):
        df = add_category_name(self.input_df)
        df = calculate_total_price_per_day(df)
        df = calculate_total_price_per_day_last_30_days(df)

        expected_data = [
            (0, "2019-02-17", 6, 40.0, "furniture", 73.0, 40.0),
            (1, "2015-10-01", 4, 69.0, "food", 69.0, 69.0),
            (2, "2019-02-17", 6, 33.0, "furniture", 73.0, 73.0),
            (3, "2019-02-17", 4, 12.0, "food", 12.0, 81.0),
            (4, "2019-02-18", 6, 20.0, "furniture", 45.0, 93.0),
            (5, "2019-02-18", 6, 25.0, "furniture", 45.0, 118.0)
        ]
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("category", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("category_name", StringType(), True),
            StructField("total_price_per_category_per_day", FloatType(), True),
            StructField("total_price_per_category_per_day_last_30_days", FloatType(), True)
        ])
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

       
        df_sorted = df.orderBy("id")
        expected_df_sorted = expected_df.orderBy("id")
        self.assertEqual(df_sorted.collect(), expected_df_sorted.collect())
