import unittest

from src.fr.hymaia.exo4.no_udf import (
    calculate_total_price_per_category_per_day,
    calculate_total_price_per_category_per_day_last_30_days)
from tests.fr.hymaia.spark_test_case import spark


class TestNoUdf(unittest.TestCase):
    def test_add_category_name_without_udf(self):
        df = spark.createDataFrame(
            [
                (0, "2019-02-16", 6, 40.0),
                (0, "2019-02-16", 6, 33.0),
                (0, "2019-02-16", 6, 70.0),
                (0, "2019-02-13", 4, 12.0),
                (0, "2019-02-13", 4, 20.0),
                (0, "2019-02-13", 4, 25.0),
            ],
            ["id", "date", "category", "price"],
        )

        df_expected = spark.createDataFrame(
            [
                (0, "2019-02-16", 6, 40.0, "furniture"),
                (0, "2019-02-16", 6, 33.0, "furniture"),
                (0, "2019-02-16", 6, 70.0, "furniture"),
                (0, "2019-02-13", 4, 12.0, "food"),
                (0, "2019-02-13", 4, 20.0, "food"),
                (0, "2019-02-13", 4, 25.0, "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )

        expected_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
            )
            for row in df_expected.collect()
        ]

    def test_calculate_total_price_per_category_per_day_last_30_days(self):
        df = spark.createDataFrame(
            [
                (0, "2019-02-16", 6, 40.0, "furniture"),
                (0, "2019-02-17", 6, 33.0, "furniture"),
                (0, "2019-02-18", 6, 70.0, "furniture"),
                (0, "2019-02-16", 4, 12.0, "food"),
                (0, "2019-02-17", 4, 20.0, "food"),
                (0, "2019-02-18", 4, 25.0, "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )

        actual = calculate_total_price_per_category_per_day_last_30_days(df)
        actual_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day_last_30_days"],
            )
            for row in actual.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                (0, "2019-02-16", 4, 12.0, "food", 12.0),
                (0, "2019-02-17", 4, 20.0, "food", 32.0),
                (0, "2019-02-18", 4, 25.0, "food", 57.0),
                (0, "2019-02-16", 6, 40.0, "furniture", 40.0),
                (0, "2019-02-17", 6, 33.0, "furniture", 73.0),
                (0, "2019-02-18", 6, 70.0, "furniture", 143.0),
            ],
            [
                "id",
                "date",
                "category",
                "price",
                "category_name",
                "total_price_per_category_per_day_last_30_days",
            ],
        )

        expected_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day_last_30_days"],
            )
            for row in df_expected.collect()
        ]

        self.assertEqual(actual.printSchema(), df_expected.printSchema())
        self.assertEqual(actual_list, expected_list)

    ###################### TESTS FAILING ######################

    def test_calculate_total_price_per_category_per_day_fail(self):
        df = spark.createDataFrame(
            [
                (0, "2019-02-16", 6, 40.0, "furniture"),
                (0, "2019-02-16", 6, 33.0, "furniture"),
                (0, "2019-02-16", 6, 70.0, "furniture"),
                (0, "2019-02-13", 4, 12.0, "food"),
                (0, "2019-02-13", 4, 20.0, "food"),
                (0, "2019-02-13", 4, 25.0, "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )

        actual = calculate_total_price_per_category_per_day(df)
        actual_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day"],
            )
            for row in actual.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                (0, "2019-02-13", 4, 12.0, "food", 57.0),
                (0, "2019-02-16", 6, 40.0, "furniture", 12.0),
            ],
            [
                "id",
                "date",
                "category",
                "price",
                "category_name",
                "total_price_per_category_per_day",
            ],
        )

        expected_list = [
            (
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day"],
                row["id"],
            )
            for row in df_expected.collect()
        ]

        self.assertNotEqual(actual.printSchema, df_expected.printSchema)
        self.assertNotEqual(actual_list, expected_list)

    def test_calculate_total_price_per_category_per_day_last_30_days_fail(self):
        df = spark.createDataFrame(
            [
                (0, "2019-02-16", 6, 40.0, "furniture"),
                (0, "2019-02-17", 6, 33.0, "furniture"),
                (0, "2019-02-18", 6, 70.0, "furniture"),
                (0, "2019-02-16", 4, 12.0, "food"),
                (0, "2019-02-17", 4, 20.0, "food"),
                (0, "2019-02-18", 4, 25.0, "food"),
            ],
            ["id", "date", "category", "price", "category_name"],
        )

        actual = calculate_total_price_per_category_per_day_last_30_days(df)
        actual_list = [
            (
                row["id"],
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day_last_30_days"],
            )
            for row in actual.collect()
        ]

        df_expected = spark.createDataFrame(
            [
                (0, "2019-02-16", 4, 12.0, "food", 12.0),
                (0, "2019-02-17", 4, 20.0, "food", 32.0),
                (0, "2019-02-18", 4, 25.0, "food", 57.0),
                (0, "2019-02-16", 6, 40.0, "furniture", 40.0),
                (0, "2019-02-17", 6, 33.0, "furniture", 8.0),
                (0, "2019-02-18", 6, 70.0, "furniture", 143.0),
            ],
            [
                "id",
                "date",
                "category",
                "price",
                "category_name",
                "total_price_per_category_per_day_last_30_days",
            ],
        )

        expected_list = [
            (
                row["date"],
                row["category"],
                row["price"],
                row["category_name"],
                row["total_price_per_category_per_day_last_30_days"],
                row["id"],
            )
            for row in df_expected.collect()
        ]

        self.assertNotEqual(actual.printSchema, df_expected.printSchema)
        self.assertNotEqual(actual_list, expected_list)


if __name__ == "__main__":
    unittest.main()
