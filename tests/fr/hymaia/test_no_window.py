import unittest

from src.fr.hymaia.exo4.no_window_func  import (
    calculate_total_price_per_category_per_day_last_30_days)
from tests.fr.hymaia.spark_test_case import spark


class TestNoUdf(unittest.TestCase):
   
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

       
if __name__ == "__main__":
    unittest.main()
