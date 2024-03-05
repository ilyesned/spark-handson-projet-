import unittest
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.fr.hymaia.exo4.no_udf import add_category_name, add_window_day

class TestMain(unittest.TestCase):
    def setUp(self):
        self.spark = spark
    
    def test_add_category_name(self):
        
        test_data = [
            (0, "2019-02-17", 6, 40.0),
            (1, "2019-02-17", 5, 33.0),
            (2, "2019-02-17", 4, 70.0),
            (3, "2019-02-17", 10, 12.0),
            (4, "2019-02-17", 3, 25.0)
        ]
        columns = ["id", "date", "category", "price"]
        test_df = self.spark.createDataFrame(test_data, columns)

        result_df = add_category_name(test_df)

        expected_results = ["furniture", "food", "food", "furniture", "food"]
        actual_results = [row["category_name"] for row in result_df.collect()]
        
        self.assertEqual(actual_results, expected_results)
    
    def test_add_window_day(self):
        
        test_data = [
            (0, "2019-02-17", 6, 40.0, "furniture"),
            (1, "2019-02-17", 6, 33.0, "furniture"),
            (2, "2019-02-17", 4, 70.0, "food"),
            (3, "2019-02-17", 4, 12.0, "food"),
            (4, "2019-02-18", 6, 20.0, "furniture"),
            (5, "2019-02-18", 6, 25.0, "furniture")
        ]
        columns = ["id", "date", "category", "price", "category_name"]
        test_df = self.spark.createDataFrame(test_data, columns)

        result_df = add_window_day(test_df)

        expected_results = [82.0, 82.0, 73.0, 73.0, 45.0, 45.0]
        actual_results = [row["total_price_per_category_per_day"] for row in result_df.collect()]

        self.assertEqual(actual_results, expected_results)
    
    def test_schema(self):

        expected_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("category", IntegerType(), True),
        StructField("price", DoubleType(), True)
        ])

        input_data = [
            (0, "2019-02-17", 6, 40.0),
            (1, "2019-02-17", 6, 33.0),
            (2, "2019-02-17", 4, 70.0),
            (3, "2019-02-17", 4, 12.0),
            (4, "2019-02-18", 6, 20.0),
            (5, "2019-02-18", 6, 25.0)
        ]

        df = self.spark.createDataFrame(input_data, schema=expected_schema)

        actual_schema = df.schema

        self.assertEqual(actual_schema, expected_schema)


