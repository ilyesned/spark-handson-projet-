import unittest
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.agregate.spark_agregate_job import calculate_population_by_department, write_csv

class TestMain(unittest.TestCase):
    def setUp(self):
        self.spark = spark
    
    def test_calculate_population_by_department(self):

        data = [("Paris", 75), ("Marseille", 13), ("Lyon", 69), ("Marseille", 13)]
        columns = ["city", "departement"]
        df = self.spark.createDataFrame(data, columns)

        result_df = calculate_population_by_department(df)

        expected_data = [("13", 2), ("69", 1), ("75", 1)]
        expected_columns = ["departement", "nb_people"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        self.assertTrue(result_df.collect(), expected_df.collect())
    