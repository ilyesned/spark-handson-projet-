import unittest
from pyspark.sql import Row, SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo4.scala_udf import addCategoryName

class TestScalaUDF(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local[2]").appName("test_spark").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def test_add_category_name_udf(self):
        # GIVEN
        data = [Row(id=1, category=5), Row(id=2, category=7)]
        df = self.spark.createDataFrame(data)

        # WHEN
        result_df = df.withColumn("category_name", addCategoryName(col("category")))

        # THEN
        expected_result = [Row(id=1, category=5, category_name="food"), Row(id=2, category=7, category_name="furniture")]
        expected_df = self.spark.createDataFrame(expected_result)
        self.assertEqual(result_df.collect(), expected_df.collect())

if __name__ == '__main__':
    unittest.main()