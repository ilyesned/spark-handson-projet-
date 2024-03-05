from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.fr.hymaia.exo4.python_udf import categoriser_udf

# Création des tests unitaires concernant le job "python_udf"
class TestPythonUDF(unittest.TestCase):
    
    # Test unitaire sur le job "python_udf"
    def test_should_return_catergory_name(self):
        
        # GIVEN

        # Traitement des données pour avoir la même chose qu'en sortie de clean
        df_sell_test = spark.createDataFrame(
            [
                Row(id='2500000', date='2020-02-18', category='12', price=62.0),
                Row(id='2600010', date='2018-10-02', category='3', price=0.2),
                Row(id='2700100', date='2017-04-02', category='6', price=5.0),
            ]
        )

        # WHEN
        
        # Dataframe obtenu en donnant un nom à la catégorie des produits
        actual_sell_category_name_test = df_sell_test.withColumn("category_name", categoriser_udf(df_sell_test["category"]))

        # THEN

        # Dataframe attendu en donnant un nom à la catégorie des produits
        excepted_sell_category_name_test = spark.createDataFrame(
            [
                Row(id='2500000', date='2020-02-18', category='12', price=62.0, category_name='furniture'),
                Row(id='2600010', date='2018-10-02', category='3', price=0.2, category_name='food'),
                Row(id='2700100', date='2017-04-02', category='6', price=5.0, category_name='furniture'),
            ]
        )

        expected_schema = StructType(
            [
                StructField('id', StringType(), True),
                StructField('date', StringType(), True),
                StructField('category', StringType(), True),
                StructField('price', DoubleType(), True),
                StructField('category_name', StringType(), True)
            ]
        )

        actual_schema = actual_sell_category_name_test.schema

        # Vérification que le DataFrame qu'on obtient a le même schéma que celui attendu
        self.assertEqual(actual_schema, expected_schema)

        # Vérification que le DataFrame qu'on obtient est le même que celui attendu
        self.assertEqual(actual_sell_category_name_test.collect(), excepted_sell_category_name_test.collect())
