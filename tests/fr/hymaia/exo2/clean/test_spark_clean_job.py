import unittest
from tests.fr.hymaia.spark_test_case import spark
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from src.fr.hymaia.exo2.clean.spark_clean_job import filter_age, join_villes, add_departement

class TestMain(unittest.TestCase):
    def setUp(self):
        self.spark = spark
    
    def test_filter_age(self):
        # Création d'un DataFrame de test / GIVEN
        data = [("Alice", 20), ("Bob", 15), ("Charlie", 25)]
        columns = ["name", "age"]
        df = self.spark.createDataFrame(data, columns)

        # Appliquer la fonction filter / WHEN
        filtered_df = filter_age(df)

        # Vérifier que seules les lignes avec un âge supérieur à 18 sont conservées / THEN
        expected_data = [("Alice", 20), ("Charlie", 25)]
        expected_df = self.spark.createDataFrame(expected_data, columns)
        self.assertEqual(filtered_df.collect(), expected_df.collect())
    
    def test_join_villes(self):

        data1 = [("PARIS 01", "75001"), ("MARSEILLE 06", "13006")]
        columns1 = ["city", "zip"]
        df1 = self.spark.createDataFrame(data1, columns1)

        data2 = [("Alice", 20, "75001"), ("Bob", 30, "13006")]
        columns2 = ["name", "age", "zip"]
        df2 = self.spark.createDataFrame(data2, columns2)

        result_df = join_villes(df1, df2, "zip")

        expected_data = [("PARIS 01", "75001", "Alice", 20), ("MARSEILLE 06", "13006", "Bob", 30)]
        expected_columns = ["city", "zip", "name", "age"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        self.assertTrue(result_df.collect(), expected_df.collect())
    
    def test_add_departement(self):
    
        data = [("05020",), ("20200",), ("13000",)]
        columns = ["zip"]
        df = self.spark.createDataFrame(data, columns)

        result_df = add_departement(df)

        expected_data = [("0dtytudtggug5",), ("2B",), ("13",)]
        expected_columns = ["departement"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        self.assertTrue(result_df.collect(), expected_df.collect())

    

    def test_integration(self):

        data_villes = [("Paris", "75000"), ("Marseille", "13000"), ("Lyon", "69000")]
        columns_villes = ["city", "zip"]
        df_villes = self.spark.createDataFrame(data_villes, columns_villes)

        data_clients = [("Alice", 25, "75000"), ("Bob", 30, "13000"), ("Charlie", 35, "69000")]
        columns_clients = ["name", "age", "zip"]
        df_clients = self.spark.createDataFrame(data_clients, columns_clients)

        filtered_clients = filter_age(df_clients)
        joined_df = join_villes(filtered_clients, df_villes, "zip")
        result_df = add_departement(joined_df)

        expected_data = [("Alice", 25, "75000", "Paris", "75"), ("Bob", 30, "13000", "Marseille", "13"), ("Charlie", 35, "69000", "Lyon", "69")]
        expected_columns = ["name", "age", "zip", "city", "departement"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)

        self.assertTrue(result_df.collect(), expected_df.collect())
    

if __name__ == '__main__':
    unittest.main()