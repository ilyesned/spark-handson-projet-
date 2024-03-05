import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, asc, desc, when, substring, count

# Premier job Spark
def main():
    spark = SparkSession.builder \
        .appName("UDF") \
        .master("local[*]") \
        .getOrCreate()
    
    # Création d'un reader de csv
    reader = spark.read.option("header", "true") \
                    .option("delimiter", ",")
    
    # Chargement des données
    df_sell = reader.csv("src/resources/exo4/sell.csv").persist()

    # Ajout de la colonne category_name en utilisant l'UDF
    df_sell_category_name = df_sell.withColumn("category_name", categoriser_udf(df_sell["category"]))

    # Ecriture du résultat au format CSV
    df_sell_category_name.write.mode("overwrite").csv("data/exo4/python_udf", header=True)

# Définition de la fonction UDF Python
def categoriser(category):
    if int(category) < 6:
        return "food"
    else:
        return "furniture"

# Convertir la fonction Python en UDF
categoriser_udf = udf(categoriser, StringType())