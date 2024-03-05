from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.column import Column, _to_java_column, _to_seq

def main():

    # Création de la session Spark
    spark = SparkSession.builder.appName("ScalaUDFInPython").config('spark.jars', 'src/resources/exo4/udf.jar').getOrCreate()

    def addCategoryName(col):
        # on récupère le SparkContext
        sc = spark.sparkContext
        # Via sc._jvm on peut accéder à des fonctions Scala
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        # On retourne un objet colonne avec l'application de notre udf Scala
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    # Chargement des données depuis le fichier CSV
    sellDF = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")

    # Utilisation de l'UDF Scala pour ajouter la colonne category_name
    dfWithCategoryName = sellDF.withColumn("category_name", addCategoryName(col("category")))

    # Ecriture du résultat au format CSV
    dfWithCategoryName.write.mode("overwrite").csv("data/exo4/scala_udf", header=True)

    # N'oubliez pas d'arrêter la session Spark à la fin
    spark.stop()

if __name__ == "__main__":
    main()