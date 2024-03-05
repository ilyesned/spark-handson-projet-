import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    # Création d'une SparkSession
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()

    input_path = '/home/christian/spark-handson/src/resources/exo1/data.csv'
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    wordcount_result = wordcount(df, "text")
    print("Wordcount result : ")
    wordcount_result.show()

    # Écrire le résultat au format Parquet
    output_path = "data/exo1/output"
    wordcount_result.write.partitionBy("count").parquet(output_path, mode="overwrite")
    print(f"Result written to {output_path}")

    # Arrêt de la SparkSession à la fin du traitement
    spark.stop()


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
