from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("exo2_agregate") \
        .master("local[*]") \
        .getOrCreate()
    
    input_path = 'data/exo2/output'
    output_path = 'data/exo2/aggregate'
    
    df = spark.read.parquet(input_path, header=True, inferSchema=True)
    df.show()

    # Calcul de la population par département
    population_by_department_df = calculate_population_by_department(df)

    # Écriture du résultat au format CSV
    write_csv(population_by_department_df, output_path)

    spark.stop()

def calculate_population_by_department(df):
    return df.groupBy("departement").agg(count("*").alias("nb_people")).orderBy(["nb_people", "departement"])


def write_csv(df, output_path):
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


if __name__ == "__main__":
    main()