import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    # Création d'une SparkSession
    spark = SparkSession.builder \
        .appName("exo2_clean") \
        .master("local[*]") \
        .getOrCreate()
    
    clients_bdd = 'src/resources/exo2/clients_bdd.csv'
    city_zipcode = 'src/resources/exo2/city_zipcode.csv'
    df1 = spark.read.csv(clients_bdd, header=True, inferSchema=True)
    df2 = spark.read.csv(city_zipcode, header=True, inferSchema=True)

    df1 = filter_age(df1)

    res_join = join_villes(df1, df2, 'zip')

    res_join = add_departement(res_join)
    res_join.show()

    # Écrire le résultat au format Parquet
    output_path = "data/exo2/output"
    res_join.write.parquet(output_path, mode="overwrite")
    print(f"Resultat écrit dans {output_path}")

    
    spark.stop()


def filter_age(df):
    return df.filter(f.col('age') > 18)


def join_villes(df1, df2, join_column):
    df_join = df1.join(df2, on=join_column, how='inner')
    return df_join

def add_departement(df):
    return df.withColumn('departement', f.when(f.substring(f.col('zip'), 1, 2) == 20, f.when(f.col('zip') <= 20190, "2A").when(f.col('zip') > 20190, "2B")).otherwise(f.substring(f.col('zip'), 1, 2)))

def write_parquet(df, file_path):
    return df.write.parquet(file_path)


if __name__ == "__main__":
    main()