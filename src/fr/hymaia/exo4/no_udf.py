from pyspark.sql.functions import when, sum, col
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def main():

    spark = SparkSession.builder \
        .appName("no_udf") \
        .master("local[*]") \
        .getOrCreate()
    
    input_path = "src/resources/exo4/sell.csv"
    df = spark.read.csv(input_path, header=True)

    df_category = add_category_name(df)

    df_window_day = add_window_day(df_category)

    # Ecriture du résultat au format CSV
    df_category.write.mode("overwrite").csv("data/exo4/no_udf", header=True)

    spark.stop()

# Fonction de split des zip codes en département
def add_category_name(df):
    df = df.withColumn("category_name", \
                       when(col("category") < 6, "food") \
                        .otherwise("furniture"))
    return df

def add_window_day(df):
    window_day = Window.partitionBy("category_name", "date")
    return df.withColumn("total_price_per_category_per_day", sum("price").over(window_day))

if __name__ == "__main__":
    main()