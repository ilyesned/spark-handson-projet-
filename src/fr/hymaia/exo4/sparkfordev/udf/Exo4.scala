// src/fr/hymaia/exo4/Exo4.scala

package fr.hymaia.exo4

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object Exo4 {
  def addCategoryNameCol(): UserDefinedFunction = udf((category: Int) => {
    if (category < 6) "food" else "furniture"
  })

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ScalaUDF").getOrCreate()

    // Charger les données et effectuer les transformations nécessaires ici

    // Arrêter la session Spark
    spark.stop()
  }
}
