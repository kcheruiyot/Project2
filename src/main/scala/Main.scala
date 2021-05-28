import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()
    //val covidDF = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/covid_19_data.csv")
    // covidDF.select("Province/State", "Country/Region").show(5)
    //covidDF.select("*").show(15)
    //covidDF.select()

    //Deaths.deathTrend()
   Deaths.CaseFatalityRate()

    //val confirmedDF = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_confirmed.csv")
    //confirmedDF.show(15)

  }

}