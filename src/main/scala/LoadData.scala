import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadData {
  val spark = SparkSession
    .builder
    .appName("Covid")
    .master("local[*]")
    .getOrCreate()
def covidGlobal():DataFrame = {
  val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/covid_19_data.csv")

  covid
}

  def covidGlobalDeaths():DataFrame = {
    val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_deaths.csv")
    covid
  }

  def covidGlobalConfirmed():DataFrame = {
    val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_confirmed.csv")
    spark.close()
    covid
  }
  def covidGlobalRecovered():DataFrame = {
    val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_recovered.csv")
    covid
  }


  def covidConfirmedUS():DataFrame = {
    val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_confirmed_US.csv")

    covid
  }
  def covidDeathsUS():DataFrame = {
    val covid = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/project2/time_series_covid_19_deaths_US.csv")
    covid
  }

}
