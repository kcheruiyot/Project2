import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf, when}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

object Deaths {

  def MonthlyDeathPerChange(): Unit = {
    // % change of number of deaths per month
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()

    // Defining some values for use
    val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020')"
    val deathrate = udf((x: Double, y: Double) => ((x - y)/y)*100)

    val deathDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/user/sadcat/project2/covid_19_data.csv")
    deathDF.createOrReplaceTempView("globalDeath")
    val deathDF2 = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Deaths) AS Deaths FROM globalDeath WHERE ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country ASC, ObservationDate")
    deathDF2.createOrReplaceTempView("globalDeath2")
    val deathDF3 = spark.sql("SELECT *, LAG(Deaths) OVER (PARTITION BY Country ORDER BY Country) AS PrevMonthDeaths FROM globalDeath2 ORDER BY Country, ObservationDate")
    val deathDF4 = deathDF3.withColumn("MonthlyDeathPerChange", deathrate(deathDF3("Deaths"), deathDF3("PrevMonthDeaths")))
    val deathDF5 = deathDF4.withColumn("MonthlyDeathPerChange", when(col("MonthlyDeathPerChange")
      .isin(Double.PositiveInfinity) or col("MonthlyDeathPerChange").isNull, "N/A").otherwise(col("MonthlyDeathPerChange")))
    deathDF5.createOrReplaceTempView("globalDeath5")
    val deathDF6 = spark.sql("SELECT ObservationDate, Country, Deaths, MonthlyDeathPerChange FROM globalDeath5")
    deathDF6.createOrReplaceTempView("globalDeath6")

    val CountryName = StdIn.readLine("Which Country would you like to view?    ")
    spark.sql(s"SELECT * FROM globalDeath6 WHERE Country = '$CountryName'").show()

    spark.close()

  }
  def CaseFatalityRate(): Unit = {
    // proportion of deaths from a certain disease compared to the total number of people diagnosed with disease
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()

    // Defining some values for use
    val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020')"
    val casefatalityrate = udf((x: Double, y: Double) => (y/x) * 100)

    val fatalDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/user/sadcat/project2/covid_19_data.csv")
    fatalDF.createOrReplaceTempView("fatal")
    val fatalDF2 = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) AS Confirmed, SUM(Deaths) AS Deaths FROM fatal WHERE ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country, ObservationDate")
    val fatalDF3 = fatalDF2.withColumn("Case-Fatality Rate", casefatalityrate(fatalDF2("Confirmed"), fatalDF2("Deaths")))
    fatalDF3.createOrReplaceTempView("fatal3")

    val CountryName = StdIn.readLine("Which Country would you like to view?    ")
    spark.sql(s"SELECT * FROM fatal3 WHERE Country = '$CountryName'").show()
    spark.close()
  }
}
