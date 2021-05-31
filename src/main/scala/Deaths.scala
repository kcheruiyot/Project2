import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, when}

import scala.io.StdIn

object Deaths {

  val spark = SparkSession
    .builder
    .appName("Covid")
    .master("local[*]")
    .getOrCreate()

  val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020')"

  def MostDeaths(): Unit = {

    val deadDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs://localhost:9000/user/project2/covid_19_data.csv")
    deadDF.createOrReplaceTempView("globalDeath")

    spark.sql("SELECT `Country/Region` AS Country, MAX(Deaths) AS Deaths" +
      " FROM globalDeath GROUP BY `Country/Region` ORDER BY Deaths DESC").show(50)

    spark.close()
  }

  def AvgCaseFatalityRateInc(): Unit = {

    // shows the top 20 countries with highest average monthly case fatality rates.

    val fatalDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs://localhost:9000/user/project2/covid_19_data.csv")
    fatalDF.createOrReplaceTempView("fatal")

    val fatalDF2 = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) AS Confirmed, " +
      s"SUM(Deaths) AS Deaths FROM fatal WHERE ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate " +
      s"ORDER BY Country, ObservationDate")
    fatalDF2.createOrReplaceTempView("fatal2")

    val fatalDF3 = spark.sql("SELECT ObservationDate, Country, Confirmed, Deaths, " +
      "format_number((Deaths / Confirmed) * 100, 2) AS CaseFatalityRate FROM fatal2 " +
      "ORDER BY Country, ObservationDate")

    val fatalDF4 = fatalDF3.withColumn("CaseFatalityRate", when(col("CaseFatalityRate")
      .isNull, 0.0: Double).otherwise(col("CaseFatalityRate").cast("Double")))
    fatalDF4.createOrReplaceTempView("fatal4")

    val fatalDF5 = spark.sql("SELECT ObservationDate, Country, " +
      "Confirmed, LAG(Confirmed) OVER (PARTITION BY Country ORDER BY Country) AS ConfirmedB4, " +
      "Deaths, LAG(Deaths) OVER (PARTITION BY Country ORDER BY Country) AS DeathsB4, " +
      "CaseFatalityRate, LAG(CaseFatalityRate) OVER (PARTITION BY Country ORDER BY Country) AS CFRB4 " +
      "FROM fatal4 ORDER BY Country, ObservationDate").na.fill(0.0:Double)
    fatalDF5.createOrReplaceTempView("fatal5")

    val fatalDF6 = spark.sql("SELECT ObservationDate, Country, " +
      "(Confirmed - ConfirmedB4) AS ConfirmedIncFromPrevMonth, " +
      "(Deaths - DeathsB4) AS DeathsIncFromPrevMonth, " +
      "(CaseFatalityRate - CFRB4) AS CFRIncFromPrevMonth " +
      "FROM fatal5 ORDER BY Country, ObservationDate")
    fatalDF6.createOrReplaceTempView("fatal6")

    spark.sql("SELECT Country AS `Country Name`, " +
      "COUNT(ObservationDate) AS `Num Of Observations`, " +
      "format_number(AVG(ConfirmedIncFromPrevMonth), 0) AS `AVG Confirmed Inc Per Month in 2020`, " +
      "format_number(AVG(DeathsIncFromPrevMonth), 0) AS `AVG Deaths Inc Per Month in 2020`, " +
      "format_string('%.2f %%', AVG(CFRIncFromPrevMonth)) AS `AVG CFR Inc Per Month in 2020`" +
      "FROM fatal6 GROUP BY `Country Name` ORDER BY `AVG CFR Inc Per Month in 2020` DESC").show()

    spark.close()
  }

  def CaseFatalityRate(): Unit = {

    // Shows the top 20 countries with highest case fatality rate.

    val avgfatalDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("hdfs://localhost:9000/user/project2/covid_19_data.csv")
    avgfatalDF.createOrReplaceTempView("avgfatal")

    val avgfatalDF2 = spark.sql("SELECT `Country/Region` AS Country, MAX(Confirmed) AS Confirmed, MAX(Deaths) " +
      "AS Deaths FROM avgfatal GROUP BY `Country/Region` ORDER BY Deaths DESC")
    avgfatalDF2.createOrReplaceTempView("avgfatal2")

    val avgfatalDF3 = spark.sql("SELECT Country, Confirmed, Deaths, " +
      "format_string('%.2f %%', cast((Deaths / Confirmed) * 100 AS FLOAT)) AS CaseFatalityRate FROM avgfatal2 " +
      "ORDER BY CaseFatalityRate DESC")

    val avgfatalDF4 = avgfatalDF3.withColumn("CaseFatalityRate", when(col("CaseFatalityRate")
      .equalTo("nu %"), "0.00 %").otherwise(col("CaseFatalityRate")))
      .orderBy(desc("CaseFatalityRate"))
    avgfatalDF4.createOrReplaceTempView("avgfatal4")

    spark.sql("SELECT Country AS `Country Name`, " +
      "Confirmed AS `Total Confirmed Cases in 2020`, " +
      "Deaths AS `Total Deaths in 2020`, " +
      "CaseFatalityRate AS `2020 Case Fatality Rate` " +
      "FROM avgfatal4 ORDER BY `2020 Case Fatality Rate` DESC").show()

    spark.close()
  }
}
