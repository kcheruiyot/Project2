import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf, when}

import scala.collection.mutable.ArrayBuffer

object Deaths {

  def deathTrend(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()
    val deathDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/user/sadcat/project2/covid_19_data.csv")

    val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020', '1/31/2021', '2/28/2021', '3/31/2021', '4/30/21')"
    deathDF.createOrReplaceTempView("globalDeath")

    //spark.sql("SELECT * FROM globalDeath").show()
    val deathDF2 = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Deaths) AS Deaths FROM globalDeath WHERE ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country ASC, Deaths")
    //deathDF2.show()

    deathDF2.createOrReplaceTempView("globalDeath2")

    val deathDF3 = spark.sql("SELECT *, LAG(Deaths) OVER (PARTITION BY Country ORDER BY Country) AS PrevMonthDeaths FROM globalDeath2 ORDER BY Country, Deaths")

    val deathrate = udf((x: Double, y: Double) => ((x - y)/y)*100)
    val deathDF4 = deathDF3.withColumn("MonthlyPerChange", deathrate(deathDF3("Deaths"), deathDF3("PrevMonthDeaths")))

    val deathDF5 = deathDF4.withColumn("MonthlyPerChange", when(col("MonthlyPerChange")
      .isin(Double.PositiveInfinity) or col("MonthlyPerChange").isNull, "N/A").otherwise(col("MonthlyPerChange")))
    deathDF5.createOrReplaceTempView("globalDeath5")

    val deathDF6 = spark.sql("SELECT ObservationDate, Country, Deaths, MonthlyPerChange FROM globalDeath5")
    deathDF6.show()
    spark.close()


  }
  def CaseFatalityRate(): Unit = {
    // proportion of deaths from a certain disease compared to the total number of people diagnosed with disease
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()

    val fatalDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/user/sadcat/project2/covid_19_data.csv")

    val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020', '1/31/2021', '2/28/2021', '3/31/2021', '4/30/21')"
    fatalDF.createOrReplaceTempView("fatal")
    //spark.sql("SELECT * FROM fatal").show()

    val fatalDF2 = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) AS Confirmed, SUM(Deaths) AS Deaths FROM fatal WHERE ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country ASC, Confirmed")

    val casefatalityrate = udf((x: Double, y: Double) => (y/x) * 100)
    val fatalDF3 = fatalDF2.withColumn("Case-Fatality Rate", casefatalityrate(fatalDF2("Confirmed"), fatalDF2("Deaths")))

    fatalDF3.show()
  }
}
