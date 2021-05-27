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


  }
}
