import LoadData.spark
import RecoveryDeathRates.{aggregateRecovery, casesByCountry}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object RecoveryDeathRates extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
    val recovered = LoadData.covidGlobalRecovered()
    val cases = LoadData.covidGlobal()
    val deaths = LoadData.covidGlobalDeaths()
     val casesByCountry=  cases.groupBy("Country/Region","ObservationDate")
      .agg(sum("Confirmed").cast(IntegerType).alias("Confirmed"))
      .where("ObservationDate = '05/02/2021'").orderBy(desc("Confirmed"))
      .withColumnRenamed("Country/Region","Country").drop("ObservationDate")

    val aggregateDeaths = deaths.groupBy("Country/Region").agg(sum("5/2/21")
      .alias("Deaths")).orderBy("Country/Region")
      .withColumnRenamed("Country/Region","Country")


   val aggregateRecovery=  recovered.groupBy("Country/Region").agg(sum("5/2/21")
      .alias("Recovered")).orderBy("Country/Region")
      .withColumnRenamed("Country/Region","Country")

  val combinedData = casesByCountry.join(aggregateRecovery, "Country").join(aggregateDeaths, "Country")
  val rates = combinedData.withColumn("Recovery Rate", (round(expr("Recovered/Confirmed") * 100, 2)))
    .withColumn("Death Rate", (round(expr("Deaths/Confirmed") * 100, 2)))

  val addPercent = udf((str:Double)=>{
    str.toString.concat("%")

  })
  spark.udf.register("addPercentt",addPercent)
  rates.withColumn("Recovery Rate", addPercent(col("Recovery Rate")))
    .withColumn("Death Rate", addPercent(col("Death Rate")))
    .orderBy(desc("Recovery Rate")).show(25)
}