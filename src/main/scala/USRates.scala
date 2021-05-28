import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, desc, round, sum}

object USRates extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val usConfirmed = LoadData.covidConfirmedUS()
  val usDeaths = LoadData.covidDeathsUS()

   val confirmed =  usConfirmed.select("Admin2","Province_State","5/2/21")
      .withColumnRenamed("5/2/21", "Confirmed")
    val deaths = usDeaths.select("Admin2", "Population","Province_State", "5/2/21")
      .withColumnRenamed("5/2/21", "Deaths")

    val confirmedDeaths = confirmed.join(deaths, usingColumns = Seq("Province_State","Admin2"))
      .select("Province_State",   "Admin2","Population", "Confirmed", "Deaths")

    val stateAndCountryRates = confirmedDeaths.withColumn("Morbidity (Cases Per 10000)",round(col("Confirmed")/col("Population")*10000, 3))
      .withColumn("Mortality (Cases Per 10000)",round(col("Deaths")/col("Population")*10000, 3)).where("Province_State = 'Texas'")
      .orderBy(desc("Mortality (Cases Per 10000)")).show()

    val statesRates = confirmedDeaths.groupBy("Province_State")
      .agg(sum("Population").alias("Population"), sum("Confirmed")
        .alias("Confirmed"),sum("Deaths").alias("Deaths")).orderBy(desc("Deaths"))
      .withColumn("Morbidity (Cases Per 10000)",round(col("Confirmed")/col("Population")*10000, 3))
      .withColumn("Mortality (Cases Per 10000)",round(col("Deaths")/col("Population")*10000, 3))
      .orderBy(desc("Mortality (Cases Per 10000)"))

    statesRates
   // confirmedDeaths.show
}
