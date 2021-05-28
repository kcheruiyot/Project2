import LoadData.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, udf}
object GlobalMonthlyCases extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val total = LoadData.covidGlobal()
  val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020'," +
    " '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', " +
    "'10/31/2020', '11/30/2020', '12/31/2020', '01/31/2021', '02/28/2021', " +
    "'03/31/2021', '04/30/2021', '05/02/2021' )"
  total.createTempView("global_cases")

  val cumulativeTotal = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) as Confirmed FROM global_cases WHERE  ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country DESC, Confirmed")
  cumulativeTotal.createTempView("cummulativeTotal")
  val monthlyTotal = spark.sql("SELECT ObservationDate, Country, Confirmed, Confirmed - lag(Confirmed) OVER (ORDER BY Confirmed) as Monthly FROM cummulativeTotal")
  monthlyTotal.createTempView("monthlyTotal")

  val maxMonth = spark.sql("SELECT Country, MAX(Monthly) as `Max` FROM monthlyTotal GROUP BY Country ORDER BY `Max` DESC")
  maxMonth.createTempView("maxMonthly")
  def toMMM  = udf((str:String) => {
    val arr = str.split("/")
    val mm = arr(0)
    var month = ""
    mm match {
      case "01" =>month =  "Jan"
      case "02" =>month =  "Feb"
      case "03" =>month = "Mar"
      case "04" =>month =  "Apr"
      case "05" =>month =  "May"
      case "06" =>month =  "Jun"
      case "07" =>month =  "Jul"
      case "08" =>month =  "Aug"
      case "09" =>month =  "Sep"
      case "10" =>month =  "Oct"
      case "11" =>month =  "Nov"
      case "12" =>month =  "Dec"
      case _ =>month =  "Unknown"
    }
    if(month=="Unknown") "Unknown"
    else
      month + " " + arr(2)
  }
  )
  spark.udf.register("toMMM",toMMM)
  val monthlyCases = spark.sql("SELECT a.Country AS COUNTRY, a.ObservationDate AS `Month`, a.Monthly AS `TOTAL CASES` FROM monthlyTotal AS a" +
    " INNER JOIN maxMonthly AS b ON (a.Country=b.Country AND a.Monthly=b.Max) ORDER BY MONTHLY DESC")
    .withColumn("MMM",toMMM(col("Month")))

  monthlyCases.createTempView("monthlyCases")

  println("Table showing the month each country peaked in covid cases")

  spark.sql("SELECT COUNTRY, MMM AS MONTH, `TOTAL CASES` FROM monthlyCases").show(250)
spark.close()
}
