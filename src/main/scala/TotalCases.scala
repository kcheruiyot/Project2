import LoadData.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions.{col, udf}
object TotalCases {
//  val spark = SparkSession
//    .builder
//    .appName("Covid")
//    .master("local[*]")
//    .getOrCreate()
  def globalTotal():Int = {
    val total = LoadData.covidGlobal()
  val endMonth = "('01/31/2020', '02/29/2020', '03/31/2020', '04/30/2020', '05/31/2020', '06/30/2020', '07/31/2020', '08/31/2020', '09/30/2020', '10/31/2020', '11/30/2020', '12/31/2020')"
  total.createTempView("global_cases")

  //val cummulativeTotal = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) as Confirmed FROM global_cases WHERE `Country/Region` = 'US' AND ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY ObservationDate")
  val cummulativeTotal = spark.sql(s"SELECT ObservationDate, `Country/Region` as Country, SUM(Confirmed) as Confirmed FROM global_cases WHERE  ObservationDate IN $endMonth GROUP BY `Country/Region`, ObservationDate ORDER BY Country DESC, Confirmed")
 // cummulativeTotal.show(200)
cummulativeTotal.createTempView("cummulativeTotal")

 val monthlyTotal = spark.sql("SELECT ObservationDate, Country, Confirmed, Confirmed - lag(Confirmed) OVER (ORDER BY Confirmed) as Monthly FROM cummulativeTotal")
//monthlyTotal.show(20)
  monthlyTotal.createTempView("monthlyTotal")
  val maxMonth = spark.sql("SELECT Country, MAX(Monthly) as `Max` FROM monthlyTotal GROUP BY Country ORDER BY `Max` DESC")
  maxMonth.createTempView("maxMonthly")
  //maxMonth.show(20)
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
  spark.sql("SELECT COUNTRY, MMM AS MONTH, `TOTAL CASES` FROM monthlyCases").show(250)
// spark.sql("SELECT ObservationDate, Country, Monthly FROM monthlyTotal WHERE Monthly IN(" +
  //"SELECT MAX(Monthly) FROM monthlyTotal GROUP BY Country) ORDER BY Country DESC").show(200)

  // spark.sql("SELECT * FROM global_cases WHERE ObservationDate = '1/31/2020' ").show(25)
//    spark.sql("SELECT SUM(Confirmed) FROM global_cases" +
//      " WHERE ObservationDate = '05/02/2021' AND `Country/Region` = 'US' ").show(25)
 // spark.sql("SELECT * FROM global_cases" +
   // " WHERE ObservationDate = '05/02/2021' AND `Country/Region` = 'US' ").show(100)
  spark.close()
    0
  }

}
