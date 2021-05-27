import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object USStates extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("Covid")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val covidSchema = StructType(Array(StructField("SNo", LongType), StructField("ObservationDate", DateType),
    StructField("Province/State", StringType), StructField("Country/Region", StringType),
    StructField("LastUpdate", StringType), StructField("Confirmed", DoubleType),
    StructField("Deaths", DoubleType), StructField("Recovered", DoubleType)))

  val covidDS = spark.read.schema(covidSchema)
    .option("header", "true")
    .option("charset", "UTF8")
    .option("delimiter", ",")
    .option("dateFormat", "MM/dd/yyyy")
    .csv("hdfs://localhost:9000/user/project2/covid_19_data.csv").as[CovidUSStates]

  covidDS.createOrReplaceTempView("covid")

  val states = "('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', " +
    "'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', " +
    "'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', " +
    "'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', " +
    "'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', " +
    "'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Vermont', 'Washington', 'West Virginia', " +
    "'Wisconsin', 'Wyoming')"

  val sql2DF = spark.sql(s"SELECT DISTINCT c1.`Country/Region`, c1.`Province/State`, c1.ObservationDate AS Start, " +
    s"c2.ObservationDate AS End, format_number(c2.Confirmed - c1.Confirmed, 0) AS Confirmed, " +
    s"format_number(c2.Deaths - c1.Deaths, 0) AS Deaths, " +
    s"format_string('%.2f%%', cast(((c2.Confirmed - c1.Confirmed) / c3.maxConfirmed) * 100 AS FLOAT)) " +
    s"AS ConfirmedPercent, " +
    s"format_string('%.2f%%', cast(((c2.Deaths - c1.Deaths) / c3.maxDeaths) * 100 AS FLOAT)) " +
    s"AS DeathPercent " +
    s"FROM covid AS c1 INNER JOIN covid AS c2 " +
    s"ON c1.`Province/State` = c2.`Province/State` AND add_months(c1.ObservationDate, 1) = c2.ObservationDate " +
    s"LEFT OUTER JOIN " +
    s"(SELECT `Province/State`, MAX(Confirmed) AS maxConfirmed, MAX(Deaths) AS maxDeaths " +
    s"FROM covid GROUP BY `Province/State`) AS c3 " +
    s"ON c1.`Province/State` = c3.`Province/State` " +
    s"WHERE c1.`Country/Region` = 'US' AND c1.`Province/State` IN $states AND day(c1.ObservationDate) = 1 " +
    s"AND c1.ObservationDate BETWEEN '2020-11-01' AND '2021-02-01' " +
    s"ORDER BY c1.`Province/State`, Start")

  sql2DF.show(200)

  sql2DF.createOrReplaceTempView("covidStates2")

  val sql3DF = spark.sql("SELECT `Province/State`, COUNT(`Province/State`) AS RowCount, " +
    "format_string('%.2f%%', SUM(substring(ConfirmedPercent, 0, length(ConfirmedPercent) - 1))) AS sumConfirmedPercent, " +
    "format_string('%.2f%%', SUM(substring(DeathPercent, 0, length(DeathPercent) - 1))) AS sumDeathPercent " +
    "FROM covidStates2 GROUP BY `Province/State` ORDER BY `Province/State`")

  sql3DF.show(50)

  spark.close()
}
