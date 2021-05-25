import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Hello, Project2")
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()
    //case class Covid(SNo:Int, ObservationDate:String,Province:String,
      //               State:String,Country:String,Region:String,`Last Update`:String,Confirmed:Int,Deaths:Int,Recovered:Int)
    val covidDF = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/cheruiyot/covid_19_data.csv")
   // covidDF.select("Province/State", "Country/Region").show(5)
   covidDF.select("*").show(15)

    val confirmedDF = spark.read.option("header", "true").option("inferSchema","true").csv("hdfs://localhost:9000/user/cheruiyot/time_series_covid_19_confirmed.csv")
//confirmedDF.show(15)

  }

}
