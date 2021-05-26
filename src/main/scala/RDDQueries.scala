import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection.universe.show
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, lit, udf, when}


object RDDQueries {
  val sc = new SparkContext("local[*]", "Covid_Cases")
  def totalCovidCases():Int = {
    val covid = sc.textFile("hdfs://localhost:9000/user/project2/covid_19_data.csv").map(line=>line.split(","))
    def isNum(s:String):Boolean  = {
      def isNumber: Boolean = s.matches("[+-]?\\d+.?\\d+")
      isNumber
    }
    val confirmed = covid.filter((line=>isNum(line(5))))
    val confirmedInt  = confirmed.map(line=>line(5).toDouble)
      confirmedInt.reduce(_+_).toInt
  }

  def monthlyTrends():Unit = {
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()

    val endMonth = Seq("1/30","2/28","2/29","3/31","4/30","5/31","6/30","7/31","8/31","9/30","10/31","11/30","12/31")
    val covid = sc.textFile("hdfs://localhost:9000/user/project2/covid_19_data.csv").map(line=>line.split(","))
    def getMMDD(str:String):String = {
      val mmdd = str.split("/")
      if(mmdd.length>1)mmdd(0)+"/"+mmdd(1)
      else ""
    }

    def fromMMDDYY(str:String):Boolean = {
      val mmdd = str.split("/")
      if(mmdd.length>1)endMonth.contains(mmdd(0)+"/"+mmdd(1))
      else false
    }
   // spark.udf.register("getMMDD",getMMDD)

    val covidRDD = covid.map(line=>(line(0),line(1),line(3),line(5), getMMDD(line(1))))
    val x = covidRDD.filter(line=>fromMMDDYY(line._5))
    val y = x.map(a=>(a._2,a._3,a._4))
   // covidRDD.take(10).foreach(println)
    y.sortBy(_._2,false)take(10000)foreach(println)
    println(y.count())
    spark.close()
  }
}

