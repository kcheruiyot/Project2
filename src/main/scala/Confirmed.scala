import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Confirmed {

  def confirmedTrends(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Covid")
      .master("local[*]")
      .getOrCreate()
    val confirmedDF = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://localhost:9000/user/sadcat/project2/time_series_covid_19_confirmed.csv")

    // Months to work with.
    val datesJan20 = confirmedDF.columns.slice(4, 14).toSeq
    val datesFeb20 = confirmedDF.columns.slice(14, 43).toSeq
    val datesMar20 = confirmedDF.columns.slice(43, 74).toSeq
    val datesApr20 = confirmedDF.columns.slice(74, 104).toSeq
    val datesMay20 = confirmedDF.columns.slice(104, 135).toSeq
    val datesJun20 = confirmedDF.columns.slice(135, 165).toSeq
    val datesJul20 = confirmedDF.columns.slice(165, 196).toSeq
    val datesAug20 = confirmedDF.columns.slice(196, 227).toSeq
    val datesSep20 = confirmedDF.columns.slice(227, 257).toSeq
    val datesOct20 = confirmedDF.columns.slice(257, 288).toSeq
    val datesNov20 = confirmedDF.columns.slice(288, 318).toSeq
    val datesDec20 = confirmedDF.columns.slice(318, 349).toSeq
    val datesJan21 = confirmedDF.columns.slice(349, 380).toSeq
    val datesFeb21 = confirmedDF.columns.slice(380, 408).toSeq
    val datesMar21 = confirmedDF.columns.slice(408, 439).toSeq
    val datesApr21 = confirmedDF.columns.slice(439, 469).toSeq
    val datesMay21 = confirmedDF.columns.slice(469, 471).toSeq

    val allDates = Array(datesJan20, datesFeb20, datesMar20, datesApr20, datesMay20, datesJun20
      , datesJul20, datesAug20, datesSep20, datesOct20, datesNov20, datesDec20, datesJan21
      , datesFeb21, datesMar21, datesApr21, datesMay21)
    //println(allDates.size) 17

    confirmedDF.createOrReplaceTempView("confirmed")

    val jan20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesJan20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      jan20Shell += addDF
    }
    val jan20confirmedDF = jan20Shell.reduce(_ union _)
    //jan20confirmedDF.show()

    val feb20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesFeb20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      feb20Shell += addDF
    }
    val feb20confirmedDF = feb20Shell.reduce(_ union _)
    //feb20confirmedDF.show()

    val mar20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesMar20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      mar20Shell += addDF
    }
    val mar20confirmedDF = mar20Shell.reduce(_ union _)

    val apr20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesApr20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      apr20Shell += addDF
    }
    val apr20confirmedDF = apr20Shell.reduce(_ union _)

    val may20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesMay20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      may20Shell += addDF
    }
    val may20confirmedDF = may20Shell.reduce(_ union _)

    val jun20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesJun20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      jun20Shell += addDF
    }
    val jun20confirmedDF = jun20Shell.reduce(_ union _)

    val jul20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesJul20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      jul20Shell += addDF
    }
    val jul20confirmedDF = jul20Shell.reduce(_ union _)

    val aug20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesAug20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      aug20Shell += addDF
    }
    val aug20confirmedDF = aug20Shell.reduce(_ union _)

    val sep20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesSep20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      sep20Shell += addDF
    }
    val sep20confirmedDF = sep20Shell.reduce(_ union _)

    val oct20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesOct20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      oct20Shell += addDF
    }
    val oct20confirmedDF = oct20Shell.reduce(_ union _)

    val nov20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesNov20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      nov20Shell += addDF
    }
    val nov20confirmedDF = nov20Shell.reduce(_ union _)

    val dec20Shell = ArrayBuffer[DataFrame]()
    for (x <- datesDec20) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      dec20Shell += addDF
    }
    val dec20confirmedDF = dec20Shell.reduce(_ union _)

    val jan21Shell = ArrayBuffer[DataFrame]()
    for (x <- datesJan21) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      jan21Shell += addDF
    }
    val jan21confirmedDF = jan21Shell.reduce(_ union _)

    val feb21Shell = ArrayBuffer[DataFrame]()
    for (x <- datesFeb21) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      feb21Shell += addDF
    }
    val feb21confirmedDF = feb21Shell.reduce(_ union _)

    val mar21Shell = ArrayBuffer[DataFrame]()
    for (x <- datesMar21) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      mar21Shell += addDF
    }
    val mar21confirmedDF = mar21Shell.reduce(_ union _)

    val apr21Shell = ArrayBuffer[DataFrame]()
    for (x <- datesApr21) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      apr21Shell += addDF
    }
    val apr21confirmedDF = apr21Shell.reduce(_ union _)

    val may21Shell = ArrayBuffer[DataFrame]()
    for (x <- datesMay21) {
      val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$x' AS Date, `$x` AS Count FROM confirmed")
      may21Shell += addDF
    }
    val may21confirmedDF = may21Shell.reduce(_ union _)

    //val datelist = ArrayBuffer[DataFrame]()
    //
    //for (x <- allDates) {
    //  for (y <- x) {
    //    val addDF = spark.sql(s"SELECT `Province/State`, `Country/Region`, '$y' AS Date, `$y` AS Count FROM confirmed WHERE `$y` = (SELECT MAX(`$y`) FROM confirmed)")
    //    datelist += addDF
    //  }
    //}
    //datelist.reduce(_ union _).show()
    //
    //for (x <- allDates(0)) {
    //  println(x)
    //}
  }
}
