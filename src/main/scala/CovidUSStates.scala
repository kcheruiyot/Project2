import java.sql.Date

case class CovidUSStates(SNo: Long, observationDate: Date, `Province/State`: String, `Country/Region`: String,
                         LastUpdate: String, Confirmed: Double, Deaths: Double, Recovered: Double)
