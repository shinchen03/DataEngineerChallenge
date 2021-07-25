package paypay.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import paypay.challenge._
import java.sql.Timestamp
import org.apache.spark.sql.functions._

class SessionAnalyzerTest extends AnyFlatSpec with SessionAnalyzerUtil {
  val spark = SparkSession
    .builder()
    .appName("SessionAnalyzer")
    .config("spark.master", "local")
    .getOrCreate()
  val df = getDF(spark)
  "Sessionized result" should "different by session interval" in {
    assert(df.count() == 13)
    // Case interval = 15
    val sessionizeddf_15_interval =
      SessionAnalyzer.getSessionizedDf(df, spark, 15)
    val df_task1_15_interval =
      SessionAnalyzer.getAggregatePageHits(sessionizeddf_15_interval, spark)
    assert(
      df_task1_15_interval
        .filter(col("sessionId") === "182.64.154.245/1")
        .select("count")
        .first()
        .getLong(0) == 5
    )
    val df_task2_15_interval =
      SessionAnalyzer.getAverageSessionTime(sessionizeddf_15_interval, spark)
    assert(
      df_task2_15_interval
        .select("avgSessionTime")
        .first()
        .getDouble(0) == 70.0
    )
    val df_task3_15_interval =
      SessionAnalyzer.getUniqueURLPerSession(sessionizeddf_15_interval, spark)
    assert(
      df_task3_15_interval
        .filter(col("sessionId") === "182.64.154.245/1")
        .select("uniqueVisitURL")
        .first()
        .getLong(0) == 5
    )
    val df_task4_15_interval =
      SessionAnalyzer.getMostEngatedUsers(sessionizeddf_15_interval, spark)
    assert(
      df_task4_15_interval
        .select("sessionTime")
        .first()
        .getLong(0) == 240
    )

    // Case interval = 30
    // Should differenct with the result interval = 15
    val sessionizeddf_30_interval =
      SessionAnalyzer.getSessionizedDf(df, spark, 30)
    val df_task1_30_interval =
      SessionAnalyzer.getAggregatePageHits(sessionizeddf_30_interval, spark)
    assert(
      df_task1_30_interval
        .filter(col("sessionId") === "182.64.154.245/1")
        .select("count")
        .first()
        .getLong(0) == 6
    )

    val df_task2_30_interval =
      SessionAnalyzer.getAverageSessionTime(sessionizeddf_30_interval, spark)
    assert(
      df_task2_30_interval
        .select("avgSessionTime")
        .first()
        .getDouble(0) == 735.0
    )

    val df_task3_30_interval =
      SessionAnalyzer.getUniqueURLPerSession(sessionizeddf_30_interval, spark)
    assert(
      df_task3_30_interval
        .filter(col("sessionId") === "182.64.154.245/1")
        .select("uniqueVisitURL")
        .first()
        .getLong(0) == 6
    )
    val df_task4_30_interval =
      SessionAnalyzer.getMostEngatedUsers(sessionizeddf_30_interval, spark)
    assert(
      df_task4_30_interval
        .select("sessionTime")
        .first()
        .getLong(0) == 1740
    )
  }
}
