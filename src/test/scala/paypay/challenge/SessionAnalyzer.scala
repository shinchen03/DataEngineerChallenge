package paypay.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import paypay.challenge._
import java.sql.Timestamp

class SessionAnalyzerTest extends AnyFlatSpec with SessionAnalyzerUtil {
  "Sessionized result" should "different by session interval" in {
    val spark = SparkSession
      .builder()
      .appName("SessionAnalyzer")
      .config("spark.master", "local")
      .getOrCreate()
    val df = getDF(spark)
    assert(df.count() == 9)
    val sessionizeddf = SessionAnalyzer.getSessionizedDf(df, spark)
    val df_task1 = SessionAnalyzer.getAggregatePageHits(sessionizeddf, spark)
    assert(df_task1.count() == 1)

  }
}
