package paypay.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import paypay.challenge._
import java.sql.Timestamp
import scala.collection.JavaConversions._
import org.apache.spark.sql.Row
import org.joda.time.format.DateTimeFormat

trait SessionAnalyzerUtil {
  def getDF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    val rawSchema = StructType(
      Seq(
        StructField("timestamp", TimestampType, true),
        StructField("elb", StringType, true),
        StructField("clientPort", StringType, true),
        StructField("backendPort", StringType, true),
        StructField("requestProcessingTime", StringType, true),
        StructField("backendProcessingTime", StringType, true),
        StructField("responseProcessingTime", StringType, true),
        StructField("elbStatusCode", IntegerType, true),
        StructField("backendStatusCode", IntegerType, true),
        StructField("receivedBytes", StringType, true),
        StructField("sentBytes", StringType, true),
        StructField("request", StringType, true),
        StructField("userAgent", StringType, true),
        StructField("sslCipher", StringType, true),
        StructField("sslProtocol", StringType, true)
      )
    )
    val df_raw = spark.read
      .option("header", "false")
      .option("inferSchema", false)
      .option("delimiter", " ")
      .schema(rawSchema)
      .csv("src/test/scala/paypay/challenge/testData.log")
    df_raw
  }
}
