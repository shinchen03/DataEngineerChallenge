package paypay.challenge

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}
import paypay.challenge._

object SessionAnalyzer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SessionAnalyzer")
      .getOrCreate()

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

    // Since this tested in the Azure Databricks, I read the file from DBFS here
    val df_raw = spark.read
      .option("header", "false")
      .option("inferSchema", false)
      .option("delimiter", " ")
      .schema(rawSchema)
      .csv("dbfs:/FileStore/2015_07_22_mktplace_shop_web_log_sample_log.gz")
    // Define the interval to 15 min
    val df = getSessionizedDf(df_raw, spark, 15)
    val df_task1 = getAggregatePageHits(df, spark)
    val df_task2 = getAverageSessionTime(df, spark)
    val df_task3 = getUniqueURLPerSession(df, spark)
    val df_task4 = getMostEngatedUsers(df, spark)
    // Display result in Databricks
    // display(df_task1)
    // display(df_task2)
    // display(df_task3)
    // display(df_task4)
  }

  def getSessionizedDf(
      df_raw: DataFrame,
      spark: SparkSession,
      interval: Integer
  ): Dataset[SessionAnalyzerSchema] = {

    val getURL = udf((url: String) => url.split(" ")(1).split("\\?")(0))
    val getIP = udf((ip_port: String) => ip_port.split(":")(0))
    val getUserAgent = udf((agent: String) => agent.split(" ")(0))
    // Partition by clientIP and userAgent, assume a user can use only one ip and one userAgent at the same time.
    // Some data have no userAgent. Need to understand why this happens and clean the data.
    val sessionWindow =
      Window.partitionBy("clientIP", "userAgent").orderBy("timestamp")
    import spark.implicits._
    val df = df_raw
      .withColumn("url", getURL(col("request")))
      .withColumn("clientIP", getIP(col("clientPort")))
      .withColumn("userAgent", getUserAgent(col("userAgent")))
    val sessionizeddf = df
      .withColumn(
        "prevTimestamp",
        lag(col("timestamp"), 1).over(sessionWindow)
      )
      .withColumn(
        "accessLag",
        unix_timestamp(col("timestamp")) - unix_timestamp(
          col("prevTimestamp")
        )
      )
      // Every access after {interval} min since last time, we define it as a new session.
      .withColumn("sessionId", unix_timestamp)
      .withColumn(
        "isNewSession",
        when(col("accessLag") > interval * 60, true)
          .when(col("accessLag").isNull, true)
          .otherwise(false)
      )
      // Not include the userAgent, since a user can use different userAgents.
      .withColumn(
        "sessionId",
        concat(
          col("clientIP"),
          lit("/"),
          sum(
            when(col("isNewSession"), 1)
              .otherwise(0)
          )
            .over(sessionWindow)
        )
      )
      .select(
        "timestamp",
        "clientIP",
        "elbStatusCode",
        "backendStatusCode",
        "url",
        "userAgent",
        "isNewSession",
        "sessionId"
      )
      .as[SessionAnalyzerSchema]
    sessionizeddf
  }

  def getAggregatePageHits(
      sessionizeddf: Dataset[SessionAnalyzerSchema],
      spark: SparkSession
  ): DataFrame = {
    // 1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session

    // Filter the error elb status codes, aggregate by session id.
    // Gourp by sessionId and userAgent, since a user can have same sessionId if the user is using different userAgent.
    val df_task1 =
      sessionizeddf
        .filter(col("elbStatusCode") < 400)
        .groupBy("sessionId", "userAgent")
        .count()
        .orderBy(desc("count"))
    df_task1
  }

  def getAverageSessionTime(
      sessionizeddf: Dataset[SessionAnalyzerSchema],
      spark: SparkSession
  ): DataFrame = {
    // 2. Determine the average session time

    // Group by sessionId and get the session start timestamp and session end timestamp, to calculate the sessionTime.
    val df_task2 = sessionizeddf
      .filter(col("elbStatusCode") < 400)
      .groupBy("sessionId", "userAgent")
      .agg(
        max("timestamp").alias("sessionEnd"),
        min("timestamp").alias("sessionStart")
      )
      .withColumn(
        "sessionTime",
        unix_timestamp(col("sessionEnd")) - unix_timestamp(
          col("sessionStart")
        )
      )
      .agg(avg("sessionTime").alias("avgSessionTime"))
    df_task2
  }

  def getUniqueURLPerSession(
      sessionizeddf: Dataset[SessionAnalyzerSchema],
      spark: SparkSession
  ): DataFrame = {
    // Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

    // Count distinct urls during a session
    val df_task3 = sessionizeddf
      .filter(col("elbStatusCode") < 400)
      .groupBy("sessionId", "userAgent")
      .agg(countDistinct(col("url")).alias("uniqueVisitURL"))
      .orderBy(desc("uniqueVisitURL"))
    df_task3
  }

  def getMostEngatedUsers(
      sessionizeddf: Dataset[SessionAnalyzerSchema],
      spark: SparkSession
  ): DataFrame = {
    // Find the most engaged users, ie the IPs with the longest session times

    // Calculate the session time of each sessionId, and order by the sessionTime, select the user to get the most engaged users.
    val df_task4 = sessionizeddf
      .filter(col("elbStatusCode") < 400)
      .groupBy("sessionId", "userAgent")
      .agg(
        max("timestamp").alias("sessionEnd"),
        min("timestamp").alias("sessionStart")
      )
      .withColumn(
        "sessionTime",
        unix_timestamp(col("sessionEnd")) - unix_timestamp(
          col("sessionStart")
        )
      )
      .orderBy(desc("sessionTime"))
    df_task4
  }
}
