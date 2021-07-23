package paypay.challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import paypay.challenge._
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.JavaConversions._
import org.apache.spark.sql.Row

trait SessionAnalyzerUtil {
  def getDF(spark: SparkSession): DataFrame = {

    val rawLog = Seq(
      Row(
        "2015-07-22T00:54:00.454767Z",
        "marketpalce-shop",
        "123.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T00:54:35.454767Z",
        "marketpalce-shop",
        "923.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T00:55:35.454767Z",
        "marketpalce-shop",
        "123.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T01:01:35.454767Z",
        "marketpalce-shop",
        "123.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T00:59:35.454767Z",
        "marketpalce-shop",
        "923.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T01:30:35.454767Z",
        "marketpalce-shop",
        "123.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T01:22:35.454767Z",
        "marketpalce-shop",
        "923.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T01:23:35.454767Z",
        "marketpalce-shop",
        "923.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      ),
      Row(
        "2015-07-22T01:32:35.454767Z",
        "marketpalce-shop",
        "123.456.789.130:54635",
        "10.0.6.158:80",
        "0.000022",
        "0.026109",
        "0.00002",
        "200",
        "200",
        "0",
        "699",
        "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36",
        "ECDHE-RSA-AES128-GCM-SHA256",
        "TLSv1.2"
      )
    )
    import spark.implicits._
    // val df = rawLog.toDF(
    //   "timestamp",
    //   "elb",
    //   "clientPort",
    //   "backendPort",
    //   "requestProcessingTime",
    //   "backendProcessingTime",
    //   "responseProcessingTime",
    //   "elbStatusCode",
    //   "backendStatusCode",
    //   "receivedBytes",
    //   "sentBytes",
    //   "request",
    //   "userAgent",
    //   "sslCipher",
    //   "sslProtocol"
    // )
    import org.apache.spark.sql.types._
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
    val df = spark.createDataFrame(rawLog, rawSchema)
    df
  }
}
