

import org.apache.spark.sql.SparkSession
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source.fromURL

object apiRequestApp extends Serializable {

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  case class Markets(
                      MarketCurrency: String,
                      BaseCurrency: String,
                      MarketCurrencyLong: String,
                      BaseCurrencyLong: String,
                      MinTradeSize: Double,
                      MarketName: String,
                      IsActive: Boolean,
                      Created: String,
                      Notice: Option[String],
                      IsSponsored: Option[Boolean],
                      LogoUrl: String
                    )


  case class Result(success: Boolean,
                    message: String,
                    result: List[Markets])


  def main(args: Array[String]): Unit = {

     implicit  val formats = org.json4s.DefaultFormats

    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.shuffle.partitions", "4")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val parsedData = parse(fromURL("https://bittrex.com/api/v1.1/public/getmarkets").mkString).extract[Result]
    val mySourceDataset = spark.createDataset(parsedData.result)
    mySourceDataset.printSchema
    mySourceDataset.show(10)
  }
}
