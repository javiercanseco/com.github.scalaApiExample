

import java.net.{HttpURLConnection, URL}
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.SparkSession

object apiRequestApp extends Serializable {

  org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
  case  class Prices(
                    value:Double,
                    datetime: String,
                    datetime_utc: String,
                    tz_time: String,
                    geo_id: Int,
                    geo_name: String)

  def main(args: Array[String]): Unit = {

     implicit  val formats = org.json4s.DefaultFormats

    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.shuffle.partitions", "4")
      .master("local[*]")
      .getOrCreate()
    

    // https://api.esios.ree.es/indicators/10211?start_date=20200101T22:00:00Z&end_date=20210103T21:00:00Z&time_trunc=hour    < api target
    val table_api ="10211"
    val startdate= "20200101T22:00:00"
    val enddate= "20210103T21:00:00"
    //Creating Connnection to ApiRest.
    val connection = new URL(s"https://api.esios.ree.es/indicators/${table_api}?start_date=${startdate}Z&end_date=${enddate}Z&time_trunc=hour").openConnection.asInstanceOf[HttpURLConnection]
    connection.setRequestProperty("Authorization", "Token token=  < add your Personal Token without quotation marks...> ")
    // Parsing json Files into case Class
    val parsedData = parse(scala.io.Source.fromInputStream(connection.getInputStream).mkString)
    val listPrices =(parsedData \\"values").extract[List[Prices]]
    import spark.implicits._
    // Creating Dataset and showing outputs
    val mySourceDataset = spark.createDataset(listPrices)
    mySourceDataset.printSchema
    mySourceDataset.show(30,false )
  }
}
