import java.io.{BufferedReader, File, FileReader}

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.circe._


case class Properties(action: String, _type: String, breakdown_schema: Option[Map[String, Properties]])

object Properties{
  implicit val encoder: Encoder[Properties] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[Properties] = io.circe.generic.semiauto.deriveDecoder
}

trait ConfigReader extends LazyLogging {

  //JSON Config
  val userInput: Json = {
    val reader = new BufferedReader(new FileReader("src/config/user-input.json"))
    val ui: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
    reader.close()
    parser.parse(ui) match {
      case Left(parsingFailure) => Json.Null
      case Right(j) => j
    }
  }
  val userInputDF: Option[Map[String,Properties]] = {
    this.userInput.asObject match {
      case None => None //TODO: Replace None with a a default data format schema
      case Some(obj) =>
        val df: Option[JsonObject] = obj.apply("DATA_FORMAT").get.asObject //check this get
        val newDF: Map[String,Properties] = df.get.toMap.map{case (k,v) =>
          val properties = v.as[Properties].right.get //check this get, risky?
          k -> properties
        }
        Some(newDF)
    }
  }
  val userInputRPF: Option[JsonNumber] = {
    this.userInput.asObject match {
      case None => None
      case Some(obj) => obj.apply("ROWS_PER_FILE").get.asNumber //check this get
    }
  }

  val userInputCS: Option[JsonNumber] = {
    this.userInput.asObject match {
      case None => None
      case Some(obj) => obj.apply("CLASS_SIZE").get.asNumber //check this get
    }
  }

  val userInputBD: Option[JsonObject] = {
    this.userInput.asObject match {
      case None => None
      case Some(obj) => obj.apply("BREAKDOWN_MAP").get.asObject //check this get
    }
  }

  /**
    * getKeyName uses the input, ap ( action property), to get the name of the key( with the given ap) in
    * the DATA_FORMAT object defined in user-input.json
    * @param ap - String ( action property)
    * @return - name of the key that has ap as its action property value.
    */
  def getKeyName(ap: String) : String = {
    val df = userInputDF.get
    val vka: Vector[KeyAndAction] = df.map{case (k,p) => KeyAndAction(k,p.action)}.filter(ka =>
      ka.action.contains(ap)).toVector
    if(vka.size > 1){
      logger.warn("Entree detected that the Data Format schema in user-input.json contains multiple" +
        "keys with the action: value. The first key will be used in the filtering process.")
    }
    vka.head.key
  }

}

object AppConfig {
  // HOCON Config for tokens etc..
  val mode: String = Mode.test
  val conf: Config = ConfigFactory.load().getConfig(mode)
  val S3ClientID: String = conf.getString("aws.s3.clientId")
  val S3ClientSecret: String = conf.getString("aws.s3.clientSecret")
}

object Mode {
  val test = "test"
  val dev = "dev"
}
