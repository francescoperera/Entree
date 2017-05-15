import java.io.{BufferedReader, File, FileReader}

import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._


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
  val userInputDF: Map[String,Properties] = {
    this.userInput.asObject match {
      case None =>
        logger.error("A Data Format object was not specified in user-input.json. Entree will use a default one.")
        Defaults.DF
      case Some(obj) =>
        val dataFormat: Map[String, Properties] = obj.apply("DATA_FORMAT") match {
          case None => Defaults.DF
          case Some(df) =>
            val newDF: Map[String, Properties] = df.asObject match {
              case None => Defaults.DF
              case Some(dfObject) =>
                dfObject.toMap.map { case (k, v) =>
                  val properties = v.as[Properties].right.get //check this get, risky?
                  k -> properties
                }
            }
            newDF
        }
        dataFormat
    }
  }
  val userInputRPF: Int = {
    this.userInput.asObject match {
      case None => Defaults.RPF
      case Some(obj) => obj.apply("ROWS_PER_FILE").getOrElse(Defaults.RPF.asJson).asNumber.get.toInt.get
    }
  }

  val userInputCS: Int = {
    this.userInput.asObject match {
      case None => Defaults.CS
      case Some(obj) => obj.apply("CLASS_SIZE").getOrElse(Defaults.CS.asJson).asNumber.get.toInt.get
    }
  }

  val userInputBD: Option[JsonObject] = {
    this.userInput.asObject match {
      case None => Defaults.BDMap.asJson.asObject
      case Some(obj) => obj.apply("BREAKDOWN_MAP").getOrElse(Defaults.BDMap.asJson).asObject
    }
  }

  /**
    * getKeyName uses the input, ap ( action property), to get the name of the key( with the given ap) in
    * the DATA_FORMAT object defined in user-input.json
    * @param ap - String ( action property)
    * @return - name of the key that has ap as its action property value.
    */
  def getKeyName(ap: String) : String = {
    val vka: Vector[KeyAndAction] = this.userInputDF.map{case (k,p) => KeyAndAction(k,p.action)}
      .filter(ka => ka.action.contains(ap)).toVector
    if(vka.size > 1){
      logger.warn("Entree detected that the Data Format schema in user-input.json contains multiple" +
        "keys with the action: value. The first key will be used in the filtering process.")
    }
    vka.head.key
  }
}

object Defaults {
  val RPF: Int = 100000
  val CS: Int = 10000
  val BDMap: Map[String, Vector[String]] = Map (
    "full_name" -> Vector("first_name","middle_name","last_name","name_modifier"),
    "address" -> Vector("house_number","street address","apartment_number","city",
      "state","zip_code")
  )
  val DF: Map[String,Properties] = Map (
    "data" -> Properties("value","String",None),
    "label" -> Properties("label","String",None),
    "column_header" -> Properties("column","String",None),
    "column_description" -> Properties("description","String",None),
    "tokenized" -> Properties("breakdown","Map[String,String]",None)
  )
}


