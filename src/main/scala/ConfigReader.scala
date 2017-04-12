import java.io.{BufferedReader, File, FileReader}

import com.typesafe.config.{Config, ConfigFactory}
import io.circe._

case class Properties(action:String,_type:String,components:Option[Vector[Map[String,Properties]]])

object Properties{
  implicit val encoder: Encoder[Properties] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[Properties] = io.circe.generic.semiauto.deriveDecoder
}

trait ConfigReader {
  val conf: Config = ConfigFactory.load()

  val userInput: Json = {
    val reader = new BufferedReader(new FileReader("src/config/user-input.json"))
    val ui: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
    reader.close()
    parser.parse(ui) match {
      case Left(parsingFailure) => Json.Null
      case Right(j) => j
    }
  }
  val userInputDF: Option[JsonObject] = {
    this.userInput.asObject match {
      case None => None //TODO: Replace None with a a default data format schema
      case Some(obj) => obj.apply("DATA_FORMAT").get.asObject //check this get
    }
  }
  val userInputRPF: Option[JsonNumber] = {
    this.userInput.asObject match {
      case None => None
      case Some(obj) => obj.apply("ROWS_PER_FILE").get.asNumber //check this get
    }
  }

  val userInputBD: Option[JsonObject] = {
    this.userInput.asObject match {
      case None => None
      case Some(obj) =>
        val ui: Option[JsonObject] = obj.apply("BREAKDOWN_MAP").get.asObject //check this get
        val newUI = ui.get.toMap.map{case (k,v) => k ->  }
        ui
    }
  }
}
