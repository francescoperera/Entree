import java.io.{BufferedReader, File, FileReader}

import com.typesafe.config.{Config, ConfigFactory}
import io.circe.{Json, parser}

trait ConfigReader {
  val conf: Config = ConfigFactory.load()
  val userInput : Json = {
    val reader = new BufferedReader(new FileReader("src/config/user-input.json"))
    val ui: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
    reader.close()
    parser.parse(ui) match {
      case Left(parsingFailure) => Json.Null
      case Right(j) => j
    }



  }
//  val rpf = conf.getInt("local.ROWS_PER_FILE")
//  val dfSchema = conf.getObject("local.DATA_FORMAT").unwrapped().asInstanceOf[java.util.Map[String,String]]
}
