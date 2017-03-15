
import com.typesafe.scalalogging.LazyLogging
import io.circe.{Json, parser}

import scala.util.{Failure, Success}

trait JsonConverter extends LazyLogging {
  /**
    * JsonConverter is an interface that resources in Entree will use to go from and to Json. It will use Circe
    */

  def toJson(str: String): Option[Json] = {
    parser.parse(str) match {
      case Left(failure) => None
      case Right(json) => Some(json)
    }
  }
}
