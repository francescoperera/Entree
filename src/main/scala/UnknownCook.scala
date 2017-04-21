import java.io.{BufferedReader, FileReader}

import HeadChef.{getKeyValuePair}
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._

object UnknownCook extends LazyLogging with ConfigReader {

  val generators : List[ String => String ]= List(scrambleString _,randomTrigrams _,wordSampler _)

  def scrambleString(s : String) : String = util.Random.shuffle(s.toList).mkString("")

  def randomTrigrams(s:String) : String = s.length match {
      case 0| 1 | 2 | 3 => scrambleString(s)
      case _ =>
        val trigrams = s.grouped(3).toList
        util.Random.shuffle(trigrams).mkString("")
    }

  def wordSampler(s:String):String = { //TODO:useless input,fix
    val stopWordsFile = "stop_words"
    val start = 1
    val end = 5
    val reader = new BufferedReader(new FileReader(stopWordsFile))
    val words = Stream.continually(reader.readLine()).takeWhile( _ != null).toVector
    reader.close()
    val n = start + scala.util.Random.nextInt( (end - start) + 1)
    scala.util.Random.shuffle(words).take(n).mkString("")

  }

  def createUnknowns(dfv: Vector[JsonObject]): Vector[JsonObject] = {
    val unknownsVector: Vector[Option[JsonObject]] = dfv.map {df =>
      val unknownLabel: String = "unknown" // the label and column values for unknowns is the same
    val colDesc: String = "" //unknowns have empty column description.
    val unknown: Option[JsonObject] = userInputDF match {
      case None =>
        logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
        None
      case Some(ui) =>
        val unKnownMap: Map[String, Json] = ui.map{case (k,p) =>
          val fn: String => String = util.Random.shuffle(UnknownCook.generators).head
          val dkn : String = getKeyName(Actions.value) //dkn = data key name
        val unKnownVal: String = fn(df.apply(dkn).getOrElse(Json.Null).asString.get)
          getKeyValuePair(p,k,Some(unKnownVal),Some(unknownLabel),colDesc,None,Some(unknownLabel),None)
        }
        unKnownMap.asJson.asObject
    }
      unknown
    }
    unknownsVector.flatten
  }


}
