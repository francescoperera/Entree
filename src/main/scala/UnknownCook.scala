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

  def wordSampler(s:String): String = { //TODO:useless input,fix
    val stopWordsFile = "stop_words"
    val start = 1
    val end = 5
    val reader = new BufferedReader(new FileReader(stopWordsFile))
    val words = Stream.continually(reader.readLine()).takeWhile( _ != null).toVector
    reader.close()
    val n = start + scala.util.Random.nextInt( (end - start) + 1)
    scala.util.Random.shuffle(words).take(n).mkString("")

  }

}
