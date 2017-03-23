import java.io.{BufferedReader, FileReader}

object UnknownCook {

  def scrambleString(s : String) : String = util.Random.shuffle(s.toList).mkString("")

  def randomTrigrams(s:String) : String = s.length match {
      case 1 | 2 | 3 => scrambleString(s)
      case _ =>
        val trigrams = s.grouped(3).toList
        util.Random.shuffle(trigrams).mkString("")
    }

  def wordSampler():String = {
    val stopWordsFile = "stop_words"
    val start = 1
    val end = 5
    val reader = new BufferedReader(new FileReader(stopWordsFile))
    val words = Stream.continually(reader.readLine()).takeWhile( _ != null).toVector
    val n = start + scala.util.Random.nextInt( (end - start) + 1)
    scala.util.Random.shuffle(words).take(n).mkString("")

  }

}
