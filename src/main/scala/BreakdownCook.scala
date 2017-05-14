import io.circe.Json

object BreakdownCook extends ConfigReader {

  //TODO: Create another config file where
  val defaultBDMap: Map[String, Vector[String]] = Map (
    "full_name" -> Vector("first_name","middle_name","last_name","name_modifier"),
    "address" -> Vector("house_number","street address","apartment_number","city",
      "state","zip_code")
  )

  val bdMap: Map[String, Vector[String]] = userInputBD match {
    case None => defaultBDMap
    case Some(bd) =>
      val m: Map[String, Json] = bd.toMap
      m.map{case (k,v) =>
        val jsonVec: Vector[Json] = v.asArray.get //TODO: think about this get and make it more safe
        val bdVec: Vector[String] = jsonVec.flatMap(_.asString) //flatMap maps JSON -> Option[String] -> String
        k -> bdVec
      }
  }

  val rbdMap: Map[String,String] = bdMap.flatMap{
    case (bdk, bdVec) =>  bdVec.map(bdField => bdField -> bdk)
  }


  def isKeyPresent(str:String): Boolean = bdMap.keySet.contains(str)

  def getCompositeFields(k:String):Vector[String] = bdMap.getOrElse(k,Vector[String]())

  def getSubLabelList(label:String): Vector[String] = bdMap.getOrElse(label,Vector.empty[String])
}

object HierarchicalLabel {
  val fullName = "full_name"
  val address = "address"
}
