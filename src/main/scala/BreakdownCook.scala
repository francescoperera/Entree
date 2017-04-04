
object BreakdownCook {

  //TODO: Create another config file where
  val bdMap:Map[String,Array[String]] = Map (
    "full_name" -> Array("first_name","middle_name","last_name","name_modifier"),
    "address" -> Array("house_number","street address","apartment_number","city",
      "state","zip_code")
  )

  def isKeyPresent(str:String): Boolean = bdMap.keySet.contains(str)

  def getCompositeFields(k:String):Vector[String] = bdMap.getOrElse(k,Array[String]()).toVector

}
