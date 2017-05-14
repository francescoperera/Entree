
object CFNMappingCook {
  /** CFNMappingCook has the only responsibility of storing the column field map and is the only resource in Entree
    * that can interact with it.
    * */

  val cfnMap:Map[String,Vector[String]] = Map(
    "email_address" -> Vector("email_address","email","emailaddress","emails"),
    "full_name" -> Vector("full_name","fullname","name","employee"),
//    "name"  -> Vector("first_name","firstname","last_name",
//      "lastname","surname","family_name","middle_name","middlename","middle","modifier"),
    "first_name" -> Vector("first_name","firstname"),
    "last_name" -> Vector("last_name","lastname","surname","family_name"),
    "middle_name" -> Vector("middle_name","middlename","middle"),
    "name_modifier" -> Vector("modifier"),
    "address" -> Vector("address","physical_address","physicaladdress","employer_address",
      "work_address","waddress","street_address","street_name","street"),
    "phone_number" -> Vector("phone_number","phonenumber","phone"),
    "username" -> Vector("username","user_name"),
    "education"-> Vector("college","university","school"),
    "employee_id" -> Vector("employee_id"),
    "uuid" -> Vector("uuid"),
//    "job" -> Vector("job","occupation"),
    "job_title" -> Vector("job_title","jobtitle","occupation_title","jobTitle","job","occupation"),
    "salary" -> Vector("salary"),
//    "employer" -> Array("employer"),
    "company_name" -> Vector("companyName","employer","organization"),
    "credit_card_number" -> Vector("credit_card_number"),
    "date" -> Vector("date"),
    "ip_number" -> Vector("ip_number"),
    "vehicle_plate" -> Vector("vehicle_plate"),
    "all" -> Vector()
  )


  val rcfnsMap: Map[String,String] = cfnMap.flatMap{
    case (label, cfnArray ) => cfnArray.map{
      case  cfn => cfn -> label
    }
  }

  /**
    * Given a certain string or value, it finds the key in cfnMap that maps to that string/value
    * @param v - string of value
    * @return - key that maps to the input value
    */
  def getKeyFromVal(v:String):String = cfnMap.find(_._2.contains(v)).get._1 //TODO: make this safe, make this Option

  /**
    * Checks whether a value,v, is present in cfnMap with a specific key,k
    * @param k - key/ string
    * @param v - value/string
    * @return - boolean
    */
  def isValPresentWithKey(k:String,v:String):Boolean = cfnMap(k).contains(v)

  /**
    * Checks whether the input string, str, exists as a value in cfnMap.
    * @param str - value/string
    * @return - boolean
    */
  def isLabelWithKeyPresent(str:String):Boolean = cfnMap.values.exists(_.contains(str))
}
