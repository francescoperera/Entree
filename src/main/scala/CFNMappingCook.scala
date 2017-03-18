
object CFNMappingCook {
  /** CFNMappingCook has the only responsibility of storing the column field map and is the only resource in Entree
    * that can interact with it.
    * */

  val cfnMap:Map[String,Array[String]] = Map ( //TODO: Fucking manual work. Automate this ASAP
    "email_address" -> Array("email_address","email","emailaddress"),
    "full_name" -> Array("full_name","fullname","name","employee"),
    "first_name" -> Array("first_name","firstname"),
    "last_name" -> Array("last_name","lastname","surname","family_name"),
    "middle_name" -> Array("middle_name","middlename","middle"),
    "name_modifier" -> Array("modifier"),
    "address" -> Array("address","physical_address","physicaladdress","employer_address","work_address"),
    "phone_number" -> Array("phone_number","phonenumber","phone"),
    "username" -> Array("username","user_name"),
    "education"-> Array("college","university","school"),
    "employee_id" -> Array("employee_id"),
    "uuid" -> Array("uuid"),
    "job_title" -> Array("job_title","jobtitle","occupation","occupation_title","jobTitle"), //TODO: double check  difference between occupation and job title
    "salary" -> Array("salary"),
    "employer" -> Array("employer"),
    "company_name" -> Array("companyName"),
    "all" -> Array()
  )

  /**
    * Given a certain string or value, it finds the key in cfnMap that maps to that string/value
    *
    * @param v - string of value
    * @return - key that maps to the input value
    */
  def getKeyFromVal(v:String):String = cfnMap.find(_._2.contains(v)).get._1

  /**
    * Checks whether a value,v, is present in cfnMap with a specific key,k
    *
    * @param k - key/ string
    * @param v - value/string
    * @return - boolean
    */
  def isValPresentWithKey(k:String,v:String):Boolean = cfnMap(k).contains(v)

  /**
    * Checks whether the input string, str, exists as a value in cfnMap.
    *
    * @param str - value/string
    * @return - boolean
    */
  def isValPresent(str:String):Boolean = cfnMap.values.exists(_.contains(str))
}
