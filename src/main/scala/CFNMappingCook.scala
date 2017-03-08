/**
  * Created by francescoperera on 3/8/17.
  */
object CFNMappingCook {

  private val cfnMap:Map[String,Array[String]] = Map ( //TODO: Fucking manual work. Automate this ASAP
    "email_address" -> Array("email_address","email","emailaddress"),
    "full_name" -> Array("full_name"),
    "first_name" -> Array("first_name","firstname"),
    "last_name" -> Array("last_name","lastname","surname","family_name"),
    "name" -> Array ("name"),
    "middle_name" -> Array("middle_name","middlename"),
    "address" -> Array("address","physical_address","physicaladdress","employer_address","work_address"),
    "phone_number" -> Array("phone_number","phonenumber","phone"),
    "username" -> Array("username","user_name"),
    "education"-> Array("college","university","school"),
    "employee" -> Array("employee"),
    "employee_id" -> Array("employee_id"),
    "uuid" -> Array("uuid"),
    "job_title" -> Array("job_title","jobtitle","occupation","occupation_title"), //TODO: double check  difference between occupation and job title
    "salary" -> Array("salary"),
    "employer" -> Array("employer")
  )

  def getKeyFromVal(v:String):String = cfnMap.find(_._2.contains(v)).get._1

  def isValPresent(k:String,v:String):Boolean = cfnMap(k).contains(v)




}
