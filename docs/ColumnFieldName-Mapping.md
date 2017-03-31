 ## Column Field Name - Mapping
 Data aggregation relies on the column field name Map being up to date. Thus, if Entree complains that it cannot find a label, it might be because
 that label is not present in the the column field name Map ( look at CFNMappingCook).

 This is the current Map:

 ```
 val cfnMap:Map[String,Array[String]] = Map (
     "email_address" -> Array("email_address","email","emailaddress"),
     "full_name" -> Array("full_name","fullname"),
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
     "job_title" -> Array("job_title","jobtitle","occupation","occupation_title","jobTitle"),
     "salary" -> Array("salary"),
     "employer" -> Array("employer","companyName"),
     "all" -> Array()
   )
```
