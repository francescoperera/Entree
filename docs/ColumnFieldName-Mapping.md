 ## Column Field Name - Mapping
 Data aggregation relies on the column field name Map being up to date. Thus, if Entree complains that it cannot
 find a label, it might be becausethat label is not present in the the column field name Map ( look at CFNMappingCook).

 This is the current Map:

 ```
val cfnMap:Map[String,Vector[String]] = Map(
    "email_address" -> Vector("email_address","email","emailaddress","emails"),
    "full_name" -> Vector("full_name","fullname","name","employee"),
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
    "job_title" -> Vector("job_title","jobtitle","occupation_title","jobTitle","job","occupation"),
    "salary" -> Vector("salary"),
    "company_name" -> Vector("companyName","employer","organization"),
    "credit_card_number" -> Vector("credit_card_number"),
    "date" -> Vector("date"),
    "ip_number" -> Vector("ip_number"),
    "vehicle_plate" -> Vector("vehicle_plate"),
    "all" -> Vector()
  )
```
The keys in cfNMap are the labels known to Entree and the values contain all the column names that Entree will tag
to the corresponding label.