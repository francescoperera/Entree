# Entree
Entree is a tool that enables you to aggregate all  JSON files present in an S3 bucket
into a one or more files with a standard data format. The data format is set to be a JSON object with three keys: data and label and originalLabel.
The key data maps to the actual value stored in an  newline delimited JSON (ndjson) object. label maps to the the greater ontology/column field name of the
the key used in the original JSON object. Lastly, originalLabel points to key used in the original JSON object.
Currently Entree works as a command line tool (clt).

## Required
### sbt
download sbt from here http://www.scala-sbt.org/

## Input
This clt requires three inputs:
  1. s3 url pointing to bucket & folder path for source data (i.e https://s3.amazonaws.com/my_bucket/folderA/folderB)
  2. s3 url pointing to the bucket & folder path that you want to save the formatted data to.(i.e https://s3.amazonaws.com/my_bucket/folder1/folder2)
  3. label or column field name that you want to format data for. If you want to label all column field names in your bucket use "all", else type the column field name ( i.e "email_address")

## Getting started

### Compile
```sbt compile```

### Run
```sbt "run s3_url_source s3_url_destination label" ```

## Example

In your s3 bucket, named "dummy_bucket", you have a folder,named "data",where you have stored all of your JSON files.

https://s3.amazonaws.com/dummy_bucket/data:
  -  email.json
  -  email_address.json
  -  emailaddress.json

Here is a preview of the data in each of these files:

#### email.json
```
{"email":"hzk@yahoo.com"}
{"email":"trevorp@hotmail.com"}
....
```

#### email_address.json
```
{"email_address":"xyz@gmail.com"}
{"email_address":"bot@gmail.com"}
....
```

#### emailaddress.json
```
{"emailaddress":"ark@byu.edu"}
{"emailaddress":"lol@aol.com"}
....
```

If you run ``` sbt "run https://s3.amazonaws.com/dummy_bucket/data https://s3.amazonaws.com/dummy_bucket/clean_data email_address" ```, all of the files under the label/ontology of email_address, such as : email_adddress.json, email.json and emailaddress.json will be aggregated into one or more files with a standard data format.

The new file will look like the following:

```
{"data":"hzk@yahoo.com","label":"email"}
{"data":"trevorp@hotmail.com","label":"email"}
{"data":"xyz@gmail.com","label":"email_address"}
{"data":"bot@gmail.com","label":"email_address"}
{"data":"ark@byu.edu","label":"emailaddress"}
{"data":"lol@aol.com","label":"emailaddress"}
```
This file will be stored in https://s3.amazonaws.com/dummy_bucket/clean_data

## Important:
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
   "middle_name" -> Array("middle_name","middlename","middle"),
   "name_modifier" -> Array("modifier"),
   "address" -> Array("address","physical_address","physicaladdress","employer_address","work_address"),
   "phone_number" -> Array("phone_number","phonenumber","phone"),
   "username" -> Array("username","user_name"),
   "education"-> Array("college","university","school"),
   "employee" -> Array("employee"),
   "employee_id" -> Array("employee_id"),
   "uuid" -> Array("uuid"),
   "job_title" -> Array("job_title","jobtitle","occupation","occupation_title","jobTitle"), //TODO: double check  difference between occupation and job title
   "salary" -> Array("salary"),
   "employer" -> Array("employer"),
   "companyName" -> Array("companyName"),
   "all" -> Array()
   )
```
 Map was last updated 03/14/2017


