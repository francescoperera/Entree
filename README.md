# Entree
Entree is a a data labeling and aggregation tool. It streams data from different files, data points are labeled and
grouped together according to the assigned label.
Individual data points are transformed into NDJSON format where the object parameters are defined by the user in
user-input.json.
Entree is able to read in files that are either in JSON or NDJSON format.
Currently Entree works as a command line tool (clt), where the user defines the inputs a json
config file (user-input.json). For more information about this config file, check [User-Input](./docs/User-Input.md)

## Why?
Data is messy and is defined in an inconsistent manner. Depending on the source, data that refer to the same concept
(i.e email address, phone numbers, address etc..) can be stored with keys or column names that can be written in
a number of ways.
For example, one dataset could use the column name "email_address" to store emails, while a different dataset
could store the same information using the column name "email".
The only way you can define that those column from both datasets are referring to the same "concept" is if they
are tagged to the same label.
Entree contains this ability to look at the data point and identify a label.

Whether you are training machine learning models or doing some analysis on Spark, you need to define the format
for your data. Sometimes, you just want the data object to have a single key value pair of column name and data value.
Other times, you want the object to contain more contextual information, like column description or a decomposition
of data if it is made of composite fields (i.e name has first name, middle name and last name).
Entree takes your input on how to structure the data format object ( what parameters / fields it should contain) and
stores the data accordingly.


## How?
Data labeling is done by maintaining a column field name map, which delineates the different ways that a certain
label can be written as.
The keys in the map are the labels. The values refer to lists of column names that refer to the labels.

For example
```
...
"phone_number" : ["phone_number","phonenumber","phone"],
...
```
shows that any data point whose key or column name is _phone_number_ , _phonenumber_ and _phone_ will all
be labeled as *phone_number*. For more information check [ColumnFieldName-Mapping](./docs/ColumnFieldName-Mapping.md)

### Label
The following is a list of labels:
 - email_address
 - full_name
 - first_name
 - last_name
 - name
 - middle_name
 - address
 - phone_number
 - username
 - education
 - employee
 - employee_id
 - uuid
 - job_title
 - salary
 - employer
 - all


Entree aggregates and stores data into a defined object by looking at how the user defined the DATA_FORMAT object
in user-input.json. Refer to [Data_Format](./docs/Data_Format.md) and  for more information.


## Required
### sbt
download sbt from here http://www.scala-sbt.org/

## Input
This clt requires three inputs:
  1. s3 url pointing to bucket & folder path for source data (i.e https://s3.amazonaws.com/my_bucket/folderA/folderB)
  2. s3 url pointing to the bucket & folder path that you want to save the formatted data to.(i.e https://s3.amazonaws.com/my_bucket/folder1/folder2)
  3. label or column field name that you want to format data for. If you want to label all column field names in your bucket use "all",
  else type the column field name ( i.e "email_address"). Entree will only accept labels that are stored as values in cfnMap.


## Output
JSON data is aggregated into one or more files in NDJSON format, where each object has a series of key value pairs.
The files are then saved back to S3.

```
        {"data":"foo","label":"bar","originalLabel":"baz"}
```

## Getting started

### Compile
```sbt compile```

### Run
```sbt "run {s3_url_source} {s3_url_destination} label" ```

## Example
For an example, check the [docs](./docs/Entree-Example.md)






