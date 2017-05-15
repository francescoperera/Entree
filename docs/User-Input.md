## User - Input

Entree is a user driven application that requires the user to specify a number of parameters. The input parameters
should be specified in **user-input.json**.

These are the following input parameters:
 - ROWS_PER_FILE
 - CLASS_SIZE
 - DATA_FORMAT
 - BREAKDOWN_MAP

 Note that if one or more of these input parameters are not defined in **user-input.json**, Entree will use
 default values.

### ROWS_PER_FILE
This parameter defines the number of aggregated and labeled data objects should be saved per file.
For example, if the associated value is 100 and the number of data objects is 1000, then 10 files will be created
and saved to S3. Each file will contain 100 data objects.
If no value is specified, then Entree will use a default value of **100000**.

### CLASS_SIZE
This parameter value is used to define a balance across label distribution. This value ensures that all labels
have a similar number of data points. If a certain label has less data points than the value defined for CLASS_SIZE,
than Entree will just get all of the data points that are tagged with that label.
If no value is specified, then Entree will use a default value of **10000**.

### DATA_FORMAT
This parameter is used to define the structure and format of the data objects used to label and aggregate data.
More information can be found in [Data_Format](./Data_Format.md)

If no DATA_FORMAT object is defined, Entree will use the following default:

```
val DF: Map[String,Properties] = Map (
    "data" -> Properties("value","String",None),
    "label" -> Properties("label","String",None),
    "column_header" -> Properties("column","String",None),
    "column_description" -> Properties("description","String",None),
    "tokenized" -> Properties("breakdown","Map[String,String]",None)
  )
```

### BREAKDOWN_MAP
This parameter defines an object of where the keys are the labels that can be decomposed into smaller subfields. The
values in the object are arrays of the subfields that make up the labels.

If no BREAKDOWN_MAP is defined, Entree will use the following map:
```
val BDMap: Map[String, Vector[String]] = Map (
    "full_name" -> Vector("first_name","middle_name","last_name","name_modifier"),
    "address" -> Vector("house_number","street address","apartment_number","city",
      "state","zip_code")
  )
```


### Example
```
{
  "ROWS_PER_FILE" : 100000,
  "CLASS_SIZE":10000,
  "DATA_FORMAT" : {
    "data":{
      "action":"value",
      "_type":"String"
    },
    "label":{
      "action":"label",
      "_type":"String"
    },
    "column_header":{
      "action":"column",
      "_type":"String"
    },
    "column_description":{
      "action":"description",
      "_type":"String"
    },
    "breakdown": {
      "action": "decomposition",
      "_type": "Vector[Map[String,String]",
      "breakdown_schema" :
        {
          "field":{
            "action":"sub_label",
            "_type":"String"
          },
          "data":{
            "action":"empty_value",
            "_type":"String"
          }
        }
    }
  },
  "BREAKDOWN_MAP": {
    "full_name": ["first_name","middle_name","last_name","name_modifier"],
    "address": ["house_number","street address","apartment_number","city",
      "state","zip_code"]
  }
}
```
