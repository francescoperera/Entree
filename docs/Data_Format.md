## Data Format Object

DATA_FORMAT is an object that users will use to define the structure and the content of the object
that Entree will use to label and aggregate data. Users can define this object in **user-input.json**.
The DATA_FORMAT object should handle and store the data and its contextual information. Most key-value pairs in the
object should be first level entries( not nested).

The base unit of this object is a key-value pair. In order to build this pair, the user needs to
provide the name of the key and a set of properties. See the Structure section below for more information


### Breakdown
If a "breakdown" of the data into its composite fields should be needed, it can be added to the DATA_FORMAT object and
it should be the only nested parameter in the object. An example of a breakdown is :


address | house_number | street_address | apartment_number | state | city | zip_code |
-------- | ------------ | -------------- | ---------------- | ----- | ---- | -------- |
123 Washington Avenue, Apt 3B, NY , NY 10000 | 123 | Washington Avenue | Apt 3B | NY | NY | 10000 |

Note that a breakdown occurs only if the label detected by Entree for the datapoint is made of composite fields.
In this case Entree looked at datapoint ```123 Washington Avenue, Apt 3B, NY , NY 10000``` labeled it as **address**
and recognized that address is a label made of composite fields : house_number, street_address, apartment_number,
state, city and zip_code.
Users can define which labels if any have composite fields  by adding a Map to **user-input.conf**.

An example of a user defined Map for relating labels with its components is :

```
....
"BREAKDOWN_MAP": {
    "full_name": ["first_name","middle_name","last_name","name_modifier"],
    "address": ["house_number","street address","apartment_number","city",
      "state","zip_code"]
  }
....
```

### Structure
The object should be composed of n key - value pairs, where n is defined by the user. Each key is defined as an object
parameter and has a set of properties : action,type and components. All keys should have an action and type. Keys have
a components property **only** if its action property is set to be decomposition.

##### Important
Each key structure in the JSON configuration file must follow the structure delineated in this doc.
Specifically, all keys **must** have the properties: action and type. Moreoever, the action property
must be set to be one of the actions that Entree supports ( see below for Actions).

##### Example of Key Structure

```
    "key_name":{
        "action": "value"
        "type": "String"
    }

```
In this example key was named *key_name* and its properties were set, such that *key_name*'s
action property is set to be "value" and the its type is set as String.

##### Special Example of Key Structure with Components Property.

```
    "another_key_name":{
        "action": "decomposition"
        "type": "Vector[Map[String,String]]"
        "components:[
            {
              "field":{
                "action":"sub_label",
                "type":"String"
              },
              "data":{
                "action":"empty_value",
                "type":"String"
              }
            }
        ]
    }

```
In this example, the key was named *another_key_name* and it was given 3 properties. Since the action
property was set to be "decomposition", then Entree expects to also be given the "components" property.
Check the Action and Components sections for more information on decomposition and  components.

#### Action
Action is a property that relates to the action that Entree will need to do to get the value for its key. E
ntree has a number of defined actions :

##### value
Entree grabs the value stored in the data point.

```{"first_name":"Frank"} ```

"Frank" is the value of the data point. So the key whose action property is *value* will map to value "Frank".
**Important**: Only key per object can have an action property set to "value".

##### label
Entree gets the label for the key in the data point

```{"first_name":"Frank"}```

Entree grabs the key, in this case "first_name", and looks for its associated label. The label for "first_name"
is "full_name". Thus the key whose action property is *label* will map to the value "full_name".

##### column
Entree gets the key used in the original data point

```{"first_name":"Frank"}```

"first_name" is the key in this data points. The key whose action property is *column* will map to value "first_name".

##### description
Entree gets the column description in the data point.
If no column description is found, then column description is a set as an empty String.

```{"first_name":"Frank","column_description":"Some random person"}```

Entree grabs the value under "column_description",in this case "Some random person". The key whose action property
is *description* will map to the value "Some random person".

#### decomposition**
Entree looks at the value stored under the property "components" and uses it to create the schema for the
breakdown data. Any key whose action is set to "decomposition", will also have components mapped to an non empty array.
For more information, check the Breakdown and Components section.

#### sub_label
Entree looks at the label for the data point and then checks if the label is made of composite field.
This done by looking at at "breakdown" Map, which can be either user defined or default one.
If label has composite fields, then these fields are stored under the key whose action property is set as "sub_label".

###empty_value
Entree performs no action and maps the corresponding key to an empty string.


#### Type
Type is a property that defines the type to be expected for the key. Type may be String,Int,Vector[String], etc...

#### Components
Components is a property that defines how to structure the breakdown of a value. Components typically map to empty arrays except when
the action is set to be *decomposition*. When a key has this action, then  the property components maps to an Array of 1 object. The object stores
the key ( and its properties) that will be used to describe the breakdown.
For example, if the DATA_FORMAT object had the following key defined :
```
    ......
    ......
    "division": {
      "action": "decomposition",
      "type": "Vector[Map[String,String]",
      "components" : [
        {
          "composite_field":{
            "action":"sub_label",
            "type":"String"
          },
          "data":{
            "action":"no_value",
            "type":"String"
          }
        }
      ]
    }
    .....
    .....
```

The following data point :  ``` 123 Washington Avenue, Apt 3B, NY , NY 10000``` is one made of composite fields( as mentioned above),
its label is address and address is made up of : house_number, street_address, apartment_number, state, city and zip_code.
The  "breakdown"  portion for the DATA_FORMAT object ( in this case the "breakdown" key is called "division") for this data is :

```   ......
       ......
       "division" : [
          {
            "composite_field" : "house_number",
            "data" : ""
          },
          {
            "composite_field" : "street address",
            "data" : ""
          },
          {
            "composite_field" : "apartment_number",
            "data" : ""
          },
          {
            "composite_field" : "city",
            "data" : ""
          },
          {
            "composite_field" : "state",
            "data" : ""
          },
          {
            "composite_field" : "zip_code",
            "data" : ""
          }
       ]
       .....
       .....
```

## Example

### user-input.json

```
{
  "ROWS_PER_FILE" : 100000,
  "DATA_FORMAT" : {
    "data":{
      "action":"value",
      "type":"String"
    },
    "label":{
      "action":"label",
      "type":"String"
    },
    "column_header":{
      "action":"column",
      "type":"String"
    },
    "column_description":{
      "action":"description",
      "type":"String"
    },
    "breakdown": {
      "action": "decomposition",
      "type": "Vector[Map[String,String]",
      "components" : [
        {
          "field":{
            "action":"sub_label",
            "type":"String"
          },
          "data":{
            "action":"no_value",
            "type":"String"
          }
        }
      ]
    }
  },
  "BREAKDOWN_MAP": {
    "full_name": ["first_name","middle_name","last_name","name_modifier"],
    "address": ["house_number","street address","apartment_number","city",
      "state","zip_code"]
  }
}
```
### Breakdown Map

```
"BREAKDOWN_MAP": {
    "full_name": ["first_name","middle_name","last_name","name_modifier"],
    "address": ["house_number","street address","apartment_number","city",
      "state","zip_code"]
  }
```

### Data Points
```
....
{"companyName": "Datalogue","jobTitle":"Software Engineer"}
{"name": "Samuel Donovan"}
{"first_name": "Eric", "column_description": "first name of employee"}
....
```

### Entree workflow & process

1) Entree reads a datapoint
2) Extracts all keys that have a label and grabs their corresponding values.
3) If data point has column description, it also grabs the value stored in this key
4) Entree reads the DATA_FORMAT schema defined in **user-input.conf** and builds a Map, which is then casted into JSON.
5) Steps 1-4 are repeated for all datapoints
6) The JSON representation of the DATA_FORMAT objects are then saved.

### Data Point Example 1:
``` {"companyName": "Datalogue","jobTitle":"Software Engineer"} ```

Entree detects that this data point has two keys that have labels. "companyName" has a label "company_name"
and "jobTitle" has "job_title". Since two labels were detected, Entree will create two seperate DATA_FORMAT object
for each label.

**(JSON) Object for company_name**

- "data" -> "Datalogue" (value stored in data point)
- "label" ->  "company_name" (label detected for value)
- "column_header" -> "companyName" (key used to store the value in the data point)
- "column_description" -> "" (empty string because data point had no column_description")
- "breakdown" -> [] ( empty array because the label is not one of the keys in the breakdownMap and thus has no composite fields)

*Note that the following DATA_FORMAT object will be shown in JSON*.

```
{
    "breakdown":[],
    "column_header":"companyName",
    "data": "Datalogue",
    "label":"company_name",
    "column_description":""
}
```


**(JSON) Object for job_title**

- "data" -> "Software Engineer" (value stored in data point)
- "label" ->  "job_title" (label detected for value)
- "column_header" -> "jobTitle" (key used to store the value in the data point)
- "column_description" -> "" (empty string because data point had no column_description")
- "breakdown" -> [] ( empty array because the label is not one of the keys in the breakdownMap and thus has no composite fields)

*Note that the following DATA_FORMAT object will be shown in JSON*.

```
{
    "breakdown":[],
    "column_header":"jobTitle",
    "data": "Software Engineer",
    "label":"job_title",
    "column_description":""
}
```


### Data Point Example 2:
``` {"name": "Samuel Donovan"} ```

Entree detects that this data point has one key that has a label. The key is "name" and its label is "full_name".
Entree will create one  DATA_FORMAT object for this data point.

**(JSON) Object for full_name**

- "data" -> "Samuel Donovan" (value stored in data point)
- "label" ->  "full_name" (label detected for value)
- "column_header" -> "name" (key used to store the value in the data point)
- "column_description" -> "" (empty string because data point had no column_description")
- "breakdown" -> [
        {
            "field":"first_name",
            "data":""
        },
        {
            "field":"middle_name",
            "data":""
        },
        {
            "field":"last_name",
            "data":""
        },
        {
            "field":"name_modifier",
            "data":""
        }
    ](full_name was detected as a label with composite fields ,check breakdownMap in the Breakdown Map section.)

*Note that the following DATA_FORMAT object will be shown in JSON*.

```
{
    "breakdown": [
        {
            "field":"first_name",
            "data":""
        },
        {
            "field":"middle_name",
            "data":""
        },
        {
            "field":"last_name",
            "data":""
        },
        {
            "field":"name_modifier",
            "data":""
        }
    ],
    "column_header":"name",
    "data": "Samuel Donovan",
    "label":"full_name",
    "column_description":""
}

```

### Data Point Example 3:
```{"first_name": "Eric", "column_description": "first name of employee"} ```

Entree detects that this data point has one key that has a label. The key is "first_name" and its label is "first_name".
Entree will create one  DATA_FORMAT object for this data point.

**(JSON) Object for first_name**

- "data" -> "Eric" (value stored in data point)
- "label" ->  "first_name" (label detected for value)
- "column_header" -> "" (key used to store the value in the data point)
- "column_description" -> "first name of employee" (value under the key column_description in the data point)
- "breakdown" -> [] ( empty array because the label is not one of the keys in the breakdownMap and thus has no composite fields)

*Note that the following DATA_FORMAT object will be shown in JSON*.

```
{
    "breakdown":[],
    "column_header":"first_name",
    "data": "Eric",
    "label":"first_name",
    "column_description":"first name of employee"
}
```






