## Data Format Object

DATA_FORMAT is an object that users will use to define the structure and the content of the object that Entree will use
to label and aggregate data. Users can define this object in **user-input.conf**.
The DATA_FORMAT object should handle and store the data and its contextual information. Most key-value pairs in the object
should be first level entries( not nested).

### Breakdown
If a "breakdown" of the data into its composite fields should be needed, it can be added to the DATA_FORMAT object and
it should be the only nested parameter in the object. An example of a breakdown is :


address | house_number | street_address | apartment_number | state | city | zip_code |
-------- | ------------ | -------------- | ---------------- | ----- | ---- | -------- |
123 Washington Avenue, Apt 3B, NY , NY 10000 | 123 | Washington Avenue | Apt 3B | NY | NY | 10000 |

Note that a breakdown occurs only if the label detected by Entree for the datapoint is made of composite fields. In this case
Entree looked at datapoint ```123 Washington Avenue, Apt 3B, NY , NY 10000``` labeled it as **address**  and recognized that
address is a label made of composite fields : house_number, street_address, apartment_number, state, city and zip_code.
Users can define which labels if any have composite fields  by adding a Map to **user-input.conf**.

An example of a user defined Map for relating labels with its components is :

```
breakdownMap = Map (
   "full_name" -> Array("first_name","middle_name","last_name","name_modifier"),
   "address" -> Array("house_number","street address","apartment_number","city",
     "state","zip_code")
   )
```

### Structure
The object should be composed of n key - value pairs, where n is defined by the user. Each key is defined as an object
parameter and has a set of properties : action,type and components. All keys should have an action ,type and components defined.

#### Action
Action is a property that relates to the action that Entree will need to do to get the value for its key. Entree has a number of defined
actions :
* value : Entree grabs the value stored in the data point. (i.e {"first_name":"Frank"}, "Frank" is the value of the data point.
* label : Entree gets the label for the key in the data point (i.e {"first_name":"Frank"}, "name" is the label for the key "first_name".
* column : Entree gets the key used in the original data point ((i.e {"first_name":"Frank"}, "first_name" is the key/column in this data point.
* description : Entree gets the column description in the data point. If none is found, then column description is a set as an empty String.
* decomposition : Entree


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
    division{
        action:decomposition
        type: "type": "Vector[Map[String,String]"
        components:[
            {
                composite_field{
                    action:sub_label
                    type:String
                    components:[]
                }
                data{
                    action:"no_action"
                    type:String
                    components:[]
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
