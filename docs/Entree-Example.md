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

If you run
```
sbt "run https://s3.amazonaws.com/dummy_bucket/data https://s3.amazonaws.com/dummy_bucket/clean_data email_address"
 ```
all of the files under the label f email_address, such as : email_adddress.json, email.json and emailaddress.json
will be aggregated into one or more files with a standard data format.

The new file will look like the following:

```
{"data":"hzk@yahoo.com","label":"email_address","originalLabel":"email"}
{"data":"trevorp@hotmail.com","label":"email_address","originalLabel":"email"}
{"data":"xyz@gmail.com","label":"email_address","originalLabel":"email_address"}
{"data":"bot@gmail.com","label":"email_address","originalLabel":"email_address"}
{"data":"ark@byu.edu","label":"email_address","originalLabel":"emailaddress"}
{"data":"lol@aol.com","label":"email_address","originalLabel":"emailaddress"}
```
This file will be stored in https://s3.amazonaws.com/dummy_bucket/clean_data


This assumes that the DATA_FORMAT object that user defined in user-input.json contained three keys: data , label and
originalLabel. "data" refers to the actual data point value, "label" refers to the label associated with the data
point and "originalLabel" refers to the original column name used to store the data point value.
For more about DATA_FORMAT, refer to [Data_Format](./docs/Data_Format.md)
