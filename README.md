# Entree
Entree is a tool that enables you to aggregate and label files present in an S3 bucket
into a one or more files with a standard data format.
The data format is set to be a JSON object with the following keys:
    - data
    - label
    - column header
    - breakdown
    - column description

The key "data" maps to the actual value stored in either a CSV column or JSON object. "label" maps to the the greater ontology/column field name of the
the key used in the original JSON object or column header in a CSV file. "column header" points to key used in the original JSON object or the column header
in the CSV file. "breakdown" maps to an object that breaks down a composite data point into its core components. A full address like "123 9th Street, NY,NY,10010" can be
broken down in street address, city,state and zipcode. Address is only one of the few labels that map to composite data. Another example is name (first name, middle name,
last nam and name modifier). Not all labels contain composite data and thus will map to an empty "breakdown" object.
The last key is "column description", if files contain a column description, it will be included here.
Currently Entree works as a command line tool (clt).

## Required
### sbt
download sbt from here http://www.scala-sbt.org/

## Input
This clt requires three inputs:
  1. s3 url pointing to bucket & folder path for source data (i.e https://s3.amazonaws.com/my_bucket/folderA/folderB)
  2. s3 url pointing to the bucket & folder path that you want to save the formatted data to.(i.e https://s3.amazonaws.com/my_bucket/folder1/folder2)
  3. label or column field name that you want to format data for. If you want to label all column field names in your bucket use "all",
  else type the column field name ( i.e "email_address"). Entree will only accept labels that are stored as values in cfnMap. Look at Important section.

### Label
The following is a list of accepted labels:
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

## Output
JSON data is aggregated into one or more files in NDJSON format, where each object has the following structure:

```
        {"data":"foo","label":"bar","originalLabel":"baz"}
```

## Getting started

### Compile
```sbt compile```

### Run
```sbt "run {s3_url_source} {s3_url_destination} label" ```

## Example
For an example, check the docs(./docs/Entree-Example.md)

To understand how the label / column field name mapping happens, check this doc(./docs/Column Field Name - Mapping.md)




