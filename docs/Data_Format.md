## Data Format

DATA_FORMAT is an object that users will use to define the structure and the content of the object that Entree will use
to label and aggregate data. Users can define this object in **user-input.conf**.
The DATA_FORMAT object should handle and store the data and its contextual information. Most key-value pairs in the object
should be first level entries( not nested).

If a "breakdown" of the data into its composite fields should be needed, it can be added to the DATA_FORMAT object and
it should be the only nested parameter in the object. An example of a breakdown is :


Address | house_number | street_address | apartment_number | state | city | zip_code |
-------- | ------------ | -------------- | ---------------- | ----- | ---- | -------- |
123 Washington Avenue, Apt 3B, NY , NY 10000 | 123 | Washington Avenue | Apt 3B | NY | NY | 10000 |

Note that a breakdown occurs only if the label detected by Entree for the datapoint is made of composite fields.

