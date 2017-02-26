# spark-meta

Meta data utilities for the Spark DataFrame.

The first version of this library adds a profile command to a DataFrame via an implicit class.   This works in a similar way to the df.describe, but acts on non-numeric columns.

## Profile

The simplest way to execute the profile on a DataFrame is by doing this:

(In this example df is a DataFrame.  Please see the note about performance below if your DataFrame is large.)

````
import  com.jasonsatran.spark.meta.profile.DataFrameUtils._
df.profile.show
````

will produce results like this:

````
scala> df.profile.show
+----------------+------------+-------------+-----------+------------+
|     Column Name|Record Count|Unique Values|Null Values|Percent Fill|
+----------------+------------+-------------+-----------+------------+
|        column 1|           5|            4|          0|       100.0|
|        column 2|           5|            5|          1|        80.0|
|        column 3|           5|            3|          0|       100.0|
+----------------+------------+-------------+-----------+------------+

````


### Results Explained

The profile command is similar to the Describe except that it gives us statistics on non-numeric columns. This allows us to get a quick overview of the contents of a DataFrame before getting into in depth analysis.

We provide the following metrics on each column of the input DataFrame

- Record Count:  The total number of records in the column
- Unique Values:  The distinct count of unique values in a column
- *Null Values:  The count of null or empty string values in a column
- Percent Fill:  Non null or empty / record count


### A Note about Performance

Limit the columns to only those that are required as these calculations are processor intensive.  Several queries are executed for each column.

In other words before running profile on a DataFrame it is advisable to select only the required colums:

````
someDataFrame.select("column1","column2").profile
````

will perform better than

````
someDataFrame.profile
````

### Demonstration

The following commands can be used to demonstrate the profile method in the Spark shell.

````
import com.jasonsatran.spark.meta.profile.DataFrameProfile
import spark.implicits._

val df =   Seq(
               ("Mets","1986","New York"),
               ("Yankees","2009","New York"),
               ("Cubs","2016","Chicago"),
               ("Cubs","2005","Chicago"),
               ("Nationals","","Washington")
             ).toDF("team","lastChampionship","city")

val profile = DataFrameProfile(df)
profile.toDataFrame.show
println(profile.toString)

// alternatively, a convenience method is added via implicit class

import  com.jasonsatran.spark.meta.profile.DataFrameUtils._

df.profile.show

````