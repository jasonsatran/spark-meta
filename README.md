# spark-meta

Meta data utilities for the Spark DataFrame


## Profile

Data profiling works similar to df.describe(), but acts on non-numeric columns.

To use profile execute the implicit method profile on a DataFrame.  An example follows.

```scala
import com.jasonsatran.spark.meta.profile.DataFrameUtils._

val df = Seq(

      ("Mets","1986", "New York", "27896702"),
      ("Yankees", "2009", "New York", "3063405"),
      ("Cubs", "2016", "Chicago", "3232420"),
      ("White Sox","2005","Chicago", "1746293"),
      ("Nationals","","Washington", "2481938"),
      ("Senators","1924","Washington",null)
    ).toDF("Team", "Last Championship", "City", "2016 Attendance")

df.profile.show

```

will produce results like this:

```
+-----------------+------------+-------------+-------------+-----------+------------+---------------+
|      Column Name|Record Count|Unique Values|Empty Strings|Null Values|Percent Fill|Percent Numeric|
+-----------------+------------+-------------+-------------+-----------+------------+---------------+
|             Team|           6|            6|            0|          0|       100.0|            0.0|
|Last Championship|           6|            6|            1|          0|        83.3|           83.3|
|             City|           6|            3|            0|          0|       100.0|            0.0|
|  2016 Attendance|           6|            6|            0|          1|        83.3|           83.3|
+-----------------+------------+-------------+-------------+-----------+------------+---------------+
```

### Results Explained

The profile command is similar to the Describe except that it gives us statistics on non-numeric columns. This allows us to get a quick overview of the contents of a DataFrame before getting into in depth analysis.

We provide the following metrics on each column of the input DataFrame

- Record Count:  The total number of records in the column
- Unique Values:  The distinct count of unique values in a column
- Empty Strings:  The count of empty strings in a column
- Null Values:  The count of null values in a column
- Percent Fill:  The percentage of records that are not empty string or null
- Percent Numeric:  The percentage of records that are numeric


### A Note about Performance

Limit the columns to only those that are required as these calculations are processor intensive.  Several queries are executed for each column.

In other words before running profile on a DataFrame it is advisable to select only the required colums:

```scala
someDataFrame.select("column1","column2").profile
```

will perform better than

```scala
someDataFrame.profile
```
