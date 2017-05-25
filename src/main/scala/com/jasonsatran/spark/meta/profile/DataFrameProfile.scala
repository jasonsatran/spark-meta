package com.jasonsatran.spark.meta.profile

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class DataFrameProfile(df: DataFrame)  {

  df.cache

  lazy val spark = SparkSession.builder().getOrCreate()

  val columnProfiles  : List[ColumnProfile] =
    for (c <- df.columns.toList)
      yield ColumnProfile.ColumnProfileFactory(df,c)

  val header : List[String] = List("Column Name","Record Count", "Unique Values", "Empty Strings" ,"Null Values", "Percent Fill", "Percent Numeric")

  def toDataFrame : DataFrame = {
    def dfFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {
      val rows = data.map{x => Row(x:_*)}
      val rdd = spark.sparkContext.parallelize(rows)
      val schema = StructType(header.split(",").
        map(fieldName => StructField(fieldName, StringType, true)))
      spark.sqlContext.createDataFrame(rdd, schema)
    }
    val data = columnProfiles.map(_.columnData)
    dfFromListWithHeader(data,header.mkString(","))
  }

  override def toString : String = {
    val colummProfileStrings : List[String] = columnProfiles.map(_.toString)
    (header.mkString(",") :: columnProfiles).mkString("\n")
  }
}

