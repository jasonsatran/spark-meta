package com.jasonsatran.spark.meta.profile

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.jasonsatran.spark.meta.helper.Helper.formatColumn

case class DataFrameProfile(df: DataFrame)  {

  df.cache

  lazy val spark = SparkSession.builder().getOrCreate()

  val columnProfiles  : List[ColumnProfile] =
    for (c <- df.columns.toList)
      yield ColumnProfile.ColumnProfileFactory(df,c)

  def toDataFrame : DataFrame = {
    def dfFromListWithHeader(data: List[List[String]], header: String) : DataFrame = {
      val rows = data.map{x => Row(x:_*)}
      val rdd = spark.sparkContext.parallelize(rows)
      val schema = StructType(header.split(",").
        map(fieldName => StructField(fieldName, StringType, true)))
      spark.sqlContext.createDataFrame(rdd,schema)
    }
    val header : List[String] = List("Column Name","Record Count", "Unique Values", "Null Values" , "Percent Fill")
    val data = columnProfiles.map(_.columnData)
    dfFromListWithHeader(data,header.mkString(","))
  }


  override def toString : String = {
    val header : String = List("Column Name","Record Count", "Unique Values", "Null Values" , "Percent Fill").map(formatColumn(_)).mkString("")
    val colummProfileStrings : List[String] = columnProfiles.map(_.toString)
    (header :: columnProfiles).mkString("\n")
  }
}

