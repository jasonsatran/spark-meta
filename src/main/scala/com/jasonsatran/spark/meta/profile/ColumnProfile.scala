package com.jasonsatran.spark.meta.profile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.jasonsatran.spark.meta.helper.Helper._

case class ColumnProfile(columnName: String
                          , totalDataSetSize: Long
                          , uniqueValues: Long
                          ,emptyStringValues : Long
                          , nullValues: Long){

  lazy val percentFill : Double = calculatedPercentFill(nullValues, emptyStringValues, totalDataSetSize)

  def columnData : List[String]= {
    List(
      columnName
      ,totalDataSetSize
      ,uniqueValues
      ,emptyStringValues
      ,nullValues
      ,percentFill
    ).map(_.toString)
  }

  def calculatedPercentFill(nullValues: Long, emptyStringValues: Long, totalRecords: Long) : Double = {
    val filledRecords = totalRecords - nullValues - emptyStringValues
    divide(filledRecords, totalRecords)
  }

  override def toString : String= {
    List(
      columnName
      ,totalDataSetSize
      ,uniqueValues
      ,emptyStringValues
      ,nullValues
      ,percentFill
    ).mkString(",")
  }
}

object ColumnProfile{
  def ColumnProfileFactory(df: DataFrame, columnName : String) : ColumnProfile = {
    val dfColumn = df.select(columnName)
    dfColumn.cache
    val recordCount = dfColumn.count()
    val uniqueValues = dfColumn.distinct().count()
    val emptyCount = dfColumn.withColumn("isEmpty", udfIsEmpty(col(columnName))).filter(col("isEmpty")===true).count
    val nullCount = dfColumn.withColumn("isNull", col(columnName).isNull).filter(col("isNull")).count()
    new ColumnProfile(columnName, recordCount, uniqueValues, emptyCount, nullCount)
  }
}