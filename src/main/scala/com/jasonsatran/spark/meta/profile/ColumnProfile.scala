package com.jasonsatran.spark.meta.profile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.jasonsatran.spark.meta.helper.Helper._

case class ColumnProfile(columnName: String, totalDataSetSize : Long, uniqueValues : Long, nullValues : Long){
  def columnData : List[String]= {
    List(
      columnName
      ,totalDataSetSize
      ,uniqueValues
      ,nullValues
      ,percentFill((totalDataSetSize-nullValues),totalDataSetSize)
    ).map(_.toString)
  }

  override def toString : String= {
    List(
      columnName
      ,totalDataSetSize
      ,uniqueValues
      ,nullValues
      ,percentFill((totalDataSetSize-nullValues),totalDataSetSize)
    ).map(x=>formatColumn(x.toString)).mkString("")
  }
}

object ColumnProfile{
  def ColumnProfileFactory(df: DataFrame, columnName : String) : ColumnProfile = {
    val dfColumn = df.select(columnName)
    dfColumn.cache
    val recordCount = dfColumn.count()
    val uniqueValues = dfColumn.distinct().count()
    val nullValues = dfColumn.withColumn("isEmpty", udfIsEmpty(col(columnName))).filter(col("isEmpty")===true).count
    new ColumnProfile(columnName,recordCount,uniqueValues,nullValues)
  }
}