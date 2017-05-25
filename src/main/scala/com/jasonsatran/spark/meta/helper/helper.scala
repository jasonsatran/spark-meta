package com.jasonsatran.spark.meta.helper

import java.text.DecimalFormat
import org.apache.spark.sql.functions._
import scala.util.Try

object Helper {

  def percentage(numerator: Double, denominator: Double) : Double = {
    round((numerator.toDouble / denominator.toDouble) * 100)
  }

  def round (x: Double) : Double = {
    val formatString = "#####.#"
    val formatter  = new DecimalFormat(formatString)
    val result = formatter.format(x)
    result.toDouble
  }

  def isEmpty(x: String) : Boolean = {
    if (Option(x) == None) return false  // null values are not empty by definition
    x.trim == ""
  }

  def isNumeric(x: String) : Boolean = {
    val z: Option[Float] =  Try(x.toFloat).toOption
    z != None
  }

  val udfIsEmpty = udf[Boolean,String] (isEmpty)
  val udfIsNumeric = udf[Boolean, String](isNumeric)

}