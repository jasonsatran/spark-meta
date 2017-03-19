package com.jasonsatran.spark.meta.helper

import java.text.DecimalFormat
import org.apache.spark.sql.functions._

object Helper {

  def divide(numerator: Double, denominator: Double) : Double = {
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

  val udfIsEmpty = udf[Boolean,String] (isEmpty)

}