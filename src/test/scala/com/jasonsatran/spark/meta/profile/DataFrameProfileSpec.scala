package com.jasonsatran.spark.meta.profile

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.DataFrame

class DataFrameProfileSpec extends FunSpec with  DataFrameSuiteBase {

  import spark.implicits._

  describe ("DataFrameProfile") {
    it("returns expected columns") {
      val profiledResult = DataFrameProfile(baseballDf).toDataFrame
      assert(profiledResult.columns.mkString("|") === "Column Name|Record Count|Unique Values|Empty Strings|Null Values|Percent Fill")
    }

    it("displays as a toString"){
      val actual : String = DataFrameProfile(baseballDf).toString
      val expected =   scala.io.Source.fromFile("./src/test/Resources/expected/expectedBaseballTest").getLines().mkString("\n")
      assert(actual.trim === expected)
    }

    it ("works with a second data set"){
      val df = spark.sqlContext.read.format("csv")
        .option("header", "true").
        option("charset", "UTF8")
        .option("delimiter",",")
        .load("./src/test/resources/NYC_Social_Media_Usage.csv")
      val actual : String = DataFrameProfile(df).toString
      val expected =   scala.io.Source.fromFile("./src/test/Resources/expected/expectedNYC").getLines().mkString("\n")
      assert (actual===expected)
    }
  }

  def baseballDf: DataFrame = {
    Seq(
      ("Mets","1986", "New York", "nick"),
      ("Yankees", "2009", "New York", "dave"),
      ("Cubs", "2016", "Chicago", "bruce"),
      ("White Sox","2005","Chicago", null),
      ("Nationals","","Washington", null)
    ).toDF("team", "lastChampionship", "city", "number 1 fan")
  }
}


