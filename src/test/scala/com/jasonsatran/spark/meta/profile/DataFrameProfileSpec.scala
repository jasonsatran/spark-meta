package com.jasonsatran.spark.meta.profile

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec
import org.apache.spark.sql.DataFrame

class DataFrameProfileSpec extends FunSpec with  DataFrameSuiteBase {

  import spark.implicits._

  describe ("DataFrameProfile") {
    it("profiles a dataframe") {
      val profiledResult = DataFrameProfile(baseballDf).toDataFrame
      assertDataFrameEquals(profiledResult,expectedDf)
    }

    it("displays as a toString"){
      val actual : String = DataFrameProfile(baseballDf).toString
      val expected =
        """
          |                                                 Column Name                                                Record Count                                               Unique Values                                                 Null Values                                                Percent Fill
          |                                                        team                                                           5                                                           4                                                           0                                                       100.0
          |                                            lastChampionship                                                           5                                                           5                                                           1                                                        80.0
          |                                                        city                                                           5                                                           3                                                           0                                                       100.0
        """.stripMargin.trim
      assert(actual.trim===expected)
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

  def expectedDf : DataFrame = {
    Seq(
      ("team","5","4","0","100.0"),
      ("lastChampionship","5","5","1","80.0"),
      ("city","5","3","0","100.0")
    ).toDF("Column Name","Record Count", "Unique Values", "Null Values" , "Percent Fill")
  }

  def baseballDf: DataFrame = {
    Seq(
      ("Mets","1986","New York"),
      ("Yankees","2009","New York"),
      ("Cubs","2016","Chicago"),
      ("Cubs","2005","Chicago"),
      ("Nationals","","Washington")
    ).toDF("team","lastChampionship","city")
  }
}


