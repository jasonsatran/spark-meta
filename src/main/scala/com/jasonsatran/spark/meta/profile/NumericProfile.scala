import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.jasonsatran.spark.meta.helper.Helper.isNumeric
import org.apache.spark.sql.functions.col



case class NumericStats(recordCount: Long, maxValue: Float, minValue: Float, avg: Float)

case class NumericProfile(df: DataFrame, columns: String*){


  lazy val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def convertToStatMatrix = {


    for (col <- columns){
      val numericData: DataFrame = colNumericData(col)
      val described: DataFrame = numericData.describe(col)
      yield extractStat(described)
    }

    columns.map {
      (col: String) => {
        val numericData: DataFrame = colNumericData(col)
        val described: DataFrame = numericData.describe(col)
        val stat = extractStat(described)




      }
    }
  }

  // expects input column to be single column that is numeric only
//
//  def colNumericStats(numOnlyDf: DataFrame): NumericStats ={
//    val c = df.columns(0)
//    val described = numOnlyDf.describe(c)
////    val count = described.filter()
//
//
//  }

  type numericStat = collection.mutable.Map[String, String]
  val statCols = List("count", "mean", "min", "max", "stddev")

  def extractStat(describedDf: DataFrame): numericStat = {

    val result = collection.mutable.Map[String, String]()
    statCols.foreach {
      (colName: String) =>
        result(colName)  =  describedDf.where(col(colName) === colName).columns(0)
    }
    result
  }

  def statsToDF(stats: List[numericStat]) : DataFrame = {
    stats.map{ (stat) =>
      numericStatToSeq(stat)
    }.toDF(statCols.mkString(","))
  }

  def numericStatToSeq(x: numericStat): Seq[String] = {
    Seq(x("count"), x("mean"), x("min"), x("max"), x("stddev"))
  }


  def colNumericData(col: String): DataFrame = {
    df.select(col).filter{ r: Row => {
      val x = Option(r(0).toString())
      x match {
        case None => false
        case Some(y) => isNumeric(y)
        }
      }
    }
  }

}
