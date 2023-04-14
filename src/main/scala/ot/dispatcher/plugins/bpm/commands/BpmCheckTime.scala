package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.bpm.util._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

import scala.collection.immutable.Map

case class BpmCheckTime(modelConfig: Option[Config], keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val DEF_FIELD: String = "traces"
  val DEF_TIME: String = "tracet"
  val DEF_TIME_SCHEMA: String = modelConfig.get.getString("timepath")

  /** For each row in dataframe command searches if value in column 'token' is in corresponding value of column 'sequence'
   *
   * @param df should have at least 2 column with trace description and times array
   * @return dataframe with time check result
   * keywords:
   *  - trace_field: name of column with traces
   *  - trace_time: name of column with times
   *  - time_schema: path to file with model times
   */
  def transform(df: DataFrame): DataFrame = {

    val traceField = Caster.safeCast[String](
      keywords.get("trace_field"),
      DEF_FIELD,
      sendError(searchId, "The value of parameter 'trace_field' should be of string type")
    )
    val traceTime = Caster.safeCast[String](
      keywords.get("trace_time"),
      DEF_TIME,
      sendError(searchId, "The value of parameter 'trace_time' should be of string type")
    )
    val times_schema = Caster.safeCast[String](
      keywords.get("time_schema"),
      DEF_TIME_SCHEMA,
      sendError(searchId, "The value of parameter 'time_schema' should be of string type")
    )

    def read_time_schema(path: String): Map[String, Double] = {
      val times_source = scala.io.Source.fromFile(path)
      val content = times_source.getLines.map(_.split(","))
      val maps = content.map(x => x(0) -> x(1).toDouble)
      val time_output = collection.immutable.Map(maps.toSeq: _*)
      times_source.close()
      time_output
    }

    val dict_time = read_time_schema(times_schema)
    val keys = dict_time.keySet.toArray
    val keys1 = keys.toSeq
    val df_time = dict_time.map{case(a,b) => Seq(a) ++ Seq(b.toString)}.toSeq

    df.withColumn("dicttime", typedLit(df_time))
      .withColumn("keys", typedLit(keys1))
      .withColumn("conformance", BpmParser.checktime(col(traceField), col(traceTime), col("dicttime"), col("keys")))
      .withColumn("time_check", col("conformance"))
      .select(col("time_check"))
  }
}

object BpmCheckTime extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmCheckTime(modelConfig, keywords, searchId, utils).transform
  }
}