package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.plugins.bpm.util.{BpmParser, Caster}
import ot.dispatcher.sdk.PluginUtils

case class BpmCycles(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val AGG_DEFAULT: Boolean = true

  /** Command finds cycles in event logs
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"
   * @return dataframe with list of cycles
   * keywords:
   *  - aggregate: returns cycles with counters if true,
   *               returns cycles for corresponding cases if false
   */

  def transform(df: DataFrame): DataFrame = {

    val agg = Caster.safeCast[Boolean](
      keywords.get("aggregate"),
      AGG_DEFAULT,
      sendError(searchId, "The value of parameter 'aggregate' should be of bool type")
    )

    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"

    val trace_df =  df.withColumn(timestampField, col(timestampField).cast(DoubleType))
      .orderBy(timestampField)
      .groupBy(caseIdField)
      .agg(collect_list(eventIdField).as("_traces"))

    val summaryCount = lit(trace_df.count)
    val result = trace_df
      .withColumn("_cycles", BpmParser.getCycles(col("_traces")))
      .select(col(caseIdField), col("_cycles").getItem("_3").as("cycles"))
      .withColumn("cycle", explode(col("cycles")))

    if (agg) {
      result.groupBy("cycle")
        .count
        .orderBy(desc("count"))
        .withColumn("occurrences", lit(100) * round(col("count") / summaryCount, 5))
        .select(col("cycle"), col("count"), col("occurrences"))
    }
    else {
      result.select(col(caseIdField), col("cycle"))
    }
  }
}

object BpmCycles extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmCycles(keywords, searchId, utils).transform
  }

}