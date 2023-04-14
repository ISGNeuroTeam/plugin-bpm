package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, min}
import org.apache.spark.sql.types.DoubleType
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

case class BpmCollect(searchId: Int, utils: PluginUtils) {

  /** Command aggregates event traces by case id
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"
   * @return dataframe with cases ids and collected event traces (column with traces is Sequence type)
   */

  def transform(df: DataFrame): DataFrame = {

    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"

    df.withColumn(timestampField, col(timestampField).cast(DoubleType))
      .orderBy(timestampField)
      .groupBy(caseIdField)
      .agg(collect_list(col(eventIdField)).as("_traces"), min(col(timestampField)).as("_time"))
  }
}

object  BpmCollect extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmCollect(searchId, utils).transform
  }
}

