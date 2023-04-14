package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

case class BpmDropConsecutive(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  /** Command deletes full duplicates and equal events which are consecutive in event trace
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"
   * @return dataframe with deleted consecutive duplicates
   */

  def transform(df: DataFrame): DataFrame = {

    val caseIdField: String = "caseId"
    val timestampField: String = "_time"
    val eventIdField: String = "eventId"
    val occurrenceField: String = "occurrenceNum"
    val rowField: String = "row_number"

    val dropColumnsList = Seq(rowField, occurrenceField, "previous_event", "previous_time", "first_event", "last_event", "row_number")

    /**
     * These windows sort events by:
     * -time
     * -order of first occurrence - to evade loss of 2-events cycles when both events got same timestamp
     */

    val caseWindow: WindowSpec = Window.partitionBy(caseIdField)
      .orderBy(rowField)
    val caseWindowFull: WindowSpec = caseWindow
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val eventWindow: WindowSpec = caseWindow.partitionBy(caseIdField, eventIdField)
    val streamWindow: WindowSpec = caseWindow.orderBy(col(timestampField), col(rowField), col(occurrenceField).desc)

    df.dropDuplicates()
      .withColumn(rowField, monotonically_increasing_id())
      .withColumn(occurrenceField, first(rowField).over(eventWindow))
      .withColumn("last_event", last(col(eventIdField)).over(caseWindowFull))
      .withColumn("first_event", first(col(eventIdField)).over(caseWindowFull))
      .withColumn("previous_event", lag(col(eventIdField), 1).over(streamWindow))
      .withColumn("previous_time", lag(col(timestampField), 1).over(streamWindow))
      .filter(!(!isnull(col("previous_event")) && (col(eventIdField) === col("previous_event"))))
      .drop(dropColumnsList: _*)
  }
}

object BpmDropConsecutive extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmDropConsecutive(keywords, searchId, utils).transform
  }
}