package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import ot.dispatcher.plugins.bpm.util.{BpmParser, Caster}
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

import scala.collection.mutable

case class BpmCluster(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val AGG_DEFAULT: Boolean = true

  /** Command gets cases in clusters according to their traces
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"
   * @return dataframe with case-cluster liaison
   * keywords:
   *  - aggregate: returns cluster definition by trace composition and which percentage current cluster gets in full
   *               dataframe
   *               returns dataframe without changes but with adding cluster number to each event.
   */

  def transform(df: DataFrame): DataFrame = {

    val aggregate = Caster.safeCast[Boolean](
      keywords.get("aggregate"),
      AGG_DEFAULT,
      sendError(searchId, "The value of parameter 'aggregate' should be of bool type")
    )

    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"

    val case_time_window = Window.partitionBy(caseIdField).orderBy(timestampField)

    val traceDf = df.withColumn(timestampField, col(timestampField).cast(DoubleType))
      .withColumn("_traces", collect_list(col(eventIdField)).over(case_time_window))
      .withColumn("_time", collect_list(col(timestampField)).over(case_time_window))
      .groupBy(caseIdField)
      .agg(max("_traces").as("_traces"), max("_time").as("_time"))
      .withColumn("_cycles", BpmParser.getCycles(col("_traces")))
      .select(col(caseIdField), col("_traces"), col("_time"))

    val summaryCount = traceDf.withColumn("one", lit(1)).agg(sum("one")).first().get(0)

    val countWindow = Window.orderBy(desc("count"))
    val clusters = traceDf.groupBy(col("_traces")).count
      .orderBy(desc("count"))
      .withColumn("summary", lit(summaryCount))
      .withColumn("_percent", lit(100)*round(col("count")/col("summary"), 4))
      .withColumn("cluster", row_number().over(countWindow))
      .select(col("_traces"), col("count").as("_count"), col("_percent"), col("cluster"))


    if (aggregate) {clusters}
    else {
      val fullWindow = Window.partitionBy("_traces")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      val traces_set = clusters.select("_traces").rdd.
        map(r => r.get(0).asInstanceOf[mutable.WrappedArray[String]].toList).collect.toList
      val traces_map = typedLit(Map(traces_set.zipWithIndex map {s => (s._1, s._2  + 1)} : _*))

      val _exploded = traceDf.withColumn("temp", BpmParser.zip(col("_traces"), col("_time")))
        .withColumn("count", size(collect_set(caseIdField).over(fullWindow)))
        .withColumn("summary", lit(summaryCount))
        .withColumn("_percent", lit(100) * round(col("count") / col("summary"), 4))
        .withColumn("cluster", traces_map(col("_traces")))
        .withColumn("temp", explode(col("temp")))
        .withColumn(eventIdField, col("temp")("_1"))
        .withColumn(timestampField, col("temp")("_2"))
        .drop("temp", "_count")
        .select(col(timestampField), col(caseIdField), col(eventIdField), col("cluster"))
        .orderBy("cluster")
      _exploded
    }
  }
}

object BpmCluster extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmCluster(keywords, searchId, utils).transform
  }
}