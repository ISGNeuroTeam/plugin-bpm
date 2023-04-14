package ot.dispatcher.plugins.bpm.commands
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.functions.col

case class BpmPrecountPercent(searchId: Int, utils: PluginUtils) {

  def transform(df: DataFrame): DataFrame = {
    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"
//    val regionIdField: String = "region"
//    val ROOIdfield: String = "ROO"

    val winPair = Window.partitionBy(eventIdField, "next_event")
    val winEvent = Window.partitionBy(eventIdField)
    val fullWindow = Window.partitionBy(caseIdField).orderBy(timestampField, "sort_column")
    val winPair2 = Window.partitionBy(eventIdField, "next_event").orderBy("dt")

    val fakeStart = df.withColumn("next_event", first(col(eventIdField)).over(fullWindow))
      .groupBy("next_event").agg(countDistinct(col(caseIdField)).as("edge_count"))
      .withColumn(eventIdField, lit("start"))
      .withColumn("edge_description", lit(null))
      .withColumn("node_description", lit(null))
      .withColumn("node_dcount", lit(null))
      //.withColumn("node_count", lit(null))
      .withColumn("edge_dcount", col("edge_count"))
      .withColumn("median", lit(null))

    val percent_graph = df.withColumn("next_event", lag(col(eventIdField), -1).over(fullWindow))
      .withColumn("next_timestamp", lag(col(timestampField), -1).over(fullWindow))
      .withColumn("dt", col("next_timestamp") - col(timestampField))
      .na.fill("finish", Array("next_event"))
      .withColumn("edge_avg_time", avg(col("dt")).over(winPair))
      .withColumn("fake_column",collect_list("dt") over winPair2)
      .withColumn("med1", element_at(col("fake_column"), ceil((size(col("fake_column")) + 1)/2).cast("int")))
      .withColumn("med2", element_at(col("fake_column"), floor((size(col("fake_column")) + 1)/2).cast("int")))
      .withColumn("med", (col("med1") + col("med2"))/2)
      .withColumn("node_count", count(caseIdField).over(winEvent))
      .withColumn("node_dcount", approx_count_distinct(caseIdField).over(winEvent))
      .withColumn("edge_count", count(caseIdField).over(winPair))
      .withColumn("edge_dcount", approx_count_distinct(caseIdField).over(winPair))
      .groupBy(eventIdField, "next_event")
      .agg(max(col("edge_avg_time")).as("edge_description"), first(col("node_count")).as("node_description"),first(col("node_dcount")).as("node_dcount"), first(col("edge_count")).as("edge_count"), first(col("edge_dcount")).as("edge_dcount"), max(col("med")).as("median"))
      .unionByName(fakeStart)
      //.withColumn("relation", struct(col("eventId"), col("next_event")))
      .withColumn("relation", concat(col("eventId"),lit('_'), col("next_event")))

      .drop("next_event")
      //.filter(col("node_description") ===  6)
    percent_graph

  }
}

object BpmPrecountPercent extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmPrecountPercent(searchId, utils).transform
  }
}