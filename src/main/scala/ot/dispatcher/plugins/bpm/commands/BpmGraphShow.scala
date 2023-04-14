package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils
import ot.dispatcher.plugins.bpm.util.{BpmParser, Caster}

case class BpmGraphShow(searchId: Int, keywords: Map[String, String], utils: PluginUtils) {

  import utils._

  val DEF_SORT_COLUMN: String = "sort_column"

  /** Command transform dataframe to a form, used in EVA to display graphs
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"+ column with secondary sort index to
   *           avoid uncertainty in event ordering (name of column should be defined in OTL command).
   * @return dataframe with columns node, relation, edge etc. (used for graph representation)
   */

  def transform(df: DataFrame): DataFrame = {

    val sort_column = Caster.safeCast[String](
      keywords.get("sort_by"),
      DEF_SORT_COLUMN,
      sendError(searchId, "The value of parameter 'sort_by' should be of String type")
    )

    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"

    val winPair = Window.partitionBy(eventIdField, "next_event")
    val winEvent = Window.partitionBy(eventIdField)
    val winPair2 = Window.partitionBy(eventIdField, "next_event").orderBy("dt")
    val fullWindow = Window.partitionBy(caseIdField).orderBy(timestampField, sort_column)

    val schema = StructType(List(StructField(eventIdField, StringType, nullable = true),
      StructField("next_event", StringType, nullable = true),
      StructField("edge_description", StringType, nullable = true),
      StructField("node_description", LongType, nullable = true),
      StructField("median", IntegerType, nullable = true),
      StructField("edge_count", IntegerType, nullable = true),
      StructField("node_color", IntegerType, nullable = true),
      StructField("edge_color", IntegerType, nullable = true),
      StructField("line_color", IntegerType, nullable = true),
      StructField("relation_id", IntegerType, nullable = true)))

    val seq = List(("finish", null, null, null, null, null, null, null, null, null))
    val rdd = spark.sparkContext.makeRDD(seq.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10)))
    val fakeFinish = spark.createDataFrame(rdd, schema)
    //fakeFinish.show(1)

    val fillNaCols: Array[String] = Array("node", "relation", "median", "edge_count", "edge_description", "node_description", "node_color",
      "edge_color", "line_color", "relation_id", "id")




    val fakeStart = df.withColumn("next_event", first(col(eventIdField)).over(fullWindow))
      .groupBy("next_event").agg(first(col(timestampField)).as("first"))
      .withColumn(eventIdField, lit("start"))
      .select(
        col(eventIdField), col("next_event"),
        lit(null).as("edge_description"),
        lit(null).as("node_description"),
        lit(null).as("median"),
        lit(null).as("edge_count"),
        lit(null).as("node_color"),
        lit(null).as("edge_color"),
        lit(null).as("line_color")
      )


    val graph_df = df
      .withColumn("edge_description", round(avg(col("dt")).over(winPair), 1))
      .withColumn("fake_column",collect_list("dt") over winPair2)
      .withColumn("med1", element_at(col("fake_column"), ceil((size(col("fake_column")) + 1)/2).cast("int")))
      .withColumn("med2", element_at(col("fake_column"), floor((size(col("fake_column")) + 1)/2).cast("int")))
      .withColumn("med", (col("med1") + col("med2"))/2)
      .withColumn("edge_count", count(caseIdField).over(winPair))
      .withColumn("node_description", count(caseIdField).over(winEvent))
      .groupBy(eventIdField, "next_event")
      .agg(max(col("edge_description")).as("edge_description"),
        max(col("node_description")).as("node_description"),
        max(col("med")).as("median"),
        max(col("edge_count")).as("edge_count")

      )
      .withColumn("max_node", max(col("node_description")).over(Window.partitionBy(lit(1))))
      .withColumn("max_edge", max(col("edge_description")).over(Window.partitionBy(lit(1))))
      .withColumn("node_color", BpmParser.getColor(col("node_description"), col("max_node")))
      .withColumn("edge_color", BpmParser.getColor(col("edge_description"), col("max_edge")))
      .withColumn("line_color", col("node_color"))
      .drop("max_node", "max_edge")
      // Unite with Start and Finish
      .unionByName(fakeStart)
      .withColumn("relation_id", BpmParser.hashInt(col("next_event")))
      .unionByName(fakeFinish)
      .withColumn("id", BpmParser.hashInt(col(eventIdField)))
      .select(
        col(eventIdField).as("node"), col("next_event").as("relation"), col("median"), col("edge_count"),
        col("edge_description"), col("node_description"), col("node_color"),
        col("edge_color"), col("line_color"), col("relation_id"), col("id")
      )
      .select(fillNaCols.map(c => col(c).cast(StringType)) : _*)
      .na.fill("-", fillNaCols)
    graph_df
  }
}

object BpmGraphShow extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmGraphShow(searchId, keywords, utils).transform
  }
}