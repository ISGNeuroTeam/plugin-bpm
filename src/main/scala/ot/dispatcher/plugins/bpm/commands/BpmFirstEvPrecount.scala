package ot.dispatcher.plugins.bpm.commands
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.functions.col

case class BpmFirstEvPrecount(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  def transform(df: DataFrame): DataFrame = {

    val caseIdField: String = "caseId"
    val eventIdField: String = "eventId"
    val timestampField: String = "_time"

//    val winPair = Window.partitionBy(eventIdField, "next_event")
//    val winEvent = Window.partitionBy(eventIdField)
    val fullWindow = Window.partitionBy(caseIdField).orderBy(timestampField, "sort_column")
    val Window_altern = Window.partitionBy(caseIdField).orderBy(desc(timestampField), desc("sort_column"))
//    val W_row = Window.orderBy(timestampField, "sort_column")
    val fakeStart2 = df.withColumn("next_event", first(col(eventIdField)).over(fullWindow))
      .withColumn(eventIdField, lit("start"))
      .withColumn("row", row_number.over(fullWindow))
      .where(col("row") === 1).drop("row")
      .withColumn("next_timestamp", lit(null))
      .withColumn("dt", lit(null))
    val full_input1 = df.withColumn("next_event", lag(col(eventIdField), -1).over(fullWindow))
      .withColumn("next_timestamp", lag(col(timestampField), -1).over(fullWindow))
      .withColumn("dt", col("next_timestamp") - col(timestampField))
      .na.fill("finish", Array("next_event"))
    val full_input = full_input1.unionByName(fakeStart2)
      //ы.withColumn("relation", struct("eventId", "next_event"))
      .withColumn("relation", concat(col("eventId"),lit('_'), col("next_event")))

      .orderBy("_time")

    val case_first_last_event = full_input1.withColumn("first_event", first(col(eventIdField)).over(fullWindow))
      .withColumn("last_event", row_number().over(Window_altern))
      .filter("last_event = 1")
      .drop("last_event")
      .select("caseId", "eventId", "first_event")
      .withColumnRenamed("eventId", "last_event")

    //Составляем сводную таблицу с описанием каждого из кейсов(действия, связи, регионы, ROO)

    val case_description1 = full_input.groupBy("caseId")
      .agg(collect_list("relation").as("array1_relations"), collect_list(col("eventId")).as("array1_events"), collect_list(col("region")).as("array_regions"), collect_list(col("ROO")).as("array_ROOS"))
      .withColumn("array1_distinct_events", array_distinct(col("array1_events")))
      .withColumn("array1_distinct_relations", array_distinct(col("array1_relations")))
      .withColumn("regions", array_distinct(col("array_regions")))
      .withColumn("ROOS", array_distinct(col("array_ROOS")))
      .drop("array_regions", "array_ROOS", "array1_events", "array1_relations")
      .withColumnRenamed("caseId", "caseId2")
    val case_description = case_description1.join(case_first_last_event, case_description1("caseId2") === case_first_last_event("caseId"), "inner")
      .drop("caseId")

    //Добавляем к начальным данным информацию по кейсам(эвент лог с расширенной информацией по каждому кейсу)
    val full_description = full_input.join(case_description, full_input("caseId") === case_description("caseId2"), "inner")
      .drop("caseId2")
      .withColumn("time_range", struct(col("_time"), col("next_timestamp")))
      .drop("next_timestamp", "dt")

    //    val description = full_description.groupBy(column_to_group).agg(collect_list(col("array1_distinct_events")).as("distinct_events"), collect_list(col("array1_distinct_relations")).as("distinct_relations"), collect_list(col("caseId")).as("array_cases"), collect_list(col("array1_events")).as("events"), collect_list(col("array1_relations")).as("relations"))
    //      .withColumn("full_actions", array_distinct(flatten(col("distinct_events"))))
    //      .withColumn("full_relations", array_distinct(flatten(col("distinct_relations"))))
    //      .withColumn("full_cases", array_distinct(col("array_cases")))


    val description = full_description.groupBy("first_event").agg(collect_set(col("array1_distinct_events")).as("distinct_events"), collect_set(col("array1_distinct_relations")).as("distinct_relations"), collect_list(col("caseId")).as("array_cases"))
      .withColumn("full_actions", array_distinct(flatten(col("distinct_events"))))
      .withColumn("full_relations", array_distinct(flatten(col("distinct_relations"))))
      .withColumn("full_cases", array_distinct(col("array_cases")))
      .drop("distinct_events", "distinct_relations", "array_cases")


    description
  }
}

object BpmFirstEvPrecount extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmFirstEvPrecount(keywords, searchId, utils).transform
  }
}
