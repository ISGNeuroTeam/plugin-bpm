package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.plugins.bpm.util.Caster
import ot.dispatcher.sdk.PluginUtils


case class BpmRemapTime(modelConfig: Option[Config], searchId: Int,
                          keywords: Map[String, String], utils: PluginUtils) {

  import utils._

  val DEFAULT_KEYWORD: String = "id_calendar"
  val DEFAULT_START: String = "start_ts"
  val DEFAULT_END: String = "end_ts"
  val CALENDAR_PATH: String = modelConfig.get.getString("calendar")

  val calendar_df: DataFrame = spark.read.option("header", value = true).csv(CALENDAR_PATH)

  /** Function cuts calendar rows by start time and end time and calculates total hours
   * @return total oh working hours between start_timestamp end_timestamp according to calendar_id
   */
  def remap_time: (Int, Int, String) => Double = { (start_timestamp, end_timestamp, calendar_id) =>
    val filtered_calendar = calendar_df
      .filter(col("id_calendar").equalTo(calendar_id))
      .filter(col("start_ts") < end_timestamp && col("end_ts") > start_timestamp)
      .withColumn("start_ts", when(col("start_ts") < start_timestamp, start_timestamp)
        .otherwise(col("start_ts")))
      .withColumn("end_ts", when(col("end_ts") > end_timestamp, end_timestamp)
        .otherwise(col("end_ts")))
      .withColumn("duration", col("end_ts") - col("start_ts"))
    val counter = filtered_calendar.agg(sum("duration")).first
    counter.getDouble(0)
  }

  val remap_time_udf: UserDefinedFunction = udf(remap_time)

  /** Command calculates working hours between two timestamps according to configured working calendar.
   * @param df should have following columns: "start_ts" - starting timestamp
   *                                          "end_ts" - ending timestamp
   *                                          key - some column to indicate calendar name from where to take working
   *                                                hours for each row.
   * @return dataframe with column "work_dif" with calculated working hours.
   */
  def transform(df: DataFrame): DataFrame = {

    val start_col = Caster.safeCast[String](
      keywords.get("start"),
      DEFAULT_START,
      sendError(searchId, "The value of parameter 'start' should be of string type")
    )

    val end_col = Caster.safeCast[String](
      keywords.get("end"),
      DEFAULT_END,
      sendError(searchId, "The value of parameter 'end' should be of string type")
    )

    val calendar_key = Caster.safeCast[String](
      keywords.get("key"),
      DEFAULT_KEYWORD,
      sendError(searchId, "The value of parameter 'key' should be of string type")
    )

    df
      .withColumn("work_dif", remap_time_udf(col(start_col), col(end_col), col(calendar_key)))
  }
}

object BpmRemapTime extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmRemapTime(modelConfig, searchId, keywords, utils).transform
  }
 }