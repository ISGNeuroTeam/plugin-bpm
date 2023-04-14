package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.bpm.util.Caster
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

case class BpmFormatDuration(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  /** Command transform timestamps values into readable values (ex. 91 -> 1 minute)
   *
   * @param df should have at least one column
   * @return dataframe with formatted duration column (indicated by keyword)
   *
   * keywords:
   *  - column: name of column to format
  */

  def transform(df: DataFrame): DataFrame = {

    val time_column = Caster.safeCast[String](
      keywords.get("column"),
      "",
      sendError(searchId, "The value of parameter 'column' should be of String type")
    )
    val format_column = {
      if (time_column == "") sendError(searchId, "The value of parameter 'column' should be defined")
      else time_column
    }

    val trans_df = df.col(format_column).cast("double")

    val max_scale: Double = df.agg(max(trans_df)).head()(0).asInstanceOf[Double]
    val day_scale = 86400
    val hour_scale = 3600
    val min_scale = 60

    val time_map = Map("days" -> day_scale, "hours" -> hour_scale, "minutes" -> min_scale)

    def scaler(df: DataFrame, first_scale: String, second_scale: String, curr_map: Map[String, Int] =  time_map): DataFrame = {
      df.withColumn("first_scale", concat(round(lit(col(format_column)) / curr_map(first_scale), 1).cast("integer"), lit(s" $first_scale")))
        .withColumn("second_scale", concat(round(lit(col(format_column)) % curr_map(first_scale) / curr_map(second_scale), 2), lit(s" $second_scale")))
        .withColumn("readable_duration", concat(col("first_scale"), lit(" "), col("second_scale")))
    }

    def scaled_df(df: DataFrame): DataFrame = {
      max_scale match {
        case max_scale if max_scale / day_scale > 1 => //days)
          scaler(df, "days", "hours")
        case max_scale if max_scale / hour_scale > 1 => //hours
          scaler(df, "hours", "minutes")
        case max_scale if max_scale / min_scale > 1 => //minutes
          scaler(df, "minutes", "seconds")
        case _ => df.withColumn("readable_duration", concat(round(lit(col(format_column)), 1), lit(" seconds")))
      }
    }
    scaled_df(df).drop("first_scale", "second_scale")
  }
}


object BpmFormatDuration extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmFormatDuration(keywords, searchId, utils).transform
  }
}