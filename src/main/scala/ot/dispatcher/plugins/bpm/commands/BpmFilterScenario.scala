package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.PluginUtils
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.bpm.util.Caster
import ot.dispatcher.plugins.small.sdk.ApplyModel


case class BpmFilterScenario(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  /** Command filters values in dataframe according to selected value in pie chart in EVA
   *
   * @param df - should have 2 columns by which pie chart is built
   * @return - dataframe filtered by pie chart segment
   */
  def transform(df: DataFrame): DataFrame = {

    val column = Caster.safeCast[String](
      keywords.get("column"),
      "",
      sendError(searchId, "The value of parameter 'column' should be of String type")
    )
    val filter_column = {
      if (column == "") sendError(searchId, "The value of parameter 'column' should be defined")
      else column
    }
    val value = Caster.safeCast[String](
      keywords.get("values"),
      "",
      sendError(searchId, "The value of parameter 'values' should be of String type")
    )
    val filter_value = {
      if (value == "") sendError(searchId, "The value of parameter 'values' should be defined")
      else value
    }

    val search_token = filter_value.replaceAll("[\\[\\]()]", "").split(",")(0)

    df.filter(col(filter_column).contains(search_token))
  }
}

object BpmFilterScenario extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmFilterScenario(keywords, searchId, utils).transform
  }
}

