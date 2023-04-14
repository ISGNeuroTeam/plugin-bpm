package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.bpm.util.Caster
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

case class BpmContains(keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val DEF_TOKEN: String = ""
  val DEF_SEQ: String = ""

  /** For each row in dataframe command searches if value in column 'token' is in corresponding value of column 'sequence'
   *
   * @param df should have at least 2 columns
   * @return dataframe with additional column of boolean type
   * keywords:
   *  - token: indicates column from witch to take value to be searched
   *  - sequence: indicates column where to search
   */

  def transform(df: DataFrame): DataFrame = {

    // Reading and checking parameters
    val token = Caster.safeCast[String](
      keywords.get("token"),
      DEF_TOKEN,
      sendError(searchId, "The value of parameter 'token' should be of String type")
    )
    val sequence = Caster.safeCast[String](
      keywords.get("sequence"),
      DEF_SEQ,
      sendError(searchId, "The value of parameter 'sequence' should be of String type")
    )

    val token_column = {
      if (token == "") sendError(searchId, "The value of parameter 'token' should be defined")
      else token
    }
    val sequence_column = {
      if (sequence == "") sendError(searchId, "The value of parameter 'sequence' should be defined")
      else sequence
    }

    df.withColumn("Contains", array_contains(col(sequence_column), col(token_column)))
  }
}


object BpmContains extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmContains(keywords, searchId, utils).transform
  }
}