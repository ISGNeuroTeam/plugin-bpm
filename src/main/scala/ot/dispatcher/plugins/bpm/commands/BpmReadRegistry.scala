package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import ot.dispatcher.plugins.bpm.util.Caster
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

case class BpmReadRegistry (modelConfig: Option[Config], keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._
  val DEF_REGISTRY_PATH: String = modelConfig.get.getString("registry_path")
  val DEF_FILEPATH: String = ""

  def transform(df: DataFrame): DataFrame = {

    val filename = Caster.safeCast[String](
      keywords.get("file"),
      DEF_FILEPATH,
      sendError(searchId, "The value of parameter 'file' should be of string type")
    )

  val filepath: String = f"/opt/otp/lookups/$filename%s"
//    TODO: Add FileNotFoundException
    val registry_df = spark.read
      .option("header", value = true)
      .option("quote", value = "\"")
      .option("escape", value = "\"")
      .csv(filepath)
    registry_df
  }
}

object BpmReadRegistry extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmReadRegistry(modelConfig, keywords, searchId, utils).transform
  }
}
