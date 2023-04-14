package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import ot.dispatcher.plugins.bpm.util.{BpmParser, Caster}
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils
import org.processmining.log.utils._
import org.processmining.contexts.uitopia._
import org.deckfour.xes.in._
import ot.zeppelin.utlis._
import java.io._

import org.processmining.plugins.heuristicsnet.miner.heuristics.miner.HeuristicsMiner
import org.processmining.plugins.heuristicsnet.miner.heuristics.miner.settings.HeuristicsMinerSettings

case class BpmGetProcess(modelConfig: Option[Config], keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val DEF_DEP_THRESH: Double = 0.7
  val DEF_PARALLEL_THRESH: Double = 0.5
  val DEF_THRESH: Double = 1.0
  val DEF_ALL_CONNECTED: Boolean = false
  val DEF_LONG_DEPENDENCY: Boolean = true
  val bin_size: Int =  modelConfig.get.getInt("binsize")

  val caseIdField: String = "caseId"
  val eventIdField: String = "eventId"
  val timestampField: String = "_time"

  /** Heuristic miner algorithm
   *
   * @param df should have standard event log columns: "caseId", "eventId", "_time"
   * @return dataframe with node-relation infos
   *  heuristics miner main parameters
   *   - dependency_threshold: determines the strength of the direct relationship between two events (causality metric)
   *   - l1/l2_threshold: causality metric for l1/l2 loops
   *   - and_threshold: AND/OR relation metric threshold
   *   - all_connected: adaptive causality metric calculation (if True, there is no need to set dependency_threshold)
   *   - long_distance_dependency: сonsider long relations (length > 2)
   */


  def transform(df: DataFrame): DataFrame = {
    //    /* ===============  heuristics miner main params ======================
    //       dependency_threshold      -- determines the strength of the direct relationship between two events (causality metric)
    //       l1/l2_threshold           -- causality metric for l1/l2 loops
    //       and_threshold             -- AND/OR relation metric threshold
    //       all_connected             -- adaptive causality metric calculation (if True, there is no need to set dependency_threshold)
    //       long_distance_dependency  -- сonsider long relations (length > 2)

    val dep_thresh = Caster.safeCast[Double](
      keywords.get("dependency_threshold"),
      DEF_DEP_THRESH,
      sendError(searchId, "The value of parameter 'dependency_threshold' should be of Double type")
    )
    val l1_thresh = Caster.safeCast[Double](
      keywords.get("l1_threshold"),
      DEF_THRESH,
      sendError(searchId, "The value of parameter 'l1_threshold' should be of Double type")
    )
    val l2_thresh = Caster.safeCast[Double](
      keywords.get("l2_threshold"),
      DEF_THRESH,
      sendError(searchId, "The value of parameter 'l2_threshold' should be of Double type")
    )
    val paral_thresh = Caster.safeCast[Double](
      keywords.get("parallel_threshold"),
      DEF_PARALLEL_THRESH,
      sendError(searchId, "The value of parameter 'parallel_threshold' should be of Double type")
    )
    val use_all_connecter = Caster.safeCast[Boolean](
      keywords.get("all_connected"),
      DEF_ALL_CONNECTED,
      sendError(searchId, "The value of parameter 'all_connected' should be of Boolean type")
    )
    val use_long_dist_dependency = Caster.safeCast[Boolean](
      keywords.get("long_distance_dependency"),
      DEF_LONG_DEPENDENCY,
      sendError(searchId, "The value of parameter 'long_distance_dependency' should be of Boolean type")
    )

    /* windows */
    val wOrder = Window.partitionBy(caseIdField).orderBy(timestampField)
    val wBin = Window.partitionBy("random_bin").orderBy(caseIdField)
//
    /* context */
    val context = new UIContext()
    val pluginContext = context.getMainPluginContext
//
    def getMinerSettings(): HeuristicsMinerSettings = {
      val hSettings = new HeuristicsMinerSettings()

      hSettings.setDependencyThreshold(dep_thresh)
      hSettings.setL1lThreshold(l1_thresh)
      hSettings.setL2lThreshold(l2_thresh)
      hSettings.setAndThreshold(paral_thresh)
      hSettings.setUseAllConnectedHeuristics(use_all_connecter)
      hSettings.setUseLongDistanceDependency(use_long_dist_dependency)
      hSettings
    }

    /* create process eventlog, XES format*/
    val log_df = df.withColumn("timestamp_str", BpmParser.parseTimestamp(col(timestampField)))
      .withColumn("event", array(col(eventIdField), col("timestamp_str")))
      .withColumn("sorted_events", collect_list(col("event")).over(wOrder))
      .groupBy(col(caseIdField))
      .agg(max(col("sorted_events")).as("events"))
      .select(col("*"), round(lit(bin_size)*rand()).as("random_bin").cast("int")) //bin count in plugin.conf params..?
      .withColumn("sorted_bin_traces", collect_list(caseIdField).over(wBin))
      .withColumn("sorted_bin_events", collect_list("events").over(wBin))
      .groupBy("random_bin")
      .agg(max(col("sorted_bin_traces")).as("bin_traces"), max(col("sorted_bin_events")).as("bin_events"))
      .select(BpmParser.createXLogPerBin(col("random_bin"), col("bin_traces"), col("bin_events")).as("binaryLogs"))

      /* parse & merge binary logs */
      val parser = new XesXmlParser()
      val fullLog = XLogHelper.generateNewXLog("FullLog")
      val logs = log_df.collect.map(l => {
        val binary = l.get(0).asInstanceOf[Array[Byte]]
        val bis = new ByteArrayInputStream(binary)
        val log = parser.parse(bis).get(0)
        fullLog.addAll(log)
      })

      /* settings */
      val classifier = XUtils.getDefaultClassifier(fullLog)
      val hSettings = getMinerSettings()
      hSettings.setClassifier(classifier)

      /* mine process */
      val hMiner = new HeuristicsMiner(pluginContext, fullLog, hSettings)
      val heuristicsNet = hMiner.mine()
      val processModelFitness = heuristicsNet.getFitness
      val begin = heuristicsNet.getStartActivities()
      val end = heuristicsNet.getEndActivities()

      val arcsMatrix = heuristicsNet.getArcUsage
      val nodes = heuristicsNet.getActivitiesMappingStructures
        .getActivitiesMapping
        .map(event => { event.getIndex -> event.getId})

      val relations = nodes.map(node => {
        val nodeIdx = node._1
        val nodeName = node._2.replace("+", "")
        val replacedOutput = heuristicsNet.getOutputSet(nodeIdx).toString.replaceAll("\\[|\\]", "")
        val outputSet = if (replacedOutput.isEmpty) Array(nodeName ->"None") else replacedOutput.split(",").map(x => x.toInt).distinct.map(el => {
          val name = nodes.find(_._1==el).get._2.toString.replace("+", "")
          nodeName -> name
        })
        outputSet
      }).flatMap(_.toList)

      val relations_withStat = relations.filter(_._2!="None").map(rel => {
        val first = nodes.find(_._2==rel._1 + "+").get._1
        val second = nodes.find(_._2==rel._2 + "+").get._1
        val arcUsage = arcsMatrix.get(first, second)
        val fire = heuristicsNet.getActivitiesActualFiring()(first)
        (rel._1, rel._2, fire, arcUsage)
      })

      val fakeStart = begin.toString.replaceAll("\\[|\\]", "").split(",").map(startNode => {
        val label = nodes.find(_._1==startNode.toInt).get._2.replace("+", "")
        ("start", label, 0, 0.0)
      })

      val data = fakeStart ++ relations_withStat

      /* result DataFrame */
      val schema = StructType(List(
        StructField("node", StringType, nullable = false),
        StructField("relations", StringType, nullable = false),
        StructField("node_visits", IntegerType, nullable = false),
        StructField("arc_visits", DoubleType, nullable = false)
      ))

      val rdd = spark.sparkContext.makeRDD(data.map(x => Row(x._1, x._2, x._3, x._4)).toSeq)
      val processModel = spark.createDataFrame(rdd, schema).withColumn("fitness", round(lit(processModelFitness), 3))

    processModel
  }
}

object BpmGetProcess extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmGetProcess(modelConfig, keywords, searchId, utils).transform
  }
}
