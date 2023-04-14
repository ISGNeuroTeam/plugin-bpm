package ot.dispatcher.plugins.bpm.commands

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ot.dispatcher.plugins.bpm.util._
import ot.dispatcher.plugins.small.sdk.ApplyModel
import ot.dispatcher.sdk.PluginUtils

import scala.annotation.tailrec

case class BpmConformance(modelConfig: Option[Config], keywords: Map[String, String], searchId: Int, utils: PluginUtils) {

  import utils._

  val DEF_TRACEFIELD_NAME: String = "traces"
  val DEF_WEIGHT_MISS: Double = 1.0
  val DEF_WEIGHT_EXTRA: Double = 1.0
  val DEF_VERSION: Int = 1
  val DEF_SCHEMA_PATH: String = modelConfig.get.getString("processSchemaPath")
  def DEF_WEIGHTS: String = modelConfig.get.getString("weightspath")


  /** Function reads process schema description from file and returns it as map
   *
   * @param path should be string path to file with schema description
   * @return map of format: node -> relation
   */
  def read_process_schema(path: String):Map[String, Array[String]] = {
    val schema_source = scala.io.Source.fromFile(path)
    val content = schema_source.getLines.map(_.split(","))
    content.next // pass header
    val maps = content.toArray.map(x => x.head -> x.tail)
    val merged = maps.toList.groupBy(_._1).map { case (k, v) =>
      val values = v.flatMap(_._2).toArray
      k -> Array(values: _*)
    }
    val schema_map = collection.immutable.Map(merged.toSeq: _*)
    schema_source.close()
    schema_map
  }

  /** Function reads weights of events in case of their absence from file and returns it as map
   *
   * @param path should be string path to file with weights description
   * @return map of format: event -> weight
   */
  def read_Dict_schema_missed(path: String): Map[String, Double] = {
    val missed_source = scala.io.Source.fromFile(path)
    val content = missed_source.getLines.map(_.split(","))
    val maps = content.map(x => x(0) -> x(1).toDouble)
    val missed_map = collection.immutable.Map(maps.toSeq: _*)
    missed_source.close()
    missed_map
  }

  /** Function reads weights of events in case if there is extra event in trace from file and returns it as map
   *
   * @param path should be string path to file with weights description
   * @return map of format: event -> weight
   */
  def read_Dict_schema_extra(path: String): Map[String, Double] = {
    val extra_source = scala.io.Source.fromFile(path)
    val content = extra_source.getLines.map(_.split(","))
    val maps = content.map(x => x(0) -> x(2).toDouble)
    val extra_map = collection.immutable.Map(maps.toSeq: _*)
    extra_source.close()
    extra_map
  }

  /** For each row in dataframe command searches if value in column 'token' is in corresponding value of column 'sequence'
   *
   * @param df should have at least 1 column with trace description
   * @return dataframe with conformance metric for each case
   * keywords:
   *  - trace_field: name of column with traces
   *  - wm: default weight for missing events
   *  - we: default weight for extra events
   *  - version: choice of version of conformance algorithn
   *  - schema_file: path to file with process schema description (if omitted - default schema from config will be used)
   *  - weights_file: path to file with weights description
   */
  def transform(df: DataFrame): DataFrame = {

    val traceField = Caster.safeCast[String](
      keywords.get("trace_field"),
      DEF_TRACEFIELD_NAME,
      sendError(searchId, "The value of parameter 'trace_field' should be of string type")
    )
    val missed_w = Caster.safeCast[Double](
      keywords.get("wm"),
      DEF_WEIGHT_MISS,
      sendError(searchId, "The value of parameter 'weight missed' ('wm') should be of double type")
    )
    val extra_w = Caster.safeCast[Double](
      keywords.get("we"),
      DEF_WEIGHT_EXTRA,
      sendError(searchId, "The value of parameter 'weight extra' ('we') should be of double type")
    )
    val version = Caster.safeCast[Int](
      keywords.get("version"),
      DEF_VERSION,
      sendError(searchId, "The value of parameter 'version' should be of int type")
    )
    val schemapath = Caster.safeCast[String](
      keywords.get("schema_file"),
      DEF_SCHEMA_PATH,
      sendError(searchId, "The value of parameter 'schema_file' should be of string type")
    )
    val weights = Caster.safeCast[String](
      keywords.get("weights_file"),
      DEF_WEIGHTS,
      sendError(searchId, "The value of parameter 'weights_file' should be of string type")
    )

    val okMsg = "Процесс соответствует эталонной схеме"

    def path(cur: String, graph1: Map[String, Array[String]], Dm: Map[String, Double]): Map[String, Array[String]] = {

      val maxi = Double.MaxValue
      val keys = graph1.keySet.toArray
      val sz = keys.length
      val values = Array.fill(sz)(maxi)
      val map1 = (keys zip values).toMap
      val map2 = map1 - cur
      val distt = map2 + (cur -> 0.0)
      val prev1 = Array.fill(sz)("")
      val prevv = (keys zip prev1).toMap
      val used1 = Array.fill(sz)(-1)
      val used = (keys zip used1).toMap
      //      val mindist = 0.0
      //      val vertex = cur

      @tailrec
      def tailrec(vertex: String, dist1: Map[String, Double], min_dist: Double, used1: Map[String, Int],
                  prev1: Map[String, String]): (Map[String, Double], Map[String, String]) = {
        if (min_dist >= maxi) {
          return (dist1, prev1)
        }
        val i = vertex
        val wi = graph1(i)
        val used2 = used1 - i
        val used = used2 + (i -> 1)

        @tailrec
        def loop1(i1: String, arr: Array[String], j: Int, dist: Map[String, Double],
                  prev: Map[String, String]): (Map[String, Double], Map[String, String]) = {
          if (j == arr.length) {
            return (dist, prev)
          }
          if ((dist(i1) + Dm(arr(j))) < dist(arr(j))) {
            val mm = dist(i1) + Dm(arr(j))
            val dist1 = dist - arr(j)
            val dist2 = dist1 + (arr(j) -> mm)
            val prev1 = prev - arr(j)
            val prev2 = prev1 + (arr(j) -> i1)
            loop1(i1, arr, j + 1, dist2, prev2)
          }
          else {
            loop1(i1, arr, j + 1, dist, prev)
          }
        }

        val (a1, a2) = loop1(i, wi, 0, dist1, prev1)
        val dist_mod = a1.filter(t => used(t._1) == -1)
        if (dist_mod.isEmpty) {
          (dist1, prev1)
        }
        else {
          val d = dist_mod.minBy(_._2)
          val d1 = d._1
          val d2 = d._2
          tailrec(d1, a1, d2, used, a2)
        }
      }

      val tailr = tailrec(cur, distt, 0, used, prevv)
      val distres = tailr._1
      val prevres = tailr._2
      val path = Array.empty[String]

      @tailrec
      def outerloop(Map1: Map[String, Array[String]], keyarr: Array[String], indicator: Int): Map[String, Array[String]] = {
        if (indicator == keyarr.length) {
          Map1
        } else {
          val curelem = keyarr(indicator)

          @tailrec
          def loop2(pat: Array[String], jj: String, prev: Map[String, String]): Array[String] = {
            if (jj == "") {
              pat
            } else {
              val pat2: Array[String] = pat ++ Array(jj)
              val jj1 = prev(jj)
              loop2(pat2, jj1, prev)
            }
          }

          if (distres(curelem) == maxi) {
            val newmap = Map1 + (curelem -> Array("-1"))
            outerloop(newmap, keyarr, indicator + 1)
          } else if (cur == curelem) {
            val newmap = Map1 + (curelem -> Array("-1"))
            outerloop(newmap, keyarr, indicator + 1)
          } else {
            val patres = loop2(path, prevres(curelem), prevres).reverse
            val newmap = Map1 + (curelem -> patres)
            outerloop(newmap, keyarr, indicator + 1)
          }
        }
      }

      val res = outerloop(Map.empty[String, Array[String]], keys, 0)
      res
    }

    @tailrec
    def precount(Map1: Map[String, Map[String, Array[String]]], graph1: Map[String, Array[String]],
                 keys1: Array[String], i: Int, Dm: Map[String, Double]): Map[String, Map[String, Array[String]]] = {
      if (i == keys1.length) {
        Map1
      } else {
        val res1 = path(keys1(i), graph1, Dm)
        val Map2 = Map1 + (keys1(i) -> res1)
        precount(Map2, graph1, keys1, i + 1, Dm)
      }
    }

    val schema = read_process_schema(schemapath)
    val missedarray = read_Dict_schema_missed(weights)
    val extraarray = read_Dict_schema_extra(weights)
    val keys = schema.keySet.toArray

    //path to spark Dataframe
    val patpatt = precount(Map.empty[String, Map[String, Array[String]]], schema, keys, 0, missedarray)
    val df_path = patpatt.map{ case(k,v) => Seq(Seq(k)) ++ v.map{ case(a,b) => Seq(a) ++ b}.toSeq}.toSeq
    val df_missed = missedarray.map{case(a,b) => Seq(a) ++ Seq(b.toString)}.toSeq
    val df_extra = extraarray.map{case(a,b) => Seq(a) ++ Seq(b.toString)}.toSeq
    val df_schema = schema.map{case(a,b) => Seq(a) ++ b}.toSeq
    val keys1 = keys.toSeq

    df.withColumn("wm", typedLit(missed_w))
      .withColumn("version", typedLit(version))
      .withColumn("keys", typedLit(keys1))
      .withColumn("dict1", typedLit(df_missed))
      .withColumn("dict2", typedLit(df_extra))
      .withColumn("pschema", typedLit(df_schema))
      .withColumn("we", typedLit(extra_w))
      .withColumn("path", typedLit(df_path))
      .withColumn("conformance", BpmParser.check(col(traceField), col("pschema"), col("dict1"),
        col("dict2"), col("wm"), col("we"), col("version"),
        col("path"), col("keys")))
      .withColumn("trace", col(traceField))
      .withColumn("metric", round(col("conformance")("_1"), 2))
      .withColumn("recomm_sc", col("conformance")("_2"))
      .withColumn("topology", col("conformance")("_3"))
      .withColumn("not_in_dict", col("conformance")("_4"))
      .select(col("trace"), col("metric"), col("recomm_sc"), col("topology"), col("not_in_dict"))
  }
}

object BpmConformance extends ApplyModel {

  override def apply(modelName: String, modelConfig: Option[Config], searchId: Int, featureCols: List[String],
                     targetName: Option[String], keywords: Map[String, String], utils: PluginUtils): DataFrame => DataFrame = {
    BpmConformance(modelConfig, keywords, searchId, utils).transform
  }
}