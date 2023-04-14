package ot.dispatcher.plugins.bpm.util
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.text.SimpleDateFormat
import java.util.Date
import scala.annotation.tailrec
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.Map
import scala.collection.immutable.Map
import org.apache.spark.sql.functions.udf
//import org.processmining.log.utils._
//import org.processmining.plugins._
//import org.deckfour.xes.model._
//import org.deckfour.xes.extension.std._
//import org.deckfour.xes.factory._
import org.deckfour.xes.out._
//import org.deckfour.xes.in._
import java.io._
import ot.zeppelin.utlis._
import org.apache.spark.sql.DataFrame
import java.math.RoundingMode
import java.text.DecimalFormat
import java.util
import scala.collection.JavaConversions._


class BpmParser extends Serializable {

  def longToDateString(timestamp: Long): String = {
    val dateObj = new Date(timestamp * 1000)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    dateFormat.format(dateObj)
  }

  def splitRelation(rel: Seq[Seq[String]]) = {
    val size = rel(0).size
    val nodes = rel.map(r => r(0))
    val arcVisits = rel.map(r => r(1))
    size match {
      case 2 => Array(nodes, arcVisits)
      case 4 => {
        val metrics = rel.map(r => r(2))
        val thresholds = rel.map(r => r(3))
        Array(nodes, arcVisits, metrics, thresholds)
      }
    }
  }

  def createBinaryXLogs(bin: Int, traceIds: Seq[String], events: Seq[Seq[Seq[String]]]): Array[Byte] = {
    val logName = "log_" + bin.toString
    val localLog = XLogHelper.generateNewXLog(logName)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val trWithEvents = traceIds.zip(events).map(tuple => {

      val (trId, trEvents) = tuple
      val trace = XLogHelper.insertTrace(localLog, trId)

      trEvents.map(ev => {
        val eventName = ev(0)
        val eventStart = dateFormat.parse(ev(1))
        XLogHelper.insertEvent(trace, eventName, eventStart)
      })
    })

    val serializer = new XesXmlSerializer()
    val outputStream = new ByteArrayOutputStream()
    serializer.serialize(localLog, outputStream)
    outputStream.close()
    val serializedLog = outputStream.toByteArray()
    serializedLog
  }

  def getCycles(input: Seq[String]): (List[Int], List[Int], List[List[String]]) = {

    val subseqs = input.inits.flatMap(_.tails.toList.init).map(_.toList).toList.distinct

    @tailrec
    def getSeqIdxs(in: List[String], s: List[String], acc: List[Int]): List[Int] = {
      val added = in.lastIndexOfSlice(s)
      if (added == -1) acc
      else getSeqIdxs(in.slice(0, added), s, added :: acc)
    }

    @tailrec
    def getCycleOccurences(idxs: List[Int], head: Int, s: List[String], accIdxs: List[Int], accSize: List[Int]): List[(Int, Int, List[String])] = {
      if (idxs.isEmpty) accIdxs.zip(accSize).zip(List.fill(accIdxs.size)(s)).map({ case ((a, b), c) => (a, b, c) })
      else if (idxs.head - head == s.size && head != -1) getCycleOccurences(idxs.tail, idxs.head, s, accIdxs, (accSize.head + 1) :: accSize.tail)
      else getCycleOccurences(idxs.tail, idxs.head, s, idxs.head :: accIdxs, 1 :: accSize)
    }

    val candidates = subseqs.map(x => {
      val idxs = getSeqIdxs(input.toList, x, Nil)
      val occ = getCycleOccurences(idxs, -1, x, Nil, Nil)
      occ.filterNot(x => x._2.equals(1))
    }).filterNot(_.isEmpty).flatten

    //candidates
    val res = candidates.groupBy(_._3).mapValues(_.map(_._2).sum).toList
      .map(x => (x._1.distinct.sorted, x._1, x._2)).sortBy(_._3)
      .groupBy(_._1).mapValues(_.map(_._2).last)
      .values.toList

    val finals = candidates.filterNot(x => res.indexOf(x._3).equals(-1)).sortBy(-_._1) //sorted by occurence in trace asc

    val idxs = finals.map(_._1)
    val cnts = finals.map(_._2)
    val cycs = finals.map(_._3)

    (idxs, cnts, cycs)
  }

  def removeCycles(input: Seq[String], cycs: Seq[Seq[String]], idxs: Seq[Int], cnts: Seq[Int]) = {

    @tailrec
    def clean(in: List[String], cyc: List[List[String]], idxs: List[Int], counts: List[Int]): List[String] = {
      def removeAt(input: List[String], cyc: List[String], cycIdx: Int, cycCount: Int): List[String] =
        input.take(cycIdx) ::: input.drop(cycIdx + cyc.size * (cycCount - 1))

      if (idxs.isEmpty) in
      else clean(removeAt(in, cyc.head, idxs.head, counts.head), cyc.tail, idxs.tail, counts.tail)
    }

    clean(input.toList, cycs.map(_.toList).toList, idxs.toList, cnts.toList)
  }

  def zipCols(traces: Seq[String], times: Seq[Double]): Seq[(String, Double)] = traces.zip(times)

  def color(number: Long, max_th: Long): Int = {
    val thresholds = Array(0.25 * max_th, 0.5 * max_th, 0.75 * max_th, max_th)
    val th = thresholds.filter(_ >= number).head
    thresholds.indexOf(th) + 1
  }

  def checkconformance(traceAB: Seq[String], schema1: Seq[Seq[String]], missedarray1: Seq[Seq[String]], extraarray1: Seq[Seq[String]], wm: Double, we: Double, version: Int, patpat1: Seq[Seq[Seq[String]]], keysAB: Seq[String]): (Double, String, String, String) = {
    val trace = traceAB.toArray
    val keys = keysAB.toArray
    val missedarray = missedarray1.map(x => (x(0) -> x(1).toDouble)).toMap
    val extraarray = extraarray1.map(x => (x(0) -> x(1).toDouble)).toMap
    val schema = schema1.map(x => (x.head -> x.tail.toArray)).toMap
    val patpat = patpat1.map(x => (x.head.head -> x.tail.toArray.map(y => (y.head -> y.tail.toArray)).toMap)).toMap

    def countw(innerd: Map[String, Double], arr: Array[String], keys1: Array[String]): Double = {
      var metr: Double = 0.0
      for (elem <- arr) {
        if (keys1 contains (elem)) {
          metr += innerd(elem)
        }
        else {
          metr += innerd("not_found")
        }
      }
      return (metr)
    }

    def norm(a: Array[String]): Array[String] = {
      if (a.size >= 1) {
        return a.slice(1, a.size)
      }
      else {
        return (a)
      }
    }
    case class result1(error: String, missed: Array[String], extra: Array[String], werr: Double, position: Int, position2: Int) {
      override def toString() = {
        "errtype : пропуск + лишняя " + ", missed : " + missed.mkString(", ") + ", extra : " + extra.mkString(", ") + ", weight : " + werr + ", position : " + position2 + ", posfin : " + position
      }
    }

    def undefinedmod(i: Int, trace: Array[String], graph1: Map[String, Array[String]], patpat1: Map[String, Map[String, Array[String]]], Dm: Map[String, Double], De: Map[String, Double], wm: Double, we: Double, keys1: Array[String]): result1 = {
      val trace2 = trace :+ "finish"
      val elem = i match {
        case -1 => "start"
        case _ => trace(i)
      }
      if (!(keys1 contains (elem))) {
        return (result1("-1", Array.empty, Array.empty, -1, -1, -1))
      }
      else {
        val st = i + 1
        val indexes = (st to trace2.size - 1).toArray
        val trace3 = trace2.slice(st, trace2.size)
        val trac = (indexes zip trace3)
        val trac2 = trac.filter { case (a: Int, b: String) => keys1 contains (b) }
        val maxi = Int.MaxValue
        val trac3 = trac2.filter { case (a: Int, b: String) => (patpat1(elem)(b)(0) != "-1") }
        if (trac3.isEmpty) {
          val res = result1("пропуск + лишняя", Array.empty, Array.empty, -1, -1, -1)
          return (res)
        }
        else {
          def f(x: Int, y: String): Double = wm * countw(Dm, norm(patpat1(elem)(y)), keys1) + we * countw(De, trace2.slice(i + 1, x), keys1)
          val trac4 = trac3.map { case (x: Int, y: String) => f(x, y) }
          val trac5 = trac3 zip trac4
          val minby = trac5.minBy(_._2)
          val j = minby._1._1
          val err = minby._2
          val el = minby._1._2
          val nr = norm(patpat1(elem)(el))
          val res = result1("пропуск + лишняя", nr, trace2.slice(i + 1, j), err, j, i)
          return (res)
        }
      }
    }

    //блок лишняя + смена ветки
    def similar(arrpat: Array[String], arrpatpos: Array[Int], arr2: Array[String]): Iterable[(String, Int)] = {
      val arr1 = arrpat.zip(arrpatpos)
      val arr = arr1.filter({ case (a: String, b: Int) => arr2 contains (a) })
      return (arr)
    }
    def path(cur: String, graph1: Map[String, Array[String]], Dm: Map[String, Double]): Map[String, Array[String]] = {
      val maxi = Double.MaxValue
      val keys = graph1.keySet.toArray
      val sz = keys.size
      val values = Array.fill(sz)(maxi)
      val map1 = (keys zip values).toMap
      val map2 = map1 - cur
      val distt = map2 + (cur -> 0.0)
      val prev1 = Array.fill(sz)("")
      val prevv = (keys zip prev1).toMap
      val used1 = Array.fill(sz)(-1)
      val used = (keys zip used1).toMap
      val mindist = 0.0
      val vertex = cur

      def tailrec(vertex: String, dist1: Map[String, Double], min_dist: Double, used1: Map[String, Int], prev1: Map[String, String]): (Map[String, Double], Map[String, String]) = {
        if (min_dist >= maxi) {
          return (dist1, prev1)
        }
        val i = vertex
        val wi = graph1(i)
        val used2 = used1 - i
        val used = used2 + (i -> 1)

        def loop1(i1: String, arr: Array[String], j: Int, dist: Map[String, Double], prev: Map[String, String]): (Map[String, Double], Map[String, String]) = {
          if (j == arr.size) {
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
        val min_d = maxi
        val dist_mod = a1.filter((t) => used(t._1) == -1)
        if (dist_mod.isEmpty) {
          return (dist1, prev1)
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

      def outerloop(Map1: Map[String, Array[String]], keyarr: Array[String], indicator: Int): Map[String, Array[String]] = {
        if (indicator == keyarr.size) {
          return Map1
        }
        else {
          val curelem = keyarr(indicator)
          def loop2(pat: Array[String], jj: String, prev: Map[String, String]): Array[String] = {
            if (jj == "") {
              return (pat)
            } else {
              val pat2: Array[String] = pat ++ Array(jj)
              val jj1 = prev(jj)
              loop2(pat2, jj1, prev)
            }
          }
          if (distres(curelem) == maxi) {
            val newmap = Map1 + (curelem -> Array("-1"))
            outerloop(newmap, keyarr, indicator + 1)
          }
          else if (cur == curelem) {
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
      return (res)
    }

    def delete(A: Array[String], B: Array[String]): Array[(Int, String)] = {
      val poss = (0 to A.size - 1).toArray
      val posA = poss zip A
      val A1 = A.toSeq
      val k = B.map { case (y: String) => A1.lastIndexOf(y) }
      val m = posA.filter { case (x: Int, y: String) => !(k contains (x)) }
      return(m)
    }

    case class gd(scenario: Array[Any], scenario2: Array[String], scenario2pos: Array[Int], err: Array[Map[String,Any]], prop: Double, metric2: Double, sch: Array[String], extra_l: Int)

    case class result2(errtype: String, retur: Array[String], elemm: String, razvetvl: String, posrazv: Int, position: Int, posfin: Int, status: String, weighterr: Double, vetv: Array[String], sim: Array[String], simpos: Array[Int], posel: Int, missed: Array[String], extra: Array[String], prop: Double, extra_l: Int, missed_pos: Array[Int], extra_pos: Array[Int]) {
      override def toString() = {
        "errtype : откат" + ", missed : " + missed.mkString(", ") + ", extra : " + extra.mkString(", ") + ", position : " + position + ", posrazv : " + posrazv + ", posfin : " + posfin + ", weight : " + (missed.size + extra.size)
      }
    }

    case class scen(scenario: Array[Any], scenario2: Array[String], scenario2pos: Array[Int], err: Array[Map[String, Any]], prop: Double, metric2: Double, globdict: Map[Int, gd], sch: Array[String], extra_l: Int)


    def u1(Dict1: Map[String, Array[String]], trace: Array[String], j: Int, j2: Int, gld: Map[Int, gd], pat2: Array[String], patpos2: Array[Int], i: Int, a1: String, a0: String, Dm: Map[String, Double], De: Map[String, Double], patpat: Map[String, Map[String, Array[String]]], wm: Double, we: Double, keys: Array[String]): result2 = {
      val flag = 0
      val elem = pat2(i)
      val poselem = patpos2(i)
      if (!(keys contains (a1))) {
        return result2("-1", Array("-1"), "-1", "-1", -1, -1, -1, "-1", -1, Array("-1"), Array("-1"), Array(-1), -1, Array("-1"), Array("-1"), -1, -1, Array(-1), Array(-1))
      }
      else if (patpat(elem)(a1)(0) == "-1") {
        return result2("-1", Array("-1"), "-1", "-1", -1, -1, -1, "-1", -1, Array("-1"), Array("-1"), Array(-1), -1, Array("-1"), Array("-1"), -1, -1, Array(-1), Array(-1))
      }
      else {
        val patt = norm(patpat(elem)(a1))
        val dictres = gld(poselem)
        val arrp = (poselem to j2 - 1).toArray
        val simp = similar(trace.slice(poselem, j2), arrp, patt).unzip
        val sim = simp._1.toSet.toArray
        val simpos = simp._2.toArray
        val err0 = dictres.metric2
        val prop0 = dictres.prop
        val extra_c0 = dictres.extra_l
        val patts = delete(patt, sim)
        val deltr = delete(trace.slice(poselem + 1, j2), sim)
        val patts1 = patts.unzip._1.map(x => x + 1 + poselem)
        val deltr1 = deltr.unzip._1.map(x => x + 1 + poselem)
        val good = we * countw(De, trace.slice(poselem + 1, j2), keys) - we * countw(De, sim, keys)
        val propgood = wm * countw(Dm,patts.unzip._2, keys)
        val err = good + propgood + err0
        val prop = prop0 + propgood
        val extra_c1 = extra_c0 + delete(trace.slice(poselem + 1, j2), sim).unzip._2.size
        val map0 = result2("смена ветки", pat2.slice(0, i), trace(j), elem, poselem, j, j2, "ошибка", err, patt, sim, simpos, poselem, patts.unzip._2, deltr.unzip._2, prop, extra_c1, patts1, deltr1)
        return (map0)
      }
    }

    def u2(pat: Array[String], patpos: Array[Int], Dict1: Map[String, Array[String]], trace: Array[String], j: Int, j2: Int, gld: Map[Int, gd], Dm: Map[String, Double], De: Map[String, Double], patpat: Map[String, Map[String, Array[String]]], wm: Double, we: Double, keys: Array[String]): result2 = {
      val a0 = j match {
        case -1 => "start"
        case _ => trace(j)
      }
      val a1 = trace(j2)
      val pat2 = pat.reverse.tail
      val patpos2 = patpos.reverse.tail
      val patp = (0 to pat2.size - 1).toArray
      val res4 = patp.map { case (y: Int) => u1(Dict1, trace, j, j2, gld, pat2, patpos2, y, a1, a0, Dm, De, patpat, wm, we, keys) }
      val res5 = res4.filter { case (x: result2) => x.errtype != "-1" }
      if (res5.nonEmpty) {
        val res6 = res5.minBy(elem0 => elem0.weighterr)
        return (res6)
      } else {
        return result2("-1", Array("-1"), "-1", "-1", -1, -1, -1, "-1", -1, Array("-1"), Array("-1"), Array(-1), -1, Array("-1"), Array("-1"), -1, -1, Array(-1), Array(-1))
      }
    }

    def u3(pat: Array[String], patpos: Array[Int], dict1: Map[String, Array[String]], trace3: Array[String], j: Int, gld: Map[Int, gd], Dm: Map[String, Double], De: Map[String, Double], patpat: Map[String, Map[String, Array[String]]], wm: Double, we: Double, keys: Array[String]): result2 = {
      val trace = trace3 :+ "finish"
      if (pat.size == 0) {
        return result2("-1", Array("-1"), "-1", "-1", -1, -1, -1, "-1", -1, Array("-1"), Array("-1"), Array(-1), -1, Array("-1"), Array("-1"), -1, -1, Array(-1), Array(-1))
      }
      else {
        val patp = (j + 1 to trace.size - 1).toArray
        val res4 = patp.map { case (y: Int) => u2(pat, patpos, dict1, trace, j, y, gld, Dm, De, patpat, wm, we, keys) }
        val res5 = res4.filter { case (x: result2) => x.errtype != "-1" }
        if (res5.nonEmpty) {
          val res6 = res5.minBy(elem0 => elem0.weighterr)
          return res6
        }
        else {
          return result2("-1", Array("-1"), "-1", "-1", -1, -1, -1, "-1", -1, Array("-1"), Array("-1"), Array(-1), -1, Array("-1"), Array("-1"), -1, -1, Array(-1), Array(-1))
        }
      }
    }

    var que: ArrayBuffer[Int] = ArrayBuffer.empty
    var sque: ArrayBuffer[String] = ArrayBuffer.empty
    var qque: ArrayBuffer[Int] = ArrayBuffer.empty
    val egd = Map(0 -> gd(Array.empty[Any], Array.empty[String], Array.empty[Int], Array.empty[Map[String, Any]], 0, 0, Array.empty[String], 0))
    var arrsc: ArrayBuffer[scen] = ArrayBuffer(scen(Array.empty[Any], Array.empty[String], Array.empty[Int], Array.empty[Map[String, Any]], 0, 0, egd, Array.empty[String], 0))

    def mainf(trace: Array[String], pos1: Int, pos2: Int, prev_elem: String, prev_pos: Int, Dict1: Map[String, Array[String]], patpat: Map[String, Map[String, Array[String]]], missed_weights: Map[String, Double], extra_weights: Map[String, Double], wm: Double, we: Double, keys: Array[String]): Int = {
      val curcur = arrsc(pos2)
      val scenarioA = curcur.scenario
      val scenario2A = curcur.scenario2
      val scenario2posA = curcur.scenario2pos
      val metric2A = curcur.metric2
      val schA = curcur.sch
      val globdictA = curcur.globdict
      val errA = curcur.err
      val propA = curcur.prop
      val extralA = curcur.extra_l
      if (pos1 >= trace.size) {
        val ll = norm(patpat("start")("finish"))
        if (prev_elem == "finish") {
          return (-1)
        }
        else if (!(keys contains (prev_elem))) {
          val errw = wm * countw(missed_weights, ll, keys)
          val pos11 = pos1 - 1
          val errB = errA :+ Map("errtype" -> "некорректный конец", "Вес ошибки" -> errw, "missed" -> ll.mkString(", "), "position" -> pos11, "статус" -> "fault", "weightmissed" -> errw, "weightextra" -> 0)
          val charr = arrsc(pos2).copy(metric2 = metric2A + errw, prop = propA + errw, scenario = scenarioA :+ "errfin", err = errB, sch = schA ++ ll)
          arrsc(pos2) = charr
          return (-1)
        }
        else if (!(Dict1(prev_elem) contains "finish")) {
          val s2 = patpat(prev_elem)("finish")
          val errw1 = wm * countw(missed_weights, norm(s2), keys)
          val errw2 = wm * countw(missed_weights, ll, keys)
          val scenarioB = scenarioA :+ "errfin"
          val pos11 = pos1 - 1
          val charr = s2(0) match {
            case "-1" => arrsc(pos2).copy(err = errA :+ Map("weightmissed" -> errw2, "weightextra" -> 0, "errtype" -> "некорректный конец", "Вес ошибки" -> errw2, "missed" -> ll.mkString(", "), "position" -> pos11, "статус" -> "fault"), metric2 = metric2A + errw2, scenario = scenarioB, prop = propA + errw2, sch = schA ++ ll)
            case _ => arrsc(pos2).copy(err = errA :+ Map("weightmissed" -> errw1,"weightextra" -> 0, "errtype" -> "некорректный конец", "Вес ошибки" -> errw1, "missed" -> norm(s2).mkString(", "), "position" -> pos11, "статус" -> "fault"), metric2 = metric2A + errw1, scenario = scenarioB :+ s2, sch = schA ++ norm(s2), prop = propA + errw1)
          }
          arrsc(pos2) = charr
          return (-1)
        }
        else {
          return (-1)
        }
      } else {
        val elem = trace(pos1)
        val reached = Dict1(prev_elem)
        val glcur = gd(scenarioA, scenario2A, scenario2posA, errA, propA, metric2A, schA, extralA)
        val globDictC = globdictA + (prev_pos -> glcur)
        if (reached contains (elem)) {
          val scenarioB = scenarioA :+ Array(elem, pos1)
          val schB = schA :+ elem
          val scenario2B = scenario2A :+ elem
          val scenario2posB = scenario2posA :+ pos1
          val errB = errA :+ Map(elem -> "OK", "статус" -> "good")
          val charr = arrsc(pos2).copy(scenario = scenarioB, err = errB, globdict = globDictC, scenario2 = scenario2B, scenario2pos = scenario2posB, sch = schB)
          arrsc(pos2) = charr
          mainf(trace, pos1 + 1, pos2, elem, pos1, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
        }
        else {
          val s1 = undefinedmod(pos1 - 1, trace, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
          val s2 = u3(scenario2A, scenario2posA, Dict1, trace, pos1 - 1, globDictC, missed_weights, extra_weights, patpat, wm, we, keys)
          val posit = s1.position
          val metric2B = metric2A + s1.werr
          val weightmissed0 = wm * countw(missed_weights, s1.missed, keys)
          val weightextra0 =  we * countw(extra_weights, s1.extra, keys)
          val propB = propA + weightmissed0
          val extralB = extralA + s1.extra.size
          val errB = errA :+ Map("weightmissed" -> weightmissed0,"weightextra" -> weightextra0, "статус" -> "fault", "errtype" -> s1.error, "position" -> s1.position2, "posfin" -> s1.position, "extra" -> s1.extra.mkString(", "), "missed" -> s1.missed.mkString(", "))
          val schB = schA ++ s1.missed
          //обработка 1 случая
          if ((s1.error != "-1") && (s2.errtype == "-1")) {
            val scenarioB = scenarioA :+ "пропуск + лишняя"
            if (posit <= trace.size - 1) {
              val elem1 = trace(posit)
              val errC = errB :+ Map(elem1 -> "OK", "статус" -> "good")
              val scenarioC = scenarioB :+ elem1
              val scenario2C = scenario2A :+ elem1
              val schC = schB :+ elem1
              val scenario2posC = scenario2posA :+ posit
              val glcur = gd(scenarioC, scenario2C, scenario2posC, errC, propB, metric2B, schC, extralB)
              val globDictE = globDictC + (posit -> glcur)
              val charr = arrsc(pos2).copy(scenario = scenarioC, err = errC, globdict = globDictE, scenario2 = scenario2C, scenario2pos = scenario2posC, metric2 = metric2B, prop = propB, sch = schC, extra_l = extralB)
              arrsc(pos2) = charr
              mainf(trace, posit + 1, pos2, elem1, posit, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
            }
            else {
              val charr = arrsc(pos2).copy(scenario = scenarioB, err = errB, globdict = globdictA, scenario2 = scenario2A, scenario2pos = scenario2posA, metric2 = metric2B, prop = propB, sch = schB, extra_l = extralB)
              arrsc(pos2) = charr
              mainf(trace, posit + 1, pos2, "finish", trace.size, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
            }
          }
          //2 случай
          else {
            var n1 = arrsc(pos2)
            var n = n1.copy()
            arrsc += n
            val rasv = s2.position
            val scenarioB = scenarioA ++ Array("пропуск + лишняя", Array(pos1, posit))
            val posendd = s2.posfin
            val Szz = trace.size
            val elem0 = posendd match {
              case Szz => "finish"
              case _ => trace(posendd)
            }
            val razv = s2.razvetvl
            val posr = s2.posrazv
            val scenario10 = globDictC(posr).scenario ++ Array("начало ошибки, откат из", elem.toString, "до", razv, "пропуск + лишняя 2", Array(pos1, razv, posendd))
            val scenario11 = scenario10 ++ s2.sim
            val scenario12 = scenario11 :+ Array(elem0, posendd)
            val scenario20 = globDictC(posr).scenario2
            val sch0 = globDictC(posr).sch
            val scenario2pos0 = globDictC(posr).scenario2pos
            val err0 = globDictC(posr).err
            val curerr = s2.sim.map { case (x: String) => Map(x -> "OK", "статус" -> "good") }
            val err1 = err0 ++ curerr
            val good = s2.extra
            val propgood = s2.missed
            val sch1 = sch0 ++ s2.vetv
            val scenario21 = posendd match {
              case Szz => scenario20
              case _ => scenario20 :+ elem0
            }
            val scenario2pos1 = posendd match {
              case Szz => scenario2pos0
              case _ => scenario2pos0 :+ posendd
            }
            val sch2 = posendd match {
              case Szz => sch1
              case _ => sch1 :+ elem0
            }
            //val misp = s2.missed_pos.map{x => x+1}
            val misp = s2.missed_pos
            val exp = s2.extra_pos.map{x => x+1}
            val scenario13 = scenario12 :+ "конец ошибки"
            val weightmissed0 = wm * countw(missed_weights, s2.missed, keys)
            val weightextra0 = we * countw(extra_weights, s2.extra, keys)
            val err25 = err1 :+ Map("статус" -> "fault", "errtype" -> s2.errtype, "position" -> s2.posrazv, "positionf" -> s2.position, "posfin" -> s2.posfin, "missed" -> s2.missed.mkString(", "), "extra" -> s2.extra.mkString(", "), "missed_pos" -> misp.mkString(", "), "extra_pos" -> exp.mkString(", "), "weighterr" -> s2.weighterr, "weightextra" -> weightextra0, "weightmissed" -> weightmissed0)
            val err2 = err25 :+ Map(elem0 -> "OK", "статус" -> "good")
            val metric20 = s2.weighterr
            val prop0 = s2.prop
            val extral0 = s2.extra_l
            val glcur2 = gd(scenario13, scenario21, scenario2pos1, err2, prop0, metric20, sch2, extral0)
            val globDictD = globDictC + (posendd -> glcur2)
            que += arrsc.size - 1
            sque += elem0
            qque += s2.posfin
            val charr1 = arrsc(pos2).copy(scenario = scenario13, err = err2, globdict = globDictD, scenario2 = scenario21, scenario2pos = scenario2pos1, metric2 = metric20, prop = prop0, sch = sch2, extra_l = extral0)
            arrsc(arrsc.size - 1) = charr1
            if (posit <= trace.length - 1) {
              val elem1 = trace(posit)
              val errC = errB :+ Map(elem1 -> "OK", "статус" -> "good")
              val scenarioC = scenarioB ++ Array(elem1, posit)
              val scenario2C = scenario2A :+ elem1
              val scenario2posC = scenario2posA :+ posit
              val schC = schB :+ elem1
              val glcur = gd(scenarioC, scenario2C, scenario2posC, errC, propB, metric2B, schC, extralB)
              val globDictE = globDictC + (posit -> glcur)
              val charr = arrsc(pos2).copy(scenario = scenarioC, err = errC, globdict = globDictE, scenario2 = scenario2C, scenario2pos = scenario2posC, metric2 = metric2B, prop = propB, sch = schC, extra_l = extralB)
              arrsc(pos2) = charr
              mainf(trace, posit + 1, pos2, elem1, posit, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
            }
            else {
              val charr = arrsc(pos2).copy(scenario = scenarioB, err = errB, globdict = globDictC, scenario2 = scenario2A, scenario2pos = scenario2posA, metric2 = metric2B, prop = propB, sch = schB, extra_l = extralB)
              arrsc(pos2) = charr
              mainf(trace, trace.size, pos2, "finish", trace.size, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
            }
            val q1 = que(que.size - 1)
            val s1 = sque(sque.size - 1)
            val qq1 = qque(qque.size - 1)
            que.remove(que.size - 1)
            sque.remove(sque.size - 1)
            qque.remove(qque.size - 1)
            mainf(trace, qq1 + 1, q1, s1, qq1, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
          }
        }
      }
    }
    def mainf2(trace: Array[String], pos1: Int, pos2: Int, prev_elem: String, prev_pos: Int, Dict1: Map[String, Array[String]], patpat: Map[String, Map[String, Array[String]]], missed_weights: Map[String, Double], extra_weights: Map[String, Double], wm: Double, we: Double, keys: Array[String]): Int = {
      val curcur = arrsc(pos2)
      val scenarioA = curcur.scenario
      val scenario2A = curcur.scenario2
      val scenario2posA = curcur.scenario2pos
      val metric2A = curcur.metric2
      val schA = curcur.sch
      val errA = curcur.err
      val propA = curcur.prop
      val extralA = curcur.extra_l
      if (pos1 >= trace.size) {
        val ll = norm(patpat("start")("finish"))
        if (prev_elem == "finish") {
          return (-1)
        }
        else if (!(keys contains (prev_elem))) {
          val errw = countw(missed_weights, ll, keys)
          val pos11 = pos1 - 1
          val errB = errA :+ Map("weightmissed" -> wm*errw,"missed" -> ll.mkString(", "),"weightextra" -> 0,"errtype" -> "некорректный конец", " Вес ошибки" -> wm * errw, "статус" -> "fault", "position" -> pos11)
          val charr = arrsc(pos2).copy(metric2 = metric2A + wm * errw, prop = propA + wm * errw, scenario = scenarioA :+ "errfin", err = errB, sch = schA ++ ll)
          arrsc(pos2) = charr
          return (-1)
        }
        else if (!(Dict1(prev_elem) contains "finish")) {
          val s2 = patpat(prev_elem)("finish")
          val errw1 = wm * countw(missed_weights, norm(s2), keys)
          val errw2 = we * countw(missed_weights, ll, keys)
          val scenarioB = scenarioA :+ "errfin"
          val pos11 = pos1 - 1
          val charr = s2(0) match {
            case "-1" => arrsc(pos2).copy(err = errA :+ Map("missed" -> ll.mkString(", "),"weightmissed" -> errw2,"weightextra" -> 0,"errtype" -> "некорректный конец", "Вес ошибки" -> errw2, "статус" -> "fault", "position" -> pos11), metric2 = metric2A + errw2, scenario = scenarioB, prop = propA + errw2, sch = schA ++ ll)
            case _ => arrsc(pos2).copy(err = errA :+ Map("missed" -> norm(s2).mkString(", "),"weightmissed" -> errw1,"weightextra" -> 0,"errtype" -> "некорректный конец", "Вес ошибки" -> errw1, "статус" -> "fault", "position" -> pos11), metric2 = metric2A + errw1, scenario = scenarioB :+ s2, sch = schA ++ norm(s2), prop = propA + errw1)
          }
          arrsc(pos2) = charr
          return (-1)
        }
        else {
          return (-1)
        }
      } else {
        val elem = trace(pos1)
        val reached = Dict1(prev_elem)
        if (reached contains (elem)) {
          val scenarioB = scenarioA :+ Array(elem, pos1)
          val schB = schA :+ elem
          val scenario2B = scenario2A :+ elem
          val scenario2posB = scenario2posA :+ pos1
          val errB = errA :+ Map(elem -> "OK", "статус" -> "good")
          val charr = arrsc(pos2).copy(scenario = scenarioB, err = errB, scenario2 = scenario2B, scenario2pos = scenario2posB, sch = schB)
          arrsc(pos2) = charr
          mainf2(trace, pos1 + 1, pos2, elem, pos1, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
        }
        else {
          val s1 = undefinedmod(pos1 - 1, trace, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
          val posit = s1.position
          val metric2B = metric2A + s1.werr
          val propB = propA + wm * countw(missed_weights, s1.missed, keys)
          val extralB = extralA + s1.extra.size
          val weightmissed0 = wm * countw(missed_weights, s1.missed, keys)
          val weightextra0 = we * countw(extra_weights, s1.extra, keys)
          val errB = errA :+ Map("weightmissed" -> weightmissed0,"weightextra" -> weightextra0,"статус" -> "fault", "errtype" -> s1.error, "position" -> s1.position2, "posfin" -> s1.position, "extra" -> s1.extra.mkString(", "), "missed" -> s1.missed.mkString(", "))
          val schB = schA ++ s1.missed
          //обработка 1 случая
          val scenarioB = scenarioA :+ "пропуск + лишняя"
          if (posit <= trace.size - 1) {
            val elem1 = trace(posit)
            val errC = errB :+ Map(elem1 -> "OK", "статус" -> "good")
            val scenarioC = scenarioB :+ elem1
            val scenario2C = scenario2A :+ elem1
            val schC = schB :+ elem1
            val scenario2posC = scenario2posA :+ posit
            val charr = arrsc(pos2).copy(scenario = scenarioC, err = errC, scenario2 = scenario2C, scenario2pos = scenario2posC, metric2 = metric2B, prop = propB, sch = schC, extra_l = extralB)
            arrsc(pos2) = charr
            mainf2(trace, posit + 1, pos2, elem1, posit, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
          }
          else {
            val charr = arrsc(pos2).copy(scenario = scenarioB, err = errB, scenario2 = scenario2A, scenario2pos = scenario2posA, metric2 = metric2B, prop = propB, sch = schB, extra_l = extralB)
            arrsc(pos2) = charr
            mainf2(trace, posit + 1, pos2, "finish", trace.size, Dict1, patpat, missed_weights, extra_weights, wm, we, keys)
          }
        }
      }
    }

    def cc2(obj: scen, trace: Array[String]): Double = {
      val full = (trace.size - obj.extra_l) + obj.metric2
      return ((obj.metric2) / full)
    }

    case class scenres(scenario2: Array[String], err: Array[Map[String, Any]], metric2: Double, sch: Array[String], cmetr: Double, prop: Double, extra_l: Int)

    def bestfitscenario(arr: ArrayBuffer[scen], trace: Array[String]): scenres = {
      def f(x: scen): Double = cc2(x, trace)
      val finmetr = arr.map { case (y: scen) => f(y) }
      val finarr = arr zip finmetr
      val finres = finarr.minBy(_._2)
      val finsc = finres._1
      val fincm = finres._2
      val finr = scenres(finsc.scenario2, finsc.err, finsc.metric2, finsc.sch, 1 - fincm, finsc.prop, finsc.extra_l)
      return (finr)
    }
    if (version == 1) {
      mainf(trace, 0,0,"start", 0,schema,patpat,missedarray,extraarray,wm,we,keys)
    }
    else {
      mainf2(trace, 0,0,"start", 0,schema,patpat,missedarray,extraarray,wm,we,keys)
    }
    val result = bestfitscenario(arrsc, trace)
    val conformance_metric: Double = result.cmetr
    val scenario_fit: Array[String] = result.sch
    val errors = result.err
    val df = new DecimalFormat("#.##")
    val messages = errors.filter(x => x("статус") == "fault").map(map => {
      val position_start = map.getOrElse("position", -10).toString.toInt
      val tps = position_start match {
        case -1 => "start"
        case _ => trace(position_start)
      }
      val position_fin = map.getOrElse("posfin", -10).toString.toInt
      val chain_missed1 = map.getOrElse("missed", "None").toString
      val chain_missed = chain_missed1 match {
        case "" => "None"
        case _ => chain_missed1
      }
      val chain_missed1pos = map.getOrElse("missed_pos", "None").toString
      val chain_missed_pos = chain_missed1pos match {
        case "" => "None"
        case _ => chain_missed1pos
      }
      val chain_extra1 = map.getOrElse("extra", "None").toString
      val chain_extra = chain_extra1 match {
        case "" => "None"
        case _ => chain_extra1
      }
      val chain_extra1pos = map.getOrElse("extra_pos", "None").toString
      val chain_extra_pos = chain_extra1pos match {
        case "" => "None"
        case _ => chain_extra1pos
      }

      val weight1 = df.format(map("weightmissed").toString.toDouble)
      val weight2 = df.format(map("weightextra").toString.toDouble)
      val errType = map("errtype").toString
      val msg = errType match {
        case "пропуск + лишняя" if ((chain_missed != "None") && (position_fin - position_start > 1)) => {
          "После действия [%s] на позиции %s отклонение : пропущено действие/цепочка действий [%s] общим весом %s; цепочка действий [%s] общим весом %s, начиная с позиции %s является лишней".format(tps,position_start+1, chain_missed, weight1, chain_extra, weight2, position_start+2)
        }
        case "пропуск + лишняя" if ((chain_missed == "None") && (position_fin - position_start > 1)) => {
          "После действия [%s] на позиции %s отклонение : цепочка действий [%s] общим весом %s, начиная с позиции %s является лишней".format(tps,position_start+1, chain_extra, weight2, position_start+2)
        }
        case "пропуск + лишняя" if ((chain_missed != "None") && (position_fin - position_start <= 1)) => {
          "После действия [%s] на позиции %s отклонение : пропущено действие/цепочка действий [%s] общим весом %s".format(tps,position_start+1, chain_missed, weight1)
        }
        case "смена ветки" if ((chain_missed != "None") && (position_fin - position_start > 1)) => {
          "После действия [%s] на позиции %s отклонение : пропущено действие/цепочка действий [%s] общим весом %s; действия [%s] на соответсвующих позициях [%s] общим весом %s являются лишними".format(trace(position_start),position_start+1, chain_missed,weight1, chain_extra, chain_extra_pos, weight2)
        }
        case "смена ветки" if ((chain_missed == "None") && (position_fin - position_start > 1)) => {
          "После действия [%s] на позиции %s отклонение : действия [%s] на соответсвующих позициях [%s] общим весом %s являются лишними ".format(tps,position_start+1, chain_extra, chain_extra_pos, weight2)
        }
        case "смена ветки" if ((chain_missed(0) != "None") && (position_fin - position_start <= 1)) => {
          "После действия [%s] на позиции %s отклонение : пропущено действие/цепочка действий [%s] общим весом %s".format(tps,position_start+1, chain_missed, weight1)
        }
        case "некорректный конец" => {
          "После действия [%s] на позиции %s отклонение : пропущено действие/цепочка действий [%s] общим весом %s".format(tps,position_start+1, chain_missed, weight1)
        }
        case _ => "OK"
      }
      msg }).mkString(" ||| ")
    val Sz = messages.size
    val mess_res = Sz match {
      case 0 => "Проверка на топологические отклонения : Трейс полностью соответствует эталонной схеме"
      case _ => "Проверка на топологические отклонения : Трейс не соответствует схеме : ||| " + messages
    }
    val trace_exp = trace.zipWithIndex
    val message_unexp = trace_exp.filter(x => !(keys contains x._1)).map(map => {
      val msg2 = "[%s, %s]".format(map._1, map._2+1)
      msg2 }).mkString(" ||| ")
    val Sz2 = message_unexp.size
    val message_unexp2 = Sz2 match {
      case 0 => "Проверка на непредусмотренные моделью действия : Трейс не содержит непредусмотренных моделью действий"
      case _ => "Проверка на непредусмотренные моделью действия : Часть действий непредусмотрена моделью : ||| " + message_unexp
    }
    (conformance_metric, scenario_fit.mkString(", "), mess_res, message_unexp2)
  }
  def checkT(trace1 : Seq[String], trace_time : Seq[Double], Dict_time : Seq[Seq[String]], keys1 : Seq[String]) : String = {
    if (trace1.size <= 1) {return "Трейс состоит из одного действия => анализ невозможен"}
    else {
      val keys = keys1.toArray
      val trace: Array[String] = trace1.toArray
      val timetrace2 = trace_time.map(x => x.toDouble).toArray
      val Dicttime = Dict_time.map(x => (x(0) -> x(1).toDouble)).toMap
      val timetr = timetrace2.tail ++ Array(0.0)
      val df = new DecimalFormat("#.##")
      val timetraceA = timetrace2.zip(timetr).map(x => df.format(x._2 - x._1).toDouble)
      val timetrace3 = timetraceA.dropRight(1)
      case class time_class(element: String, status: String, model_time: Double, real_time: Double, typerr: String)
      def timech(element: String, timetr: Double, Dicttime: Map[String, Double], keys: Array[String]): time_class = {
        if (!(keys contains element)) {
          time_class(element, "Fault", -1, timetr, "Несуществующее действие")
        }
        else if (Dicttime(element) < timetr) {
          time_class(element, "Fault", Dicttime(element), timetr, "Превышено время")
        }
        else {
          time_class(element, "good", -1, -1, "время соответствует")
        }
      }
      val szz = trace.size
      val trace2 = trace.dropRight(1)
      val tracet = trace2.zip(timetrace3)
      val messages3 = tracet.zipWithIndex.filter(x => timech(x._1._1, x._1._2, Dicttime, keys).status == "Fault").map(map => {
        val errType = timech(map._1._1, map._1._2, Dicttime, keys).typerr
        val msg4 = errType match {
          case "Несуществующее действие" => {
            "Действие [%s] на позиции %s не предусмотрено эталонной схемой".format(map._1._1, map._2 + 1)
          }
          case "Превышено время" => {
            val razn = map._1._2 - Dicttime(map._1._1)
            "При выполнении действия [%s] на позиции %s реальное время (%s секунд) превысило эталонное время на %s секунд".format(map._1._1, map._2 + 1, map._1._2, df.format(razn))
          }
        }
        msg4
      }).mkString(" ||| ")
      val Sz3 = messages3.size
      val mess3_res = Sz3 match {
        case 0 => "Проверка на время корректна"
        case _ => "Проверка времени : ||| " + messages3
      }
      return (mess3_res)
    }
  }

  def filterBySegment(input: String, filter: String): Boolean = {
    val filterValues = filter.replaceAll("[\\[\\]\\(\\)]", "").split(",").zipWithIndex.filter(_._2 % 2 == 0).map(_._1)
    filterValues.contains(input)
  }

//  def remapTime(start_date: String, end_date: String, calendar_key: String): Int = {
//    3
//  }

}

object BpmParser {
  @transient lazy val jp = new BpmParser()
  def parseTimestamp: UserDefinedFunction  = udf((time: Long) => jp.longToDateString(time))
  def parseRelation: UserDefinedFunction  = udf((relation: Seq[Seq[String]]) => jp.splitRelation(relation))
  def createXLogPerBin: UserDefinedFunction  = udf((
                                                 bins: Int, traceList: Seq[String],
                                                 eventList : Seq[Seq[Seq[String]]]
                                               ) => jp.createBinaryXLogs(bins, traceList, eventList))
  def getCycles: UserDefinedFunction  = udf((trace: Seq[String]) => jp.getCycles(trace))
  def clean: UserDefinedFunction  = udf((
                                        trace: Seq[String],
                                        cycles: Seq[Seq[String]],
                                        indexes: Seq[Int],
                                        counts: Seq[Int]) => jp.removeCycles(trace, cycles, indexes, counts))
  def zip: UserDefinedFunction  = udf((tr: Seq[String], dt: Seq[Double]) => jp.zipCols(tr, dt))
  def hashInt: UserDefinedFunction  = udf((input: String) => input.hashCode())
  def getColor: UserDefinedFunction  = udf((num: Long, max_th: Long) => jp.color(num, max_th))
  def filterSegment: UserDefinedFunction = udf((in: String, f: String) => jp.filterBySegment(in, f))
  def check: UserDefinedFunction  = udf((traceAB: Seq[String], schema1: Seq[Seq[String]], missedarray1: Seq[Seq[String]], extraarray1: Seq[Seq[String]], wm: Double, we: Double, version: Int, patpat1: Seq[Seq[Seq[String]]], keysAB: Seq[String]) =>  jp.checkconformance(traceAB, schema1, missedarray1,extraarray1,wm,we,version, patpat1, keysAB))
  def checktime: UserDefinedFunction  = udf((trace: Seq[String], trace_time : Seq[Double], Dicttime : Seq[Seq[String]], keys : Seq[String]) =>  jp.checkT(trace, trace_time, Dicttime, keys))
//  def remapTime: UserDefinedFunction = udf((start_date: String,
//                                            end_date: String,
//                                            calendar_key: String) => jp.remapTime(start_date, end_date, calendar_key))
}