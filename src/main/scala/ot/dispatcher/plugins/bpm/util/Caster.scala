package ot.dispatcher.plugins.bpm.util

import scala.util.{Failure, Success, Try}

object Caster {
  private[bpm] def safeCast[A](optionValue: Option[String], defaultValue: A, sendError: => Nothing): A = {

    def matcher[B](x: Try[_]): B = x match {
      case Success(vv) => vv.asInstanceOf[B]
      case Failure(_) => sendError
    }

    optionValue match {
      case Some(v) => defaultValue match {
        case _: Int => matcher[A]( Try(v.toInt) )
        case _: Double => matcher[A]( Try(v.toDouble) )
        case _: Float => matcher[A]( Try(v.toFloat) )
        case _: Long => matcher[A]( Try(v.toLong) )
        case _: Boolean => matcher[A]( Try(v.toBoolean) )
        case _: String => matcher[A]( Try(v) )
        case _ => defaultValue
      }
      case None => defaultValue
    }
  }
}
