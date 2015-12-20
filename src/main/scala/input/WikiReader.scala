package input

import java.io.InputStream
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.pull._

/*
  Code used from here: https://tuxdna.wordpress.com/2014/02/03/a-simple-scala-parser-to-parse-44gb-wikipedia-xml-dump/
 */
class WikiReader(val in : InputStream) {

  val xml = new XMLEventReader(Source.fromInputStream(in))

  def read(): String = {
    var insidePage = false
    var buf = ArrayBuffer[String]()
    for (event <- xml) {
      event match {
        case EvElemStart(_, "page", _, _) =>
          insidePage = true
          val tag = "<page>"
          buf += tag
        case EvElemEnd(_, "page") =>
          val tag = "</page>"
          buf += tag
          insidePage = false
          return buf.mkString
        case e@EvElemStart(_, tag, _, _) =>
          if (insidePage) {
            buf += ("<" + tag + ">")
          }
        case e@EvElemEnd(_, tag) =>
          if (insidePage) {
            buf += ("</" + tag + ">")
          }
        case EvText(t) =>
          if (insidePage) {
            buf += t
          }
        case _ => // ignore
      }
    }
    ""
  }
}