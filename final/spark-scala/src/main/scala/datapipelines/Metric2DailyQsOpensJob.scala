package datapipelines

import datapipelines.SessionEvent._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Metric2DailyQsOpensJob {
  final case class DailyDocumentOpen(day: String, documentId: String, openCount: Long)

  def run(parsedBySession: RDD[Seq[SessionEvent]]): RDD[DailyDocumentOpen] = {
    parsedBySession
      .flatMap(extractSessionPairs)
      .reduceByKey(_ + _)
      .map { case ((day, documentId), openCount) =>
        DailyDocumentOpen(day, documentId, openCount)
      }
  }

  private def extractSessionPairs(events: Seq[SessionEvent]): Seq[((String, String), Long)] = {
    val quickSearchDocsBySearchId = mutable.Map.empty[String, Set[String]]
    val result = mutable.ArrayBuffer.empty[((String, String), Long)]
    var sessionDay: Option[String] = None

    events.foreach {
      case SessionStart(timestamp) =>
        sessionDay = extractDate(timestamp).orElse(sessionDay)

      case QuickSearch(_, _, searchId, docs) =>
        quickSearchDocsBySearchId.update(searchId, docs.toSet)

      case DocOpen(timestamp, searchId, documentId) =>
        if (quickSearchDocsBySearchId.get(searchId).exists(_.contains(documentId))) {
          val day = extractDate(timestamp).orElse(sessionDay)
          day.foreach(d => result += (((d, documentId), 1L)))
        }

      case _ =>
    }

    result.toSeq
  }

  private def extractDate(timestamp: String): Option[String] = {
    val datePart = timestamp.split("_").headOption.getOrElse("")
    if (datePart.matches("\\d{2}\\.\\d{2}\\.\\d{4}")) Some(datePart) else None
  }
}
