package datapipelines

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Try

sealed trait SessionEvent

object SessionEvent {
  final case class SessionStart(timestamp: String) extends SessionEvent
  final case class SessionEnd(timestamp: String) extends SessionEvent

  final case class QuickSearch(
      timestamp: String,
      query: String,
      searchId: String,
      documents: Seq[String]
  ) extends SessionEvent

  final case class CardSearch(
      timestamp: String,
      params: List[(String, String)],
      searchId: String,
      documents: Seq[String]
  ) extends SessionEvent

  final case class DocOpen(timestamp: String, searchId: String, documentId: String) extends SessionEvent
}

object SessionParser {
  import SessionEvent._

  private val TokenSessionStart = "SESSION_START"
  private val TokenSessionEnd = "SESSION_END"
  private val TokenQs = "QS"
  private val TokenCardSearchStart = "CARD_SEARCH_START"
  private val TokenCardSearchEnd = "CARD_SEARCH_END"
  private val TokenDocOpen = "DOC_OPEN"

  private sealed trait State
  private case object Idle extends State
  private case class AwaitingQsResults(ts: String, query: String) extends State
  private case class InCardSearch(ts: String, params: List[(String, String)]) extends State
  private case object AwaitingCardResults extends State

  private case class PendingCard(ts: String, params: List[(String, String)])

  def parse(lines: Iterable[String]): Seq[SessionEvent] = {
    val events = List.newBuilder[SessionEvent]
    var state: State = Idle
    var pendingCard: Option[PendingCard] = None

    lines.iterator.foreach { rawLine =>
      val line = rawLine.trim
      if (line.nonEmpty) {
        state match {
          case AwaitingQsResults(ts, query) =>
            parseResultLine(line) match {
              case Some((searchId, docs)) =>
                events += QuickSearch(ts, query, searchId, docs)
                state = Idle
              case None =>
                state = Idle
                processIdleLine(line, events).foreach(s => state = s)
            }

          case InCardSearch(ts, params) =>
            if (line.startsWith(TokenCardSearchEnd)) {
              pendingCard = Some(PendingCard(ts, params))
              state = AwaitingCardResults
            } else {
              parseParamLine(line).foreach { param =>
                state = InCardSearch(ts, params :+ param)
              }
            }

          case AwaitingCardResults =>
            parseResultLine(line) match {
              case Some((searchId, docs)) =>
                pendingCard.foreach { pc =>
                  events += CardSearch(pc.ts, pc.params, searchId, docs)
                }
                pendingCard = None
                state = Idle
              case None =>
                pendingCard = None
                state = Idle
                processIdleLine(line, events).foreach(s => state = s)
            }

          case Idle =>
            processIdleLine(line, events).foreach(s => state = s)
        }
      }
    }

    events.result()
  }

  private def processIdleLine(
      line: String,
      events: collection.mutable.Builder[SessionEvent, List[SessionEvent]]
  ): Option[State] = {
    val parts = line.split(" ", 3)
    if (parts.isEmpty) return None

    parts(0) match {
      case TokenSessionStart if parts.length >= 2 =>
        events += SessionStart(parts(1))
        None

      case TokenSessionEnd if parts.length >= 2 =>
        events += SessionEnd(parts(1))
        None

      case TokenQs if parts.length >= 3 =>
        Some(AwaitingQsResults(parts(1), extractQuery(parts(2))))

      case TokenCardSearchStart if parts.length >= 2 =>
        Some(InCardSearch(parts(1), Nil))

      case TokenDocOpen =>
        parseDocOpen(line).foreach(events += _)
        None

      case _ => None
    }
  }

  private def parseResultLine(line: String): Option[(String, Seq[String])] = {
    val tokens = line.split("\\s+").toSeq
    if (tokens.length < 2) return None

    Try(tokens.head.toLong).toOption.map { _ =>
      val searchId = tokens.head
      val docs = tokens.tail.filter(isDocumentId)
      (searchId, docs)
    }
  }

  private def parseParamLine(line: String): Option[(String, String)] = {
    if (!line.startsWith("$")) return None

    val spaceIdx = line.indexOf(' ')
    if (spaceIdx < 0) Some((line.substring(1), ""))
    else {
      val paramId = line.substring(1, spaceIdx)
      val paramValue = line.substring(spaceIdx + 1).trim
      Some((paramId, paramValue))
    }
  }

  private def parseDocOpen(line: String): Option[DocOpen] = {
    val tokens = line.trim.split("\\s+")
    tokens.length match {
      case 4 => Some(DocOpen(tokens(1), tokens(2), tokens(3)))
      case 3 => Some(DocOpen("", tokens(1), tokens(2)))
      case _ => None
    }
  }

  private def extractQuery(raw: String): String = {
    val trimmed = raw.trim
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) trimmed.substring(1, trimmed.length - 1)
    else trimmed
  }

  private def isDocumentId(token: String): Boolean =
    token.matches("[A-Za-z0-9]+_\\d+")
}

object Metric1CardSearchJob {
  import SessionEvent.CardSearch

  val TargetDocumentId = "ACC_45616"

  def run(parsedBySession: org.apache.spark.rdd.RDD[Seq[SessionEvent]]): Long =
    parsedBySession.map(computeInSingleSession).sum().toLong

  private def computeInSingleSession(events: Seq[SessionEvent]): Long = {
    events.collect {
      case card: CardSearch if isTargetSearch(card) => 1L
    }.sum
  }

  private def isTargetSearch(card: CardSearch): Boolean = {
    val inResults = card.documents.contains(TargetDocumentId)
    // Extra guard: some broken logs may place target doc in card parameters.
    val inParams = card.params.exists { case (_, value) =>
      value.split("\\s+").contains(TargetDocumentId)
    }
    inResults || inParams
  }
}

object SessionMetricsApp {
  private final case class AppConfig(inputPath: String, outputPath: String)

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("Session metrics")
      .getOrCreate()

    try {
      val parsedBySession = spark.sparkContext
        .wholeTextFiles(config.inputPath)
        .map { case (_, content) => SessionParser.parse(content.split("\\r?\\n").toIterable) }
        .cache()

      val metric1Count = Metric1CardSearchJob.run(parsedBySession)
      val metric2DailyOpens = Metric2DailyQsOpensJob.run(parsedBySession)

      overwritePath(spark, config.outputPath)
      writeMetric1(spark, config.outputPath, metric1Count)
      writeMetric2(spark, config.outputPath, metric2DailyOpens)
    } finally {
      spark.stop()
    }
  }

  private def writeMetric1(spark: SparkSession, outputPath: String, value: Long): Unit = {
    spark.sparkContext
      .parallelize(Seq(s"card_search_target_count_${Metric1CardSearchJob.TargetDocumentId}=$value"), 1)
      .saveAsTextFile(s"$outputPath/metric1_card_search_target_count")
  }

  private def writeMetric2(
      spark: SparkSession,
      outputPath: String,
      metric2DailyOpens: org.apache.spark.rdd.RDD[Metric2DailyQsOpensJob.DailyDocumentOpen]
  ): Unit = {
    import spark.implicits._

    val ds: Dataset[Metric2DailyQsOpensJob.DailyDocumentOpen] = metric2DailyOpens.toDS()
      .orderBy($"day".asc, $"documentId".asc)

    ds.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputPath/metric2_daily_qs_document_opens")
  }

  private def overwritePath(spark: SparkSession, outputPath: String): Unit = {
    val path = new Path(outputPath)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  private def parseArgs(args: Array[String]): AppConfig = {
    val params = args.sliding(2, 2).collect {
      case Array(k, v) if k.startsWith("--") => (k.stripPrefix("--"), v)
    }.toMap

    val inputPath = params.getOrElse("input", throw new IllegalArgumentException("Missing --input argument"))
    val outputPath = params.getOrElse("output", throw new IllegalArgumentException("Missing --output argument"))

    AppConfig(inputPath, outputPath)
  }
}
