import IteratorUtils._
import Ip4Utils._
import RangeStream._
import Solutions._

import java.util.TimeZone
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.elasticsearch.spark.sql._


object Main extends App {

  // Spark configuration:
  val conf =
    new SparkConf()
      .setMaster(sys.env.getOrElse("ALBACROSS_SPARK", "local[*]"))
      .setAppName("spark-albacross1")
      .set(ES_INDEX_AUTO_CREATE, "true")
      .set(ES_NODES, sys.env.getOrElse("ALBACROSS_ES_NODES", ES_NODES_DEFAULT))

  implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  val context: SparkContext = spark.sparkContext
  context.setLogLevel("ERROR")
  println(s"Using Scala ${util.Properties.versionString} Apache Spark version ${spark.version}")

  // MySQL configuration:
  val mysqlConnectionProperties = Map(
    "driver" -> "com.mysql.cj.jdbc.Driver",
    "url" -> sys.env.getOrElse("ALBACROSS_DB_URL", s"jdbc:mysql://localhost:3306/albacross1"),
    "user" -> sys.env.getOrElse("ALBACROSS_DB_USER", "root"),
    "password" -> sys.env.getOrElse("ALBACROSS_DB_PASSWORD", ""),
    "useUnicode" -> "true",
    "characterEncoding" -> "UTF-8",
    "zeroDateTimeBehavior" -> "convertToNull",
    "serverTimezone" -> TimeZone.getDefault().getID(),
    "useCursorFetch" -> "true"
  )
  val tableInput = "input"
  val tableOutput = "output"

  // Test from file input1.txt
  val s = solution1(fileSource())
  printRange(s)
  val s1 = solution1(fileSource()).toSeq
  val s2 = solution2(fileSource()).toSeq // assume the input ranges are sorted in this file
  val s3 = solution3(fileSource()).collect().toSeq.sorted
  assert(s1 == s2)
  assert(s1 == s3)

  // Test procedure
  // This do test (calling solution1 - solution3) over given set of ranges.
  val testTask: Int => RangeStream => Future[Unit] = forkIterator[IpRange](4) andThen testTask4threads(_)
  // Source data will be consumed through several threads.
  // It is important to provide right number of consumers here.
  // The first consumer is used to save the input into MySQL.

  private def testTask4threads(set_id: Int)(fork4: RangeStream): Future[Unit] = {
    import spark.implicits._

    val saveInputToMysql = Future {
      fork4
        .map { case (a, b) => (set_id, a, b) }.toSeq.toDF("set_id", "begin", "end")
        .write.mode(Append).format("jdbc").options(mysqlConnectionProperties).option("dbtable", tableInput)
        .save()
    }

    def saveOutput(df: DataFrame) = Future {
      // save result to MySQL
      df.write.mode(Append).format("jdbc").options(mysqlConnectionProperties).option("dbtable", tableOutput).save()

      // save result to ElasticSearch
      val long2ip = udf[String, Long](longToIp4 _)
      val edf = df.withColumns(Map("begin" -> long2ip($"begin"), "end" -> long2ip($"end")))

      val esIndex = sys.env.getOrElse("ALBACROSS_ES_INDEX", "albacross/docs")
      if (esIndex.trim.nonEmpty)
        edf.saveToEs(esIndex)
    }

    val subtasks = Seq(
      Future(solution1(fork4).toSeq),
      Future(solution2(fork4.toSeq.sorted.iterator).toSeq),
      Future(solution3(fork4).collect().sorted.toSeq)
    )

    val futureComputed = Future.sequence(subtasks).map { case head :: tail =>
      assert(tail.forall(_ == head)) // all results should be the same
      head
    }

    futureComputed.failed.foreach { e =>
      println(s"Task failed with error: ${e.getMessage}")
    }

    for {
      _ <- saveInputToMysql
      result <- futureComputed
      df = result.map { case (b, e) => (set_id, b, e) }.toDF("set_id", "begin", "end")
      _ <- saveOutput(df)
    } yield ()
  }

  // Run test with saved input ranges and 20 with random ranges
  val inputs = fileSource() +: (1 to 20).to(LazyList).map(_ => randomSource(100))
  val tasks = (LazyList.from(
    spark.read
      .format("jdbc").options(mysqlConnectionProperties).option("dbtable", tableInput).load()
      .agg("set_id" -> "max")
      .head().get(0) match {
      case i: Int => i + 1
      case _ => 1 // max returned NULL
    }) zip inputs).map { case (set_id, input) => testTask(set_id)(input) }

  tasks.zipWithIndex.foreach { case (task, num) =>
    println(s"Task# $num");
    Await.ready(task, Duration.Inf) // do tasks one by one
    task.failed.foreach { error =>
      println(s"Error in task# $num : ${error.getMessage}")
    }
    println(s"Task# $num finished")
  }

  println("Local Spark is to be stopped")
  Thread.sleep(1000)

  context.stop()
  spark.stop()
  // shut down may cause NoSuchFileException: .../hadoop-client-api-3.3.2.jar
}
