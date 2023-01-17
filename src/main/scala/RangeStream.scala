import Ip4Utils._

import scala.io.Source
import scala.util.Success

object RangeStream {
  type IpRange = (Long, Long)
  type RangeStream = Iterator[IpRange]

  def empty: RangeStream = Iterator.empty[IpRange]

  // Read source data from collection
  def iterableSource: Iterable[(String, String)] => RangeStream =
    _.iterator.map { case r@(sbegin, send) =>
      {
        for {
          begin <- ip4ToLong(sbegin)
          end <- ip4ToLong(send)
        } yield begin -> end
      }.getOrElse(throw new Exception(s"Wrong input range: $r"))
    }

  // Read source data from file
  def fileSource(inputfile: String = "input1.txt"): RangeStream =
    Source.fromFile(inputfile).getLines().flatMap(line => line.split(",").map(ip4ToLong) match {
      case Array(Success(begin), Success(end)) => assert(begin <= end); Some(begin -> end)
      case _ => println(s"Warning: skip wrong input line: $line"); None
    })

  // Generate random source data
  def randomSource(size: Int = 50): RangeStream = (1 to size).iterator.map(_ => randomIpRange)

  // Print IP ranges (to debug)
  def printRange: RangeStream => Unit = LazyList.from(1).zip(_).foreach {
    case (num, (begin, end)) => assert(begin <= end); println(s"$num\t${longToIp4(begin)}, ${longToIp4(end)}")
  }

}
