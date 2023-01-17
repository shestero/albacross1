import IteratorUtils._
import Ip4Utils._
import RangeStream.{IpRange, RangeStream}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.{SortedMap, SortedSet}
import scala.language.implicitConversions

import cats.syntax.semigroup._


object Solutions {

  type PointCounter = SortedMap[Long, Int]
  object PointCounter {
    def empty = SortedMap.empty[Long, Int]
  }

  protected case class PointCounterHelper(counter: PointCounter) {
    def countEnds(points: Iterable[Long]): PointCounter =
      counter ++ points.map(point => point -> (counter.getOrElse(point, 0) - 1))
  }
  implicit def fromPointCounter[K, V](counter: PointCounter) = PointCounterHelper(counter)

  // Logic

  protected def processPreparedPoints(points: Iterable[(Long, (Int, Int))]): RangeStream =
    processPreparedPoints(points.iterator)

  protected def processPreparedPoints(points: Iterator[(Long, (Int, Int))]): RangeStream = {
    val (it, zero) = points.checkOrder.foldLeft(Iterator.empty[Long] -> 0) {
      case ((output, counter), (ip, (delta1, delta2))) =>
        assert(counter >= 0)
        assert(delta1 >= 0)
        assert(delta2 <= 0)
        val next = counter + delta1 + delta2
        assert(next >= 0)
        val out = counter -> next -> delta1 match {
          case ((0, 0), 1) => Iterator(ip, ip) // standalone single point range without overlapping
          case ((0, 0), _) => Iterator.empty // several single point ranges at the same place
          case ((1, 1), _) => Iterator(ip - 1, ip + 1) // overlapping with single point point range(s)
          case ((0, 1), 1) | ((1, 0), 0) => Iterator.single(ip) // begin or end of a range
          case ((0, 1), _) => Iterator(ip + 1) // overlapping with single point range(s) at the begin of a range
          case ((1, 0), _) => Iterator(ip - 1) // overlapping with single point range(s) at the end of a range
          case ((_, 1), _) => Iterator.single(ip + 1) // end of overlapping
          case ((1, _), _) => Iterator.single(ip - 1) // begin of overlapping
          case _ => Iterator.empty
        }
        output ++ out -> next
    }
    assert(zero == 0)
    // assert(it.size % 2 == 0) // this check drains it!

    it.grouped(2).flatMap {
      case Seq(begin, end) if begin - 1 == end => Iterator.empty
      // this situation occurs if inside one bigger range there are side by side ranges; e.g.:
      // [1,4],[2,2],[3,3] => [1,1],{3,2},[4,4] => [1,1],[4,4]
      // [1,8],[3,4],[5,6] => [1,2],{5,4},[7,8] => [1,2],[7,8]
      case Seq(begin, end) => assert(begin <= end); Iterator(begin -> end)
      case sq => throw new Exception(s"Unpaired points in processPreparedPoints! $sq")
    }.checkIntervalsOrder
  }

  // solution1 - by total sorting
  def solution1(input: RangeStream): RangeStream = {
    val prepared = input.flatMap { case (begin, end) => Iterator(begin -> (+1, 0), end -> (0, -1)) }
      .toSeq.groupMapReduce(_._1)(_._2)(_ |+| _)
      .toSeq.sorted

    processPreparedPoints(prepared)
  }

  // solution2 - using (assuming) the order of input ranges by beginning of each range
  def solution2(input: RangeStream): RangeStream = {
    val (prepared0, lastEnds) =
      input.checkOrder(_._1).groupByOrdered(_._1)(_._2)
        .foldLeft(Iterator.empty[(Long, Int, PointCounter)] -> PointCounter.empty) {
          case ((it, inheritedEnds), (begin, ends)) =>
            val allEnds = inheritedEnds.countEnds(ends)
            val currentEnds = allEnds.rangeTo(begin)
            val nextEnds = allEnds.rangeFrom(begin + 1)
            it ++ Iterable.single((begin, ends.size, currentEnds)) -> nextEnds
        }

    val prepared = prepared0.flatMap {
      case (now, beginCounter, ends) =>
        val endsBefore = ends.rangeUntil(now).view.mapValues(0 -> _)
        val endCounter = ends.get(now).getOrElse(0)
        endsBefore ++ Map(now -> (beginCounter, endCounter))
    } ++ lastEnds.view.mapValues(0 -> _)

    processPreparedPoints(prepared.checkOrder)
  }

  // Spark
  def solution3(input: RangeStream)(implicit spark: SparkSession): Dataset[IpRange] = {
    import spark.implicits._

    // cut long inter-partition ranges apart
    val (prepared, gluePoints) = input.foldLeft(RangeStream.empty -> SortedSet.empty[Long]) { case ((in, s), r@(b, e)) =>
      val (p1, p2) = (partition(b), partition(e))
      assert(p1 <= p2)
      if (p1 == p2)
        in ++ Iterator.single(r) -> s
      else {
        val cuts = p1 + 1 to p2
        val points = b +: cuts.flatMap(junctions) :+ e
        val splits: RangeStream = points.grouped(2).map { case Seq(b: Long, e: Long) => b -> e }
        (in ++ splits) -> (s ++ cuts.toSet)
      }
    }

    val srcDS = prepared.toSeq.toDS()

    val udfPartitioner = udf[Long, Long](partition)
    val partitioned = srcDS
      .repartitionByRange(udfPartitioner($"_1"))
      .sortWithinPartitions($"_1" /* , $"_2" */)

    val ds = partitioned.mapPartitions(solution2) // using solution2 within each partition

    // glue inter-partition ranges together
    lazy val faste = ds.filter(_._1 % partitioner == 0).collect().to(scala.collection.mutable.SortedMap)
    lazy val fastb = ds.filter(_._2 % partitioner == partitioner - 1).map(_.swap).collect().to(scala.collection.mutable.SortedMap)
    // faste and fastb are mutable sturcutres used only to boost the speed of algorithm; in fact they are indices
    gluePoints.foldLeft(ds) { case (set, p) =>
      val Seq(b, e) = junctions(p)
      (fastb.get(b), faste.get(e)) match {
        case (Some(left), Some(right)) =>
          fastb.remove(b)
          faste.remove(e)
          faste.addOne(left -> right) // may be needed in case of a range was cut for more than two peaces
          fastb.addOne(right -> left)
          set.filter(_ != (left -> b)).filter(_ != (e -> right)) union Seq((left -> right)).toDS()
        case _ => set
      }
    }
  }

}

