import java.util.concurrent.Phaser
import scala.annotation.tailrec
import scala.language.implicitConversions


object IteratorUtils {

  case class OrderViolation[T](a: T, b: T) extends Exception(s"Order assert violation: $a then $b")

  protected case class IteratorImplicit1[T](in: Iterator[T]) {

    // check if iterator represent ordered sequence
    def checkOrder[K](implicit ord: Ordering[T]): Iterator[T] = checkOrder[T](identity[T] _)

    def checkOrder[K](k: T => K)(implicit ord: Ordering[K]): Iterator[T] =
      Iterator.unfold(in.buffered) { bi =>
        bi.nextOption().map { current =>
          bi.headOption.foreach { next =>
            if (ord.gt(k(current), k(next))) throw OrderViolation(current, next) // detect order violation
          }
          current -> bi
        }
      }

    def groupByOrdered[K, V](k: T => K)(v: T => V /* = identity */): Iterator[(K, List[V])] =
      Iterator.unfold(in.buffered) { bi =>
        bi.headOption.map(k).map { key =>
          @tailrec def group(list: List[T] = List.empty): List[T] =
            if (bi.headOption.exists(k(_) == key)) group(bi.next() :: list) else list
          // Note: don't use  bi.takeWhile(k(_) == key)  - it spoils one-path iterator

          key -> group().map(v) -> bi
        }
      }

  }

  protected case class IteratorImplicit2[T](in: Iterator[(T, T)]) {
    def checkIntervalsOrder(implicit ord: Ordering[T]): Iterator[(T, T)] =
      Iterator.unfold(in.buffered) { bi =>
        bi.nextOption().map { current =>
          if (ord.gt(current._1, current._2)) throw OrderViolation(current._1, current._2) // detect order violation
          bi.headOption.foreach { next =>
            if (ord.gt(current._1, next._1)) throw OrderViolation(current, next) // detect order violation
          }
          current -> bi
        }
      }
  }

  implicit def fromIterator1[T](in: Iterator[T]) = IteratorImplicit1[T](in)

  implicit def fromIterator2[T](in: Iterator[(T, T)]) = IteratorImplicit2[T](in)

  def replicate[T](n: Int): Iterator[T] => Iterator[T] = _.flatMap(Iterator.fill[T](n)(_)) // repeat each element n times

  // Forking iterator. This is know-how
  def forkIterator[T](n: Int): Iterator[T] => Iterator[T] = replicate(n) andThen { it =>
    new Phaser(n) with Iterator[T] {

      private var hasNext0: Boolean = it.hasNext // works like java.util.concurrent.atomic.AtomicBoolean

      override def hasNext: Boolean = it.synchronized {
        hasNext0
      }

      override def next(): T = {
        arriveAndAwaitAdvance()
        val next = it.synchronized {
          val next = it.next()
          if (hasNext0) hasNext0 = it.hasNext
          next
        }
        arriveAndAwaitAdvance() // otherwise the tasks may locks at the end the last data element
        next
      }

      // In case that a consumer cancel [to read] before the end of the source stream,
      // it should drain the last to avoid block others. (Note: Phaser has no "unregister" method).
      // Example:
      // while (it.hasNext) it.next()
    }
  }
}
