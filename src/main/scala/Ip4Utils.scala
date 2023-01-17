import scala.util.{Random, Success, Try}

object Ip4Utils {

  val partitioner: Long = 0x10000000

  def partition(ip: Long): Long = ip / partitioner

  def junctions(p: Long) = Seq(p * partitioner - 1, p * partitioner)

  val ip4ToLongUnsafe: String => Long = _.trim.split("\\.").map(_.toLong) match {
    case r@Array(a, b, c, d) if r.map(_ & ~0xFF).forall(_ == 0) => a * 16777216 + b * 65536 + c * 256 + d
  }

  val ip4ToLong: String => Try[Long] = Success(_).map(ip4ToLongUnsafe)

  def longToIp4(ip0: Long): String = {
    import scala.math.Integral.Implicits._
    val (ip1, b1) = ip0 /% 256L
    val (ip2, b2) = ip1 /% 256L
    val (ip3, b3) = ip2 /% 256L
    val (ip4, b4) = ip3 /% 256L
    assert(ip4 == 0)
    s"$b4.$b3.$b2.$b1"
  }


  // random IP generation:

  private val random = Random

  def randomIp: String =
    (random.nextInt(223) + 1).toString + "." + (1 to 3).map { _ => random.nextInt(255) }.mkString(".")

  def randomIpLong: Long = ip4ToLong(randomIp).get

  def randomIpRange: (Long, Long) = {
    //val r@(r1, r2) = (randomIpLong, randomIpLong)
    //if (r1 > r2) r.swap else r
    val r1 = randomIpLong
    val r2 = r1 + Math.sqrt(random.nextLong(10000000000000000L).toDouble).toLong
    r1 -> r2.max(0xFFFFFFFF)
  }

}
