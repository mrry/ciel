object ScalaBinomialOptions {

  private def init_vals(t:Double, v:Double, rf:Double, n:Int) : (Double,Double,Double,Double) = {
    var h = t / n
    var xd = (rf - 0.5 * (v * v)) * h
    var xv = v * scala.math.sqrt(h)
    var u = scala.math.exp ( xd + xv )
    var d = scala.math.exp ( xd - xv )
    var drift = scala.math.exp (rf * h)
    var q = (drift - d) / (u - d)
    (q, u, d, drift)
  }

  private def c_stkval(n:Double, s:Double, u:Double, d:Double, j:Double) : Double = {
    s * (scala.math.pow(u, (n-j))) * (scala.math.pow(d,j))
  }

  private def gen_initial_optvals(n:Int, s:Double, u:Double, d:Double, k:Double, cp:Double) {
    for (j <- n.until(-1,-1)) {
      var stkval = c_stkval (n,s,u,d,j)
      var v = scala.math.max(0, (cp * (stkval - k)))
      println(v)
    }
  }

  private def eqn(q:Double, drift:Double, a:Double, b:Double) =
   ((q * a) + (1.0 - q) * b) / drift

  private def apply_column(v:Double ,v1:Double, acc:Array[Double], pos:Int ,chunk:Int, q:Double, drift:Double) = {
    var v1 = acc(0)
    acc(0) = v
    var maxcol = scala.math.min(chunk,pos)
    for (idx <- 1.until(maxcol+1)) {
      var nv1 = eqn(q,drift,acc(idx-1),v1)
      v1 = acc(idx)
      acc(idx) = nv1
    }
    if (maxcol == chunk)
      println(acc(maxcol))
  }

  private def process_rows(rowstart:Int, rowto:Int, q:Double, drift:Double) = {
    println(rowto)
    var chunk = rowstart - rowto
    var acc = new Array[Double](chunk+1)
    var v1 = 0.0
    for (pos <- 0.until(rowstart+1)) {
      var v = readLine.toDouble
      apply_column(v, v1, acc, pos, chunk, q, drift)
    }
  }

  def main(args: Array[String]) {
    var s = args(0).toDouble
    var k = args(1).toDouble
    var t = args(2).toDouble
    var v = args(3).toDouble
    var rf = args(4).toDouble
    var cp = args(5).toDouble
    var n = args(6).toInt
    var chunk = args(7).toInt
    var start = args(8).toInt
    var (q,u,d,drift) = init_vals(t, v, rf, n)
    start match {
      case 1 =>
        println(n)
        System.err.println(n)
        gen_initial_optvals(n, s, u, d, k, cp)
      case _ =>
        var rowstart = readLine.toInt
        if (rowstart == 0) {
          var r = readLine
          println(r)
        } else {
          var rowto = scala.math.max(0,(rowstart - chunk))
          process_rows(rowstart, rowto, q, drift)
        }
    }
  }
}

