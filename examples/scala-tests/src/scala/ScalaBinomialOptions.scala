import java.io.DataOutputStream
import java.io.DataInputStream
import java.io.ObjectOutputStream

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

class ScalaBinomialOptions(var s:Double, var k:Double, var t:Double, var v:Double, var rf:Double, var cp:Double, var n:Int, var chunk:Int, var in_ref:Option[Reference]) extends FirstClassJavaTask {

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

  private def gen_initial_optvals(sout:DataOutputStream, n:Int, s:Double, u:Double, d:Double, k:Double, cp:Double) {
    for (j <- n.until(-1,-1)) {
      var stkval = c_stkval (n,s,u,d,j)
      var v = scala.math.max(0, (cp * (stkval - k)))
      sout.writeDouble(v)
    }
  }

  private def eqn(q:Double, drift:Double, a:Double, b:Double) =
   ((q * a) + (1.0 - q) * b) / drift

  private def apply_column(sout:DataOutputStream, v:Double, v1:Double, acc:Array[Double], pos:Int ,chunk:Int, q:Double, drift:Double) = {
    var v1 = acc(0)
    acc(0) = v
    var maxcol = scala.math.min(chunk,pos)
    for (idx <- 1.until(maxcol+1)) {
      var nv1 = eqn(q,drift,acc(idx-1),v1)
      v1 = acc(idx)
      acc(idx) = nv1
    }
    if (maxcol == chunk)
      sout.writeDouble(acc(maxcol))
  }

  private def process_rows(sout:DataOutputStream, sin:DataInputStream, rowstart:Int, rowto:Int, q:Double, drift:Double) = {
    sout.writeInt(rowto)
    var chunk = rowstart - rowto
    var acc = new Array[Double](chunk+1)
    var v1 = 0.0
    for (pos <- 0.until(rowstart+1)) {
      var v = sin.readDouble
      apply_column(sout, v, v1, acc, pos, chunk, q, drift)
    }
  }

  override def getDependencies : Array[Reference] = {
    this.in_ref match {
      case Some(ref) =>
        val refs = new Array[Reference](1)
        refs(0) = ref
        return refs
      case None =>
        return new Array[Reference](0)
    }
  }

  override def setup = {}

  override def invoke () = {
    var (q,u,d,drift) = init_vals(t, v, rf, n)

    val out_ref = Ciel.RPC.getOutputFilename(0, true, false, false)
    var raw_out = out_ref.open()
    
    this.in_ref match {

      case None =>
        val dos = new DataOutputStream(raw_out)
        dos.writeInt(n)
        gen_initial_optvals(dos, n, s, u, d, k, cp)
	dos.close()

      case Some(ref) => 
       val in_stream = Ciel.RPC.getStreamForReference(ref, 1, true, false, false)
       val data_in = new DataInputStream(in_stream)
       var rowstart = data_in.readInt
       if (rowstart == 0) {
         val oos = new ObjectOutputStream(raw_out)
         var r = data_in.readDouble()
	 oos.writeObject(r)
	 oos.close()
       } else {
         var rowto = scala.math.max(0,(rowstart - chunk))
	 val dos = new DataOutputStream(raw_out)
 	 process_rows(dos, data_in, rowstart, rowto, q, drift)
	 dos.close()
       }

       data_in.close()

    }

  }

}
