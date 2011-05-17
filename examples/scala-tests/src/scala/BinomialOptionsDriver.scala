
import java.io.DataInputStream;
import java.io.FileInputStream;

import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.references.Reference
import com.asgow.ciel.references.CielFuture
import com.asgow.ciel.tasks.FirstClassJavaTask
import com.asgow.ciel.scala.SkylaFuture
import com.asgow.ciel.scala.SkylaThread

class BinomialOptionsDriver extends SkylaThread[String] {

      override def run = {
		
		val n = Ciel.args(0).toInt
		val chunk_size = Ciel.args(1).toInt
		
		val s: Double = 100
		val k: Double = 100
		val t: Double = 1
		val v: Double = 0.3
		val rf: Double = 0.03
		val cp: Double = -1
		
		var lastNode = new ScalaBinomialOptions(s, k, t, v, rf, cp, n, chunk_size, None)
	        var last_refs = Ciel.spawn(lastNode, new Array[String](0), 1)
		val range = scala.Range(0, n+1, chunk_size)
		for(i <- range) {
			lastNode = new ScalaBinomialOptions(s, k, t, v, rf, cp, n, chunk_size, Some(last_refs(0)))
			last_refs = Ciel.spawn(lastNode, new Array[String](0), 1)
		}
		
		val wait_ref = last_refs(0)
		val skyla_future = new SkylaFuture[Double](new CielFuture(wait_ref))

		val ret_double = skyla_future.get
		
		("The last node returned: " + ret_double + "\n")
		
	}


}

