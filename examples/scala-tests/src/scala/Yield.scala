import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._
import scala.util.continuations._

class Yield extends SkylaThread[Int] {

  override def run = {
    var i = 0
    while (i < 1000) {
      Skyla.yieldTask
      i = i + 1
    }    
    42
  }
}
