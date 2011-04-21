import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._

class ScalaInitTest extends SkylaThread[Int] {

  override def run = {
    val a = Skyla.spawn { _ => 42 }
    val b = Skyla.spawn { _ => 16 }
    val c = Skyla.spawn { _ => 10 }

    println("%%%%% SPAWNED 3 TASKS")

    val realA = a.get
    println("%%%%% GOT REALA")

    val realB = b.get
    println("%%%%% GOT REALB")

    val realC = c.get
    println("%%%%% GOT REALC")

    println(realA)
    println(realB)
    println(realC)

    realA + realB + realC
  }

}
