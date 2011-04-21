import com.asgow.ciel.executor.Ciel
import com.asgow.ciel.tasks._
import com.asgow.ciel.scala._
import com.asgow.ciel.references._

class InitTailTest(deps : Array[CielFuture[Int]]) extends FirstClassJavaTask {

  override def setup = { }

  override def getDependencies = deps.asInstanceOf[Array[Reference]]

  override def invoke = {
    deps.foreach { i => println(i.get()) }
    Ciel.returnPlainString("Hello, world!")
  }

}

class ScalaInitTest extends SkylaThread[Int] {

  override def run = {
    val a = Skyla.spawn { _ => println("^^^^^^^^^^^^^ A"); 42 }
    val b = Skyla.spawn { _ => println("^^^^^^^^^^^^^ B"); 16 }
    val c = Skyla.spawn { _ => println("^^^^^^^^^^^^^ C"); 10 }

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
